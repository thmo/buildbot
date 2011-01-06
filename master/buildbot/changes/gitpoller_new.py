#
# written by Thomas Moschny <thomas.moschny@gmx.de>
#
# Notes:
# - intended to be used on a bare, mirroring repo in repodir (created
#   with "git clone --mirror")
# - next version will clone the repo if not already there
# - maybe we could use reflogs, but then wildcards wouldn't work
#
# - we don't deal with encodings yet.
#   - git returns commit messages as UTF-8 unless told otherwise
#   - file names are stored as bytes.
#
from buildbot import util
from buildbot.changes import base
from buildbot.changes.changes import Change

from twisted.internet import defer, reactor, utils
from twisted.internet.task import LoopingCall
from twisted.python import log

import os
import re

PREFIX = 'refs/heads/'
REFS = '*'

class GitPoller(base.ChangeSource, util.ComparableMixin):

    compare_attrs = ["name", "project", "repodir",
                     "pollinterval", "category"]

    parent = None
    loop = None
    working = False

    def __init__(self, repodir=None, name=None,
                 pollinterval=10*60, branch_re=r'.*',
                 gitbin='git', category=None, project=None):

        self.repodir = repodir
        self.name = name or project or repodir
        self.pollinterval = pollinterval
        self.gitbin = gitbin
        self.branch_re = re.compile(branch_re)
        self.loop = LoopingCall(self.checkgit)
        self.category = category
        if project is None:
            project = ''
        self.project = project

        self.oldrefs = None
        self.refs = None

        self.environ = os.environ.copy()

    def __str__(self):
        return "GitPoller(%s)" % self.name

    def describe(self):
        return str(self)

    def startService(self):
        log.msg("%s starting" % self)
        base.ChangeSource.startService(self)
        # the reactor is not running yet
        reactor.callLater(0, self.initialize)

    def initialize(self):
        # FIXME: do initial fetch etc here, and on success:
        reactor.callLater(0, self.loop.start, self.pollinterval)

    def stopService(self):
        log.msg("%s shutting down" % self)
        self.loop.stop()
        return base.ChangeSource.stopService(self)

    def checkgit(self):

        def get_refs(_=None, first=False):

            def parse_refs(res):
                refs = {}
                for line in res.splitlines():
                    ref, _, name = line.split(' ')
                    assert name.startswith(PREFIX)
                    name = name[len(PREFIX):]
                    refs[name] = ref
                return refs

            def set_refs(res, first):
                if first:
                    self.oldrefs = res
                else:
                    self.refs = res

            args = ['for-each-ref',
                    '--format=%(objectname) %(objecttype) %(refname)',
                    '%s%s' % (PREFIX, REFS)]
            d = utils.getProcessOutput(self.gitbin, args, path=self.repodir,
                                       env=self.environ)
            d.addCallback(parse_refs)
            d.addCallback(set_refs, first)
            return d


        def fetch(_=None):
            args = ['fetch', '-q']
            return utils.getProcessOutput(self.gitbin, args, path=self.repodir,
                                          env=self.environ)

        def get_logs(_):

            def parse_log(log, branch):
                # format of the returned log:
                #
                #   log := entry? (\0 entry)*
                #   entry := ref \0 author \0 date \0 subject \0 (\n (file \0)+ )?
                #
                # there's always a \0\0 between two consecutive entries so we
                # can split there.
                if log:
                    for entry in log.rstrip('\0').split('\0\0'):
                        f = entry.split('\0')
                        assert len(f)>=4
                        ref, who, comments = f[0], f[1], f[3]
                        when = float(f[2])
                        files = f[4:]
                        if files:
                            # remove the leading newline
                            files[0] = files[0].lstrip('\n')
                        c = Change(who=who, when=when, files=files, comments=comments,
                                   revision=ref, branch=branch, category=self.category,
                                   repository=self.repodir, project=self.project)
                        self.parent.addChange(c)
                return defer.succeed(None)

            # log A..B basically means: show all revisions reachable from
            # B but not from A
            l = []
            for name, ref in self.refs.iteritems():
                if not self.branch_re.match(name):
                    continue
                oldref = self.oldrefs.get(name, '')
                if oldref == ref:
                    continue
                args = ['log', "%s%s" % (oldref and "%s.." % oldref, ref),
                        '--pretty=format:%H%x00%aN <%aE>%x00%at%x00%B%x00',
                        '-z' ,'--name-only', '--reverse']
                d = utils.getProcessOutput(self.gitbin, args, path=self.repodir,
                                           env=self.environ)
                d.addCallback(parse_log, name)
                l.append(d)
            return defer.gatherResults(l)

        def finished_ok(res):
            log.msg("%s finished polling %s" % (self, res or ''))
            self.oldrefs, self.refs = self.refs, None
            self.working = False
            return res

        def finished_failure(failure):
            log.msg("%s failed %s" % (self, failure))
            self.working = False
            return False

        if self.working:
            log.msg("%s overrun, previous run not finished yet" % self)
            return defer.succeed(None)
        self.working = True

        if not self.oldrefs:
            d = get_refs(first=True)
            d.addCallback(fetch)
        else:
            d = fetch()
        d.addCallback(get_refs)
        d.addCallback(get_logs)
        d.addCallbacks(finished_ok, finished_failure)
        return d


