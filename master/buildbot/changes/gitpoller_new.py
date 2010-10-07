#
# written by Thomas Moschny <thomas.moschny@gmx.de>
#
# Notes:
# - intended to be used on a bare, mirroring repo in repodir
# - next version will clone the repo if not already there
#
# - we don't deal with encodings at all
# - maybe we could use reflogs, but then wildcards wouldn't work
#
from buildbot import util
from buildbot.changes import base
from buildbot.changes.changes import Change

from twisted.internet import defer, reactor, utils
from twisted.internet.task import LoopingCall
from twisted.python import log

import os

PREFIX='refs/heads/'

class GitPoller(base.ChangeSource, util.ComparableMixin):

    compare_attrs = ["name", "project", "branchpattern", "repodir",
                     "pollinterval", "category"]
        
    parent = None
    loop = None
    working = False

    def __init__(self, repodir=None, name=None,
                 pollinterval=10*60, branchpattern='*',
                 gitbin='git', category=None, project=None):

        self.repodir = repodir
        self.name = name or project or repodir
        self.pollinterval = pollinterval
        self.branchpattern = branchpattern
        self.gitbin = gitbin
        self.loop = LoopingCall(self.checkgit)
        self.category = category
        self.project = project

        self.oldrefs = None
        self.refs = None

        self.environ = os.environ.copy()

    def __str__(self):
        return "GitPoller(%s)" % self.name

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

    def describe(self):
        return str(self)

    def checkgit(self):
        if self.working:
            log.msg("%s overrun, previous run not finished yet" % self)
            return defer.succeed(None)
        self.working = True

        if not self.oldrefs:
            d = self.get_refs()
            d.addCallback(self.set_old_refs)
            d.addCallback(self.fetch)
        else:
            d = self.fetch()
        d.addCallback(self.get_refs)
        d.addCallback(self.set_refs)
        d.addCallback(self.get_logs)
        d.addCallbacks(self.finished_ok, self.finished_failure)
        return d

    def set_old_refs(self, refs):
        self.oldrefs = refs

    def set_refs(self, refs):
        self.refs = refs

    def get_logs(self, _=None):
        # log A..B is basically means: show all revisions reachable
        # from B but not from A
        l = []
        for name, ref in self.refs.iteritems():
            oldref = self.oldrefs.get(name, '')
            if oldref != ref:
                args = ['log', "%s%s" % (oldref and "%s.." % oldref, ref),
                        '--pretty=format:%H%x00%cN <%cE>%x00%at%x00%s%x00',
                        '-z' ,'--name-only', '--reverse']
                d = utils.getProcessOutput(self.gitbin, args, path=self.repodir,
                                           env=self.environ)
                d.addCallback(self.parse_log, name)
                l.append(d)
        return defer.gatherResults(l)

    def parse_log(self, log, branch):
        # format of the returned log:
        #
        #   log := entry? (\0 entry)*
        #   stanza := ref \0 author \0 date \0 subject \0 (\n (file \0)+ )?
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

    def get_refs(self, _=None):
        args = ['for-each-ref',
                '--format=%(objectname) %(objecttype) %(refname)',
                '%s%s' % (PREFIX, self.branchpattern)]
        d = utils.getProcessOutput(self.gitbin, args, path=self.repodir,
                                   env=self.environ)
        d.addCallback(self.parse_refs)
        return d

    def fetch(self, _=None):
        args = ['fetch', '-q']
        return utils.getProcessOutput(self.gitbin, args, path=self.repodir,
                                      env=self.environ)

    def parse_refs(self, res):
        refs = {}
        for line in res.splitlines():
            ref, _, name = line.split(' ')
            assert name.startswith(PREFIX)
            name = name[len(PREFIX):]
            refs[name] = ref
        return refs

    def finished_ok(self, res):
        log.msg("%s finished polling %s" % (self, res or ''))
        self.oldrefs, self.refs = self.refs, None
        self.working = False
        return res

    def finished_failure(self, failure):
        log.msg("%s failed %s" % (self, failure))
        self.working = False
        return False
