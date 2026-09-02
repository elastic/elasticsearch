#!/bin/bash

# Bootstrap workaround for a git checkout-ordering gap on Windows CI agents.
#
# *.java and *.java.st are declared `text eol=lf` in .gitattributes so
# spotless (configured with LineEnding.UNIX) always sees LF regardless of
# host OS, and so *.java files generated at build time from *.java.st
# templates (StringTemplateTask copies the template's line endings verbatim
# into its output) inherit LF too. That works for any file whose content
# differs between the two commits, because git rewrites it against the
# (now-correct) attribute during checkout. It does NOT help files unchanged
# between commits: the Windows agent's initial `git clone` checks out the
# target branch's current HEAD *before* this rule existed there,
# autocrlf/core.eol on the agent writes those files as CRLF, and a later
# `git checkout -f <pr-commit>` skips them entirely -- git's checkout only
# rewrites paths whose content actually differs, and CRLF-on-disk
# clean-filters back to the same LF blob, so it's considered unmodified and
# left untouched. Result: every *.java/*.java.st file present before this PR
# keeps stale CRLF forever on Windows, and spotlessJavaCheck
# (LineEnding.UNIX) flags all of them -- including files generated from a
# still-CRLF *.java.st template.
#
# Fix up the working tree here, once per job, right after checkout: reset the
# line-ending config to match .gitattributes, then force git to rewrite every
# tracked *.java/*.java.st file from HEAD by deleting it from disk first
# (checkout skips files it still considers unmodified, so simply re-running
# `checkout` without deleting first is not sufficient).
#
# This becomes a fast no-op once these .gitattributes rules are merged to
# `main`, since fresh clones will then already materialize LF from the
# start. Remove this script (and its invocation from pre-command.bat) at
# that point.

set -euo pipefail

git config core.autocrlf false
git config core.eol lf

file_count=$(git ls-files -- '*.java' '*.java.st' | wc -l | tr -d ' ')

if [[ "$file_count" -gt 0 ]]; then
  echo "Renormalizing line endings for $file_count tracked *.java/*.java.st file(s)"
  git ls-files -z -- '*.java' '*.java.st' | xargs -0 rm -f --
  git checkout -f -- '*.java' '*.java.st'
else
  echo "No tracked *.java/*.java.st files found, nothing to renormalize"
fi
