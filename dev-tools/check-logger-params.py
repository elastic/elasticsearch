import os
import re

# NOTE: this is only approximate, it gets fooled by nested expressions, by debug/info/trace taking the optional Throwable cause, etc.!

reLoggerLine = re.compile(r'(logger.(?:error|warn|debug|info|trace)\s*)\((.*?)\)\s*;', re.DOTALL)

def parseParams(s):
  upto = 0
  inQuote = False
  params = []
  lastStart = 0
  while upto < len(s):
    ch = s[upto]
    upto += 1
    if inQuote:
      if ch == '"':
        inQuote = False
    elif ch == ',':
      params.append(s[lastStart:upto])
      lastStart = upto
    elif ch == '"':
      inQuote = True
  if lastStart < upto:
    params.append(s[lastStart:upto])
  return params

def getLineNumber(s, fragment):
  # stupid slow but hopefully bug free approach:
  loc = s.find(fragment)
  if loc == -1:
    raise RuntimeError('fragment "%s" does not occur in file' % repr(s))

  upto = 0
  for lineNumber, line in enumerate(s.splitlines()):
    upto += len(line)+1
    if upto > loc:
      return lineNumber+1
  return lineNumber+1
    
for root, dirs, files in os.walk("."):
  for file in files:
    if file.endswith('.java'):
      fullPath = os.path.join(root, file)
      s = open(fullPath).read()
      for parts in reLoggerLine.findall(s):
        params = parseParams(parts[1])

        expectedParams = parts[1].count('{}')

        if parts[0] in ('logger.error', 'logger.warn'):
          # Cause is required arg:
          expectedParams += 1

        if len(params)-1 != expectedParams:
          print('\n%s:%d: expected %d params but saw %d' % (fullPath, getLineNumber(s, '%s(%s);' % parts), expectedParams, len(params)-1))
          print('  %s(%s);' % parts)

