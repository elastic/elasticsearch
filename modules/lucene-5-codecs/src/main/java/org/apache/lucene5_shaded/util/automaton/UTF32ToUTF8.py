# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import types
import os
import sys
import random

MAX_UNICODE = 0x10FFFF

# TODO
#   - could be more minimal
#     - eg when bracket lands on a utf8 boundary, like 3 - 2047 -- they can share the two * edges
#     - also 3 2048 or 3 65536 -- it should not have an * down the red path, but it does

# MASKS[0] is bottom 1-bit
# MASKS[1] is bottom 2-bits
# ...

utf8Ranges = [(0, 127),
              (128, 2047),
              (2048, 65535),
              (65536, 1114111)]

typeToColor = {'startend': 'purple',
               'start': 'blue',
               'end': 'red'}

class FSA:

  def __init__(self):
    # maps fromNode -> (startUTF8, endUTF8, endNode)
    self.states = {}
    self.nodeUpto = 0

  def run(self, bytes):
    state = self.start
    for b in bytes:
      found = False
      oldState = state
      for label, s, e, n in self.states[state][1:]:
        if b >= s and b <= e:
          if found:
            raise RuntimeError('state %s has ambiguous output for byte %s' % (oldState, b))
          state = n
          found = True
      if not found:
        return -1
      
    return state
        
  def addEdge(self, n1, n2, v1, v2, label):
    """
    Adds edge from n1-n2, utf8 byte range v1-v2.
    """
    assert n1 in self.states
    assert type(v1) is types.IntType
    assert type(v2) is types.IntType
    self.states[n1].append((label, v1, v2, n2))

  def addNode(self, label=None):
    try:
      self.states[self.nodeUpto] = [label]
      return self.nodeUpto
    finally:
      self.nodeUpto += 1

  def toDOT(self, label):
    __l = []
    w = __l.append
    endNode = startNode = None
    for id, details in self.states.items():
      name = details[0]
      if name == 'end':
        endNode = id
      elif name == 'start':
        startNode = id

    w('digraph %s {' % label)
    w('  rankdir=LR;')
    w('  size="8,5";')
    w('  node [color=white label=""]; Ns;')

    w('  node [color=black];')
    w('  node [shape=doublecircle, label=""]; N%s [label="%s"];' % (endNode, endNode))
    w('  node [shape=circle];')

    w('  N%s [label="%s"];' % (startNode, startNode))
    w('  Ns -> N%s;' % startNode)
    for id, details in self.states.items():
      edges = details[1:]
      w('  N%s [label="%s"];' % (id, id))
      for type, s, e, dest in edges:
        c = typeToColor.get(type, 'black')
        if type == 'all*':
          # special case -- matches any utf8 byte at this point
          label = '*'
        elif s == e:
          label = '%s' % binary(s)
        else:
          label = '%s-%s' % (binary(s), binary(e))
        w('  N%s -> N%s [label="%s" color="%s"];' % (id, dest, label, c))
      if name == 'end':
        endNode = id
      elif name == 'start':
        startNode = id
    w('}')
    return '\n'.join(__l)

  def toPNG(self, label, pngOut):
    open('tmp.dot', 'wb').write(self.toDOT(label))
    if os.system('dot -Tpng tmp.dot -o %s' % pngOut):
      raise RuntimeException('dot failed')
    

MASKS = []
v = 2
for i in range(32):
  MASKS.append(v-1)
  v *= 2

def binary(x):
  if x == 0:
    return '00000000'
  
  l = []
  while x > 0:
    if x & 1 == 1:
      l.append('1')
    else:
      l.append('0')
    x = x >> 1

  # big endian!
  l.reverse()

  l2 = []
  while len(l) > 0:
    s = ''.join(l[-8:])
    if len(s) < 8:
      s = '0'*(8-len(s)) + s
    l2.append(s)
    del l[-8:]

  return ' '.join(l2)

def getUTF8Rest(code, numBytes):
  l = []
  for i in range(numBytes):
    l.append((128 | (code & MASKS[5]), 6))
    code = code >> 6
  l.reverse()
  return tuple(l)

def toUTF8(code):
  # code = Unicode code point
  assert code >= 0
  assert code <= MAX_UNICODE

  if code < 128:
    # 0xxxxxxx
    bytes = ((code, 7),)
  elif code < 2048:
    # 110yyyxx 10xxxxxx
    byte1 = (6 << 5) | (code >> 6)
    bytes = ((byte1, 5),) + getUTF8Rest(code, 1)
  elif code < 65536:
    # 1110yyyy 10yyyyxx 10xxxxxx
    len = 3
    byte1 = (14 << 4) | (code >> 12)
    bytes = ((byte1, 4),) + getUTF8Rest(code, 2)
  else:
    # 11110zzz 10zzyyyy 10yyyyxx 10xxxxxx
    len = 4
    byte1 = (30 << 3) | (code >> 18)
    bytes = ((byte1, 3),) + getUTF8Rest(code, 3)

  return bytes

def all(fsa, startNode, endNode, startCode, endCode, left):
  if len(left) == 0:
    fsa.addEdge(startNode, endNode, startCode, endCode, 'all')
  else:
    lastN = fsa.addNode()
    fsa.addEdge(startNode, lastN, startCode, endCode, 'all')
    while len(left) > 1:
      n = fsa.addNode()
      fsa.addEdge(lastN, n, 128, 191, 'all*')
      left = left[1:]
      lastN = n
    fsa.addEdge(lastN, endNode, 128, 191, 'all*')
          
def start(fsa, startNode, endNode, utf8, doAll):
  if len(utf8) == 1:
    fsa.addEdge(startNode, endNode, utf8[0][0], utf8[0][0] | MASKS[utf8[0][1]-1], 'start')
  else:
    n = fsa.addNode()
    fsa.addEdge(startNode, n, utf8[0][0], utf8[0][0], 'start')
    start(fsa, n, endNode, utf8[1:], True)
    end = utf8[0][0] | MASKS[utf8[0][1]-1]
    if doAll and utf8[0][0] != end:
      all(fsa, startNode, endNode, utf8[0][0]+1, end, utf8[1:])

def end(fsa, startNode, endNode, utf8, doAll):
  if len(utf8) == 1:
    fsa.addEdge(startNode, endNode, utf8[0][0] & ~MASKS[utf8[0][1]-1], utf8[0][0], 'end')
  else:
    if utf8[0][1] == 5:
      # special case -- avoid created unused edges (utf8 doesn't accept certain byte sequences):
      start = 194
    else:
      start = utf8[0][0] & (~MASKS[utf8[0][1]-1])
    if doAll and utf8[0][0] != start:
      all(fsa, startNode, endNode, start, utf8[0][0]-1, utf8[1:])
    n = fsa.addNode()
    fsa.addEdge(startNode, n, utf8[0][0], utf8[0][0], 'end')
    end(fsa, n, endNode, utf8[1:], True)

def build(fsa,
          startNode, endNode,
          startUTF8, endUTF8):

  # Break into start, middle, end:
  if startUTF8[0][0] == endUTF8[0][0]:
    # Degen case: lead with the same byte:
    if len(startUTF8) == 1 and len(endUTF8) == 1:
      fsa.addEdge(startNode, endNode, startUTF8[0][0], endUTF8[0][0], 'startend')
      return
    else:
      assert len(startUTF8) != 1
      assert len(endUTF8) != 1
      n = fsa.addNode()
      # single value edge
      fsa.addEdge(startNode, n, startUTF8[0][0], startUTF8[0][0], 'single')
      build(fsa, n, endNode, startUTF8[1:], endUTF8[1:])
  elif len(startUTF8) == len(endUTF8):
    if len(startUTF8) == 1:
      fsa.addEdge(startNode, endNode, startUTF8[0][0], endUTF8[0][0], 'startend')
    else:
      start(fsa, startNode, endNode, startUTF8, False)
      if endUTF8[0][0] - startUTF8[0][0] > 1:
        all(fsa, startNode, endNode, startUTF8[0][0]+1, endUTF8[0][0]-1, startUTF8[1:])
      end(fsa, startNode, endNode, endUTF8, False)
  else:
    # start
    start(fsa, startNode, endNode, startUTF8, True)

    # possibly middle
    byteCount = 1+len(startUTF8)
    while byteCount < len(endUTF8):
      s = toUTF8(utf8Ranges[byteCount-1][0])
      e = toUTF8(utf8Ranges[byteCount-1][1])
      all(fsa, startNode, endNode,
          s[0][0],
          e[0][0],
          s[1:])
      byteCount += 1

    # end
    end(fsa, startNode, endNode, endUTF8, True)

def main():

  if len(sys.argv) not in (3, 4):
    print
    print 'Usage: python %s startUTF32 endUTF32 [testCode]' % sys.argv[0]
    print
    sys.exit(1)

  utf32Start = int(sys.argv[1])
  utf32End = int(sys.argv[2])

  if utf32Start > utf32End:
    print 'ERROR: start must be <= end'
    sys.exit(1)

  fsa = FSA()
  fsa.start = fsa.addNode('start')
  fsa.end = fsa.addNode('end')

  print 's=%s' % ' '.join([binary(x[0]) for x in toUTF8(utf32Start)])
  print 'e=%s' % ' '.join([binary(x[0]) for x in toUTF8(utf32End)])

  if len(sys.argv) == 4:
    print 't=%s [%s]' % \
          (' '.join([binary(x[0]) for x in toUTF8(int(sys.argv[3]))]),
           ' '.join(['%2x' % x[0] for x in toUTF8(int(sys.argv[3]))]))
  
  build(fsa, fsa.start, fsa.end,
        toUTF8(utf32Start),
        toUTF8(utf32End))

  fsa.toPNG('test', '/tmp/outpy.png')
  print 'Saved to /tmp/outpy.png...'

  test(fsa, utf32Start, utf32End, 100000);

def test(fsa, utf32Start, utf32End, count):

  # verify correct ints are accepted
  for i in range(count):
    r = random.randint(utf32Start, utf32End)
    dest = fsa.run([tup[0] for tup in toUTF8(r)])
    if dest != fsa.end:
      print 'FAILED: valid %s (%s) is not accepted' % (r, ' '.join([binary(x[0]) for x in toUTF8(r)]))
      return False

  invalidRange = MAX_UNICODE - (utf32End - utf32Start + 1)
  if invalidRange >= 0:
    # verify invalid ints are not accepted
    for i in range(count):
      r = random.randint(0, invalidRange-1)
      if r >= utf32Start:
        r = utf32End + 1 + r - utf32Start
      dest = fsa.run([tup[0] for tup in toUTF8(r)])
      if dest != -1:
        print 'FAILED: invalid %s (%s) is accepted' % (r, ' '.join([binary(x[0]) for x in toUTF8(r)]))
        return False

  return True

def stress():

  print 'Testing...'

  iter = 0
  while True:
    if iter % 10 == 0:
      print '%s...' % iter
    iter += 1

    v1 = random.randint(0, MAX_UNICODE)
    v2 = random.randint(0, MAX_UNICODE)
    if v2 < v1:
      v1, v2 = v2, v1

    utf32Start = v1
    utf32End = v2

    fsa = FSA()
    fsa.start = fsa.addNode('start')
    fsa.end = fsa.addNode('end')
    build(fsa, fsa.start, fsa.end,
          toUTF8(utf32Start),
          toUTF8(utf32End))

    if not test(fsa, utf32Start, utf32End, 10000):
      print 'FAILED on utf32Start=%s utf32End=%s' % (utf32Start, utf32End)

if __name__ == '__main__':
  if len(sys.argv) > 1:
    main()
  else:
    stress()
