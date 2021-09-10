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

# Note, this file is known to work with rev 120 of the moman
# repository (http://bitbucket.org/jpbarrette/moman/overview)
#
# See also: http://sites.google.com/site/rrettesite/moman

import math
import os
import sys
# sys.path.insert(0, 'moman/finenight/python')
sys.path.insert(0, '../../../../../../../../build/core/moman/finenight/python')
try:
  from possibleStates import genTransitions
except ImportError:
  from finenight.possibleStates import genTransitions

MODE = 'array'
PACKED = True
WORD = 64
LOG2_WORD = int(math.log(WORD) / math.log(2))
# MODE = 'switch'

class LineOutput:

  def __init__(self, indent=''):
    self.l = []
    self._indent = self.startIndent = indent
    self.inComment = False

  def __call__(self, s, indent=0):
    if s.find('}') != -1:
      assert self._indent != self.startIndent
      self._indent = self._indent[:-2]

    if indent != 0:
      indent0 = '  ' * (len(self._indent) / 2 + indent)
    else:
      indent0 = self._indent

    if s.find('/*') != -1:
      if s.find('*/') == -1:
        self.inComment = True
    elif s.find('*/') != -1:
      self.inComment = True

    if self.inComment:
      self.l.append(indent0 + s)
    else:
      self.l.append(indent0 + s.lstrip())

    self.inComment = self.inComment and s.find('*/') == -1

    if s.find('{') != -1:
      self._indent += '  '

  def __str__(self):
    if True:
      assert self._indent == self.startIndent, 'indent %d vs start indent %d' % \
             (len(self._indent), len(self.startIndent))
    return '\n'.join(self.l)

  def indent(self):
    self._indent += '  '

  def outdent(self):
    assert self._indent != self.startIndent
    self._indent = self._indent[:-2]

def charVarNumber(charVar):
  """
  Maps binary number (eg [1, 0, 1]) to its decimal value (5).
  """

  p = 1
  sum = 0
  downTo = len(charVar) - 1
  while downTo >= 0:
    sum += p * int(charVar[downTo])
    p *= 2
    downTo -= 1
  return sum

def main():

  if len(sys.argv) != 3:
    print
    print 'Usage: python -u %s N <True/False>' % sys.argv[0]
    print
    print 'NOTE: the resulting .java file is created in the current working dir!'
    print
    sys.exit(1)

  n = int(sys.argv[1])

  transpose = (sys.argv[2] == "True")

  tables = genTransitions(n, transpose)

  stateMap = {}

  # init null state
  stateMap['[]'] = -1

  # init start state
  stateMap['[(0, 0)]'] = 0

  w = LineOutput()

  w('/*')
  w(' * Licensed to the Apache Software Foundation (ASF) under one or more')
  w(' * contributor license agreements.  See the NOTICE file distributed with')
  w(' * this work for additional information regarding copyright ownership.')
  w(' * The ASF licenses this file to You under the Apache License, Version 2.0')
  w(' * (the "License"); you may not use this file except in compliance with')
  w(' * the License.  You may obtain a copy of the License at')
  w(' *')
  w(' *     http://www.apache.org/licenses/LICENSE-2.0')
  w(' *')
  w(' * Unless required by applicable law or agreed to in writing, software')
  w(' * distributed under the License is distributed on an "AS IS" BASIS,')
  w(' * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.')
  w(' * See the License for the specific language governing permissions and')
  w(' * limitations under the License.')
  w(' */')
  w('package org.apache.lucene5_shaded.util.automaton;')
  w('')
  w('// The following code was generated with the moman/finenight pkg')
  w('// This package is available under the MIT License, see NOTICE.txt')
  w('// for more details.')
  w('')
  w('import org.apache.lucene5_shaded.util.automaton.LevenshteinAutomata.ParametricDescription;')
  w('')
  if transpose:
    w('/** Parametric description for generating a Levenshtein automaton of degree %s, ' % n)
    w('    with transpositions as primitive edits */')
    className = 'Lev%dTParametricDescription' % n
  else:
    w('/** Parametric description for generating a Levenshtein automaton of degree %s */' % n)
    className = 'Lev%dParametricDescription' % n

  w('class %s extends ParametricDescription {' % className)

  w('')
  w('@Override')
  w('int transition(int absState, int position, int vector) {')

  w('  // null absState should never be passed in')
  w('  assert absState != -1;')

  w('')
  w('  // decode absState -> state, offset')
  w('  int state = absState/(w+1);')
  w('  int offset = absState%(w+1);')
  w('  assert offset >= 0;')
  w('')

  machines = []

  for i, map in enumerate(tables):
    if i == 0:
      w('if (position == w) {')
    elif i == len(tables) - 1:
      w('} else {')
    else:
      w('} else if (position == w-%d) {' % i)

    if i != 0 and MODE == 'switch':
      w('switch(vector) {')

    l = map.items()
    l.sort()

    numCasesPerVector = None
    numVectors = len(l)

    if MODE == 'array':
      toStateArray = []
      toOffsetIncrArray = []

    for charVar, states in l:

      # somehow it's a string:
      charVar = eval(charVar)

      if i != 0 and MODE == 'switch':
        w('case %s: // <%s>' % (charVarNumber(charVar), ','.join([str(x) for x in charVar])))
        w.indent()

      l = states.items()

      byFromState = {}

      # first pass to assign states
      byAction = {}
      for s, (toS, offset) in l:
        state = str(s)

        toState = str(toS)
        if state not in stateMap:
          stateMap[state] = len(stateMap) - 1
        if toState not in stateMap:
          stateMap[toState] = len(stateMap) - 1

        byFromState[stateMap[state]] = (1 + stateMap[toState], offset)

        fromStateDesc = s[1:len(s) - 1]
        toStateDesc = ', '.join([str(x) for x in toS])

        tup = (stateMap[toState], toStateDesc, offset)
        if tup not in byAction:
          byAction[tup] = []
        byAction[tup].append((fromStateDesc, stateMap[state]))

      if numCasesPerVector is None:
        numCasesPerVector = len(l)
      else:
        # we require this to be uniform... empirically it seems to be!
        assert numCasesPerVector == len(l)

      if MODE == 'array':

        for s in range(numCasesPerVector):
          toState, offsetIncr = byFromState[s]
          toStateArray.append(toState)
          toOffsetIncrArray.append(offsetIncr)

      else:

        # render switches
        w('switch(state) {   // %s cases' % len(l))

        for (toState, toStateDesc, offset), lx in byAction.items():
          for fromStateDesc, fromState in lx:
            w('case %s: // %s' % (fromState, fromStateDesc))
          w.indent()
          w('  state = %s; // %s' % (toState, toStateDesc))
          if offset > 0:
            w('  offset += %s;' % offset)
          w('break;')
          w.outdent()

        w('}')
        if i != 0:
          w('break;')
          w.outdent()

    if MODE == 'array':
      # strangely state can come in wildly out of bounds....
      w('  if (state < %d) {' % numCasesPerVector)
      w('    final int loc = vector * %d + state;' % numCasesPerVector)
      if PACKED:
        w('    offset += unpack(offsetIncrs%d, loc, NBITSOFFSET%d);' % (i, i))
        w('    state = unpack(toStates%d, loc, NBITSSTATES%d)-1;' % (i, i))
      else:
        w('    offset += offsetIncrs%d[loc];' % i)
        w('    state = toStates%d[loc]-1;' % i)
      w('  }')
    elif i != 0:
      w('}')

    machines.append((toStateArray, toOffsetIncrArray, numCasesPerVector, numVectors))

  # ends switch statement for machine
  w('}')

  w('')

  w('  if (state == -1) {')
  w('    // null state')
  w('    return -1;')
  w('  } else {')
  w('    // translate back to abs')
  w('    return state*(w+1)+offset;')
  w('  }')

  # ends transition method
  w('}')

  subs = []
  if MODE == 'array':
    w.indent()
    for i, (toStateArray, toOffsetIncrsArray, numCasesPerVector, numVectors) in enumerate(machines):
      w('')
      w.outdent()
      w('// %d vectors; %d states per vector; array length = %d' % \
        (numVectors, numCasesPerVector, numVectors * numCasesPerVector))
      w.indent()
      if PACKED:
        # pack in python
        l, nbits = pack(toStateArray)
        subs.append(('NBITSSTATES%d' % i, str(nbits)))
        w('  private final static long[] toStates%d = new long[] /*%d bits per value */ %s;' % \
          (i, nbits, renderList([hex(long(x)) for x in l])))

        l, nbits = pack(toOffsetIncrsArray)
        subs.append(('NBITSOFFSET%d' % i, str(nbits)))
        w('  private final static long[] offsetIncrs%d = new long[] /*%d bits per value */ %s;' % \
          (i, nbits, renderList([hex(long(x)) for x in l])))
      else:
        w('  private final static int[] toStates%d = new int[] %s;' % \
          (i, renderList([str(x) for x in toStateArray])))
        w('  private final static int[] offsetIncrs%d = new int[] %s;' % \
          (i, renderList([str(x) for x in toStateArray])))
    w.outdent()

  stateMap2 = dict([[v, k] for k, v in stateMap.items()])
  w('')
  w('// state map')
  sum = 0
  minErrors = []
  for i in xrange(len(stateMap2) - 1):
    w('//   %s -> %s' % (i, stateMap2[i]))
    # we replace t-notation as it's not relevant here
    st = stateMap2[i].replace('t', '')

    v = eval(st)
    minError = min([-i + e for i, e in v])
    c = len(v)
    sum += c
    minErrors.append(minError)
  w('')

  w.indent()
  # w('private final static int[] minErrors = new int[] {%s};' % ','.join([str(x) for x in minErrors]))

  w.outdent()

  w('')
  w('  public %s(int w) {' % className)
  w('    super(w, %d, new int[] {%s});' % (n, ','.join([str(x) for x in minErrors])), indent=1)
  w('  }')

  if 0:
    w('')
    w('@Override')
    w('public int size() { // this can now move up?')
    w('  return %d*(w+1);' % (len(stateMap2) - 1))
    w('}')

    w('')
    w('@Override')
    w('public int getPosition(int absState) { // this can now move up?')
    w('  return absState % (w+1);')
    w('}')

    w('')
    w('@Override')
    w('public boolean isAccept(int absState) { // this can now move up?')
    w('  // decode absState -> state, offset')
    w('  int state = absState/(w+1);')
    w('  if (true || state < minErrors.length) {')
    w('    int offset = absState%(w+1);')
    w('    assert offset >= 0;')
    w('    return w - offset + minErrors[state] <= %d;' % n)
    w('  } else {')
    w('    return false;')
    w('  }')
    w('}')

  if MODE == 'array' and PACKED:

    # we moved into super class
    if False:
      w('')

      v = 2
      l = []
      for i in range(63):
        l.append(hex(v - 1))
        v *= 2

      w('private final static long[] MASKS = new long[] {%s};' % ','.join(l), indent=1)
      w('')

      # unpack in java
      w('private int unpack(long[] data, int index, int bitsPerValue) {')
      w('  final long bitLoc = bitsPerValue * index;')
      w('  final int dataLoc = (int) (bitLoc >> %d);' % LOG2_WORD)
      w('  final int bitStart = (int) (bitLoc & %d);' % (WORD - 1))
      w('  //System.out.println("index=" + index + " dataLoc=" + dataLoc + " bitStart=" + bitStart + " bitsPerV=" + bitsPerValue);')
      w('  if (bitStart + bitsPerValue <= %d) {' % WORD)
      w('    // not split')
      w('    return (int) ((data[dataLoc] >> bitStart) & MASKS[bitsPerValue-1]);')
      w('  } else {')
      w('    // split')
      w('    final int part = %d-bitStart;' % WORD)
      w('    return (int) (((data[dataLoc] >> bitStart) & MASKS[part-1]) +')
      w('      ((data[1+dataLoc] & MASKS[bitsPerValue-part-1]) << part));', indent=1)
      w('  }')
      w('}')

  # class
  w('}')
  w('')

  fileOut = '%s.java' % className

  s = str(w)
  for sub, repl in subs:
    s = s.replace(sub, repl)

  open(fileOut, 'wb').write(s)

  print 'Wrote %s [%d lines; %.1f KB]' % \
        (fileOut, len(w.l), os.path.getsize(fileOut) / 1024.)

def renderList(l):
  lx = ['    ']
  for i in xrange(len(l)):
    if i > 0:
      lx.append(',')
      if i % 4 == 0:
        lx.append('\n    ')
    lx.append(l[i])
  return '{\n%s\n  }' % ''.join(lx)

MASKS = []
v = 2
for i in xrange(63):
  MASKS.append(v - 1)
  v *= 2

# packs into longs; returns long[], numBits
def pack(l):
  maxV = max(l)
  bitsPerValue = max(1, int(math.ceil(math.log(maxV + 1) / math.log(2.0))))

  bitsLeft = WORD
  pendingValue = 0

  packed = []
  for i in xrange(len(l)):
    v = l[i]
    if pendingValue > 0:
      bitsUsed = math.ceil(math.log(pendingValue) / math.log(2.0))
      assert bitsUsed <= (WORD - bitsLeft), 'bitsLeft=%s (%s-%s=%s) bitsUsed=%s' % (bitsLeft, WORD, bitsLeft, WORD - bitsLeft, bitsUsed)

    if bitsLeft >= bitsPerValue:
      pendingValue += v << (WORD - bitsLeft)
      bitsLeft -= bitsPerValue
      if bitsLeft == 0:
        packed.append(pendingValue)
        bitsLeft = WORD
        pendingValue = 0
    else:
      # split

      # bottom bitsLeft go in current word:
      pendingValue += (v & MASKS[bitsLeft - 1]) << (WORD - bitsLeft)
      packed.append(pendingValue)

      pendingValue = v >> bitsLeft
      bitsLeft = WORD - (bitsPerValue - bitsLeft)

  if bitsLeft < WORD:
    packed.append(pendingValue)

  # verify(l, packed, bitsPerValue)

  return packed, bitsPerValue

def verify(data, packedData, bitsPerValue):
  for i in range(len(data)):
    assert data[i] == unpack(packedData, i, bitsPerValue)

def unpack(data, index, bitsPerValue):
  bitLoc = bitsPerValue * index
  dataLoc = int(bitLoc >> LOG2_WORD)
  bitStart = int(bitLoc & (WORD - 1))
  if bitStart + bitsPerValue <= WORD:
    # not split
    return int(((data[dataLoc] >> bitStart) & MASKS[bitsPerValue - 1]))
  else:
    # split
    part = WORD - bitStart;
    return int((((data[dataLoc] >> bitStart) & MASKS[part - 1]) +
                ((data[1 + dataLoc] & MASKS[bitsPerValue - part - 1]) << part)))

if __name__ == '__main__':
  if not __debug__:
    print
    print 'ERROR: please run without -O'
    print
    sys.exit(1)
  main()
