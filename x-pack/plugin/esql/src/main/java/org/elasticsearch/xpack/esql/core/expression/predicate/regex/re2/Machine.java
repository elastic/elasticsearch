/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
// Original Go source here:
// http://code.google.com/p/go/source/browse/src/pkg/regexp/exec.go

package com.google.re2j;

import java.util.Arrays;

// A Machine matches an input string of Unicode characters against an
// RE2 instance using a simple NFA.
//
// Called by RE2.doExecute.
class Machine {

  // A logical thread in the NFA.
  private static class Thread {
    Thread(int n) {
      this.cap = new int[n];
    }

    int[] cap;
    Inst inst;
  }

  // A queue is a 'sparse array' holding pending threads of execution.  See:
  // research.swtch.com/2008/03/using-uninitialized-memory-for-fun-and.html
  private static class Queue {

    final Thread[] denseThreads; // may contain stale Thread in slots >= size
    final int[] densePcs; // may contain stale pc in slots >= size
    final int[] sparse; // may contain stale but in-bounds values.
    int size; // of prefix of |dense| that is logically populated

    Queue(int n) {
      this.sparse = new int[n];
      this.densePcs = new int[n];
      this.denseThreads = new Thread[n];
    }

    boolean contains(int pc) {
      int j = sparse[pc];
      return j < size && densePcs[j] == pc;
    }

    boolean isEmpty() {
      return size == 0;
    }

    int add(int pc) {
      int j = size++;
      sparse[pc] = j;
      denseThreads[j] = null;
      densePcs[j] = pc;
      return j;
    }

    void clear() {
      size = 0;
    }

    @Override
    public String toString() {
      StringBuilder out = new StringBuilder();
      out.append('{');
      for (int i = 0; i < size; ++i) {
        if (i != 0) {
          out.append(", ");
        }
        out.append(densePcs[i]);
      }
      out.append('}');
      return out.toString();
    }
  }

  // Corresponding compiled regexp.
  private RE2 re2;

  // Compiled program.
  private final Prog prog;

  // Two queues for runq, nextq.
  private final Queue q0, q1;

  // pool of available threads
  // Really a stack:
  private Thread[] pool = new Thread[10];
  private int poolSize;

  // Whether a match was found.
  private boolean matched;

  // Capture information for the match.
  private int[] matchcap;
  private int ncap;

  // Make sure to include new fields in the copy constructor

  // Pointer to form a linked stack for the pool of Machines. Not included in copy constructor.
  Machine next;

  /**
   * Constructs a matching Machine for the specified {@code RE2}.
   */
  Machine(RE2 re2) {
    this.prog = re2.prog;
    this.re2 = re2;
    this.q0 = new Queue(prog.numInst());
    this.q1 = new Queue(prog.numInst());
    this.matchcap = new int[prog.numCap < 2 ? 2 : prog.numCap];
  }

  /** Copy constructor, but does not include {@code next} */
  Machine(Machine copy) {
    // Make sure to include any new fields here
    this.re2 = copy.re2;
    this.prog = copy.prog;
    this.q0 = copy.q0;
    this.q1 = copy.q1;
    this.pool = copy.pool;
    this.poolSize = copy.poolSize;
    this.matched = copy.matched;
    this.matchcap = copy.matchcap;
    this.ncap = copy.ncap;
  }

  // init() reinitializes an existing Machine for re-use on a new input.
  void init(int ncap) {
    // length change need new arrays
    this.ncap = ncap;
    if (ncap > matchcap.length) {
      initNewCap(ncap);
    } else {
      resetCap(ncap);
    }
  }

  private void resetCap(int ncap) {
    // same size just reset to 0
    for (int i = 0; i < poolSize; i++) {
      Thread t = pool[i];
      Arrays.fill(t.cap, 0, ncap, 0);
    }
  }

  private void initNewCap(int ncap) {
    for (int i = 0; i < poolSize; i++) {
      Thread t = pool[i];
      t.cap = new int[ncap];
    }
    this.matchcap = new int[ncap];
  }

  int[] submatches() {
    if (ncap == 0) {
      return Utils.EMPTY_INTS;
    }
    return Arrays.copyOf(matchcap, ncap);
  }

  // alloc() allocates a new thread with the given instruction.
  // It uses the free pool if possible.
  private Thread alloc(Inst inst) {
    Thread t;
    if (poolSize > 0) {
      poolSize--;
      t = pool[poolSize];
    } else {
      t = new Thread(matchcap.length);
    }
    t.inst = inst;
    return t;
  }

  // Frees all threads on the thread queue, returning them to the free pool.
  private void free(Queue queue) {
    free(queue, 0);
  }

  private void free(Queue queue, int from) {
    int numberOfThread = queue.size - from;
    int requiredPoolLength = poolSize + numberOfThread;
    if (pool.length < requiredPoolLength) {
      pool = Arrays.copyOf(pool, Math.max(pool.length * 2, requiredPoolLength));
    }

    for (int i = from; i < queue.size; ++i) {
      Thread t = queue.denseThreads[i];
      if (t != null) {
        pool[poolSize] = t;
        poolSize++;
      }
    }
    queue.clear();
  }

  // free() returns t to the free pool.
  private void free(Thread t) {
    if (pool.length <= poolSize) {
      pool = Arrays.copyOf(pool, pool.length * 2);
    }
    pool[poolSize] = t;
    poolSize++;
  }

  // match() runs the machine over the input |in| starting at |pos| with the
  // RE2 Anchor |anchor|.
  // It reports whether a match was found.
  // If so, matchcap holds the submatch information.
  boolean match(MachineInput in, int pos, int anchor) {
    int startCond = re2.cond;
    if (startCond == Utils.EMPTY_ALL) { // impossible
      return false;
    }
    if ((anchor == RE2.ANCHOR_START || anchor == RE2.ANCHOR_BOTH) && pos != 0) {
      return false;
    }
    matched = false;
    Arrays.fill(matchcap, 0, prog.numCap, -1);
    Queue runq = q0, nextq = q1;
    int r = in.step(pos);
    int rune = r >> 3;
    int width = r & 7;
    int rune1 = -1;
    int width1 = 0;
    if (r != MachineInput.EOF) {
      r = in.step(pos + width);
      rune1 = r >> 3;
      width1 = r & 7;
    }
    int flag; // bitmask of EMPTY_* flags
    if (pos == 0) {
      flag = Utils.emptyOpContext(-1, rune);
    } else {
      flag = in.context(pos);
    }
    for (; ; ) {

      if (runq.isEmpty()) {
        if ((startCond & Utils.EMPTY_BEGIN_TEXT) != 0 && pos != 0) {
          // Anchored match, past beginning of text.
          break;
        }
        if (matched) {
          // Have match; finished exploring alternatives.
          break;
        }
        if (!re2.prefix.isEmpty() && rune1 != re2.prefixRune && in.canCheckPrefix()) {
          // Match requires literal prefix; fast search for it.
          int advance = in.index(re2, pos);
          if (advance < 0) {
            break;
          }
          pos += advance;
          r = in.step(pos);
          rune = r >> 3;
          width = r & 7;
          r = in.step(pos + width);
          rune1 = r >> 3;
          width1 = r & 7;
        }
      }
      if (!matched && (pos == 0 || anchor == RE2.UNANCHORED)) {
        // If we are anchoring at begin then only add threads that begin
        // at |pos| = 0.
        if (ncap > 0) {
          matchcap[0] = pos;
        }
        add(runq, prog.start, pos, matchcap, flag, null);
      }
      int nextPos = pos + width;
      flag = in.context(nextPos);
      step(runq, nextq, pos, nextPos, rune, flag, anchor, pos == in.endPos());
      if (width == 0) { // EOF
        break;
      }
      if (ncap == 0 && matched) {
        // Found a match and not paying attention
        // to where it is, so any match will do.
        break;
      }
      pos += width;
      rune = rune1;
      width = width1;
      if (rune != -1) {
        r = in.step(pos + width);
        rune1 = r >> 3;
        width1 = r & 7;
      }
      Queue tmpq = runq;
      runq = nextq;
      nextq = tmpq;
    }
    free(nextq);
    return matched;
  }

  // step() executes one step of the machine, running each of the threads
  // on |runq| and appending new threads to |nextq|.
  // The step processes the rune |c| (which may be -1 for EOF),
  // which starts at position |pos| and ends at |nextPos|.
  // |nextCond| gives the setting for the EMPTY_* flags after |c|.
  // |anchor| is the anchoring flag and |atEnd| signals if we are at the end of
  // the input string.
  private void step(
      Queue runq,
      Queue nextq,
      int pos,
      int nextPos,
      int c,
      int nextCond,
      int anchor,
      boolean atEnd) {
    boolean longest = re2.longest;
    for (int j = 0; j < runq.size; ++j) {
      Thread t = runq.denseThreads[j];
      if (t == null) {
        continue;
      }
      if (longest && matched && ncap > 0 && matchcap[0] < t.cap[0]) {
        free(t);
        continue;
      }
      Inst i = t.inst;
      boolean add = false;
      switch (i.op) {
        case Inst.MATCH:
          if (anchor == RE2.ANCHOR_BOTH && !atEnd) {
            // Don't match if we anchor at both start and end and those
            // expectations aren't met.
            break;
          }
          if (ncap > 0 && (!longest || !matched || matchcap[1] < pos)) {
            t.cap[1] = pos;
            System.arraycopy(t.cap, 0, matchcap, 0, ncap);
          }
          if (!longest) {
            free(runq, j + 1);
          }
          matched = true;
          break;

        case Inst.RUNE:
          add = i.matchRune(c);
          break;

        case Inst.RUNE1:
          add = c == i.runes[0];
          break;

        case Inst.RUNE_ANY:
          add = true;
          break;

        case Inst.RUNE_ANY_NOT_NL:
          add = c != '\n';
          break;

        default:
          throw new IllegalStateException("bad inst");
      }
      if (add) {
        t = add(nextq, i.out, nextPos, t.cap, nextCond, t);
      }
      if (t != null) {
        free(t);
        runq.denseThreads[j] = null;
      }
    }
    runq.clear();
  }

  // add() adds an entry to |q| for |pc|, unless the |q| already has such an
  // entry.  It also recursively adds an entry for all instructions reachable
  // from |pc| by following empty-width conditions satisfied by |cond|.  |pos|
  // gives the current position in the input.  |cond| is a bitmask of EMPTY_*
  // flags.
  private Thread add(Queue q, int pc, int pos, int[] cap, int cond, Thread t) {
    if (pc == 0) {
      return t;
    }
    if (q.contains(pc)) {
      return t;
    }
    int d = q.add(pc);
    Inst inst = prog.inst[pc];
    switch (inst.op) {
      default:
        throw new IllegalStateException("unhandled");

      case Inst.FAIL:
        break; // nothing

      case Inst.ALT:
      case Inst.ALT_MATCH:
        t = add(q, inst.out, pos, cap, cond, t);
        t = add(q, inst.arg, pos, cap, cond, t);
        break;

      case Inst.EMPTY_WIDTH:
        if ((inst.arg & ~cond) == 0) {
          t = add(q, inst.out, pos, cap, cond, t);
        }
        break;

      case Inst.NOP:
        t = add(q, inst.out, pos, cap, cond, t);
        break;

      case Inst.CAPTURE:
        if (inst.arg < ncap) {
          int opos = cap[inst.arg];
          cap[inst.arg] = pos;
          add(q, inst.out, pos, cap, cond, null);
          cap[inst.arg] = opos;
        } else {
          t = add(q, inst.out, pos, cap, cond, t);
        }
        break;

      case Inst.MATCH:
      case Inst.RUNE:
      case Inst.RUNE1:
      case Inst.RUNE_ANY:
      case Inst.RUNE_ANY_NOT_NL:
        if (t == null) {
          t = alloc(inst);
        } else {
          t.inst = inst;
        }
        if (ncap > 0 && t.cap != cap) {
          System.arraycopy(cap, 0, t.cap, 0, ncap);
        }
        q.denseThreads[d] = t;
        t = null;
        break;
    }
    return t;
  }
}
