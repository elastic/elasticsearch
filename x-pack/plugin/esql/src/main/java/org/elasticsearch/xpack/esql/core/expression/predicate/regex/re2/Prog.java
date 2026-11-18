/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
// Original Go source here:
// http://code.google.com/p/go/source/browse/src/pkg/regexp/syntax/prog.go

package com.google.re2j;

import java.util.Arrays;

/**
 * A Prog is a compiled regular expression program.
 */
final class Prog {

  Inst[] inst = new Inst[10];
  int instSize = 0;
  int start; // index of start instruction
  int numCap = 2; // number of CAPTURE insts in re
  // 2 => implicit ( and ) for whole match $0

  // Constructs an empty program.
  Prog() {}

  // Returns the instruction at the specified pc.
  // Precondition: pc > 0 && pc < numInst().
  Inst getInst(int pc) {
    return inst[pc];
  }

  // Returns the number of instructions in this program.
  int numInst() {
    return instSize;
  }

  // Adds a new instruction to this program, with operator |op| and |pc| equal
  // to |numInst()|.
  void addInst(int op) {
    if (instSize >= inst.length) {
      inst = Arrays.copyOf(inst, inst.length * 2);
    }
    inst[instSize] = new Inst(op);
    instSize++;
  }

  // skipNop() follows any no-op or capturing instructions and returns the
  // resulting instruction.
  Inst skipNop(int pc) {
    Inst i = inst[pc];
    while (i.op == Inst.NOP || i.op == Inst.CAPTURE) {
      i = inst[pc];
      pc = i.out;
    }
    return i;
  }

  // prefix() returns a pair of a literal string that all matches for the
  // regexp must start with, and a boolean which is true if the prefix is the
  // entire match.  The string is returned by appending to |prefix|.
  boolean prefix(StringBuilder prefix) {
    Inst i = skipNop(start);

    // Avoid allocation of buffer if prefix is empty.
    if (!Inst.isRuneOp(i.op) || i.runes.length != 1) {
      return i.op == Inst.MATCH; // (append "" to prefix)
    }

    // Have prefix; gather characters.
    while (Inst.isRuneOp(i.op) && i.runes.length == 1 && (i.arg & RE2.FOLD_CASE) == 0) {
      prefix.appendCodePoint(i.runes[0]); // an int, not a byte.
      i = skipNop(i.out);
    }
    return i.op == Inst.MATCH;
  }

  // startCond() returns the leading empty-width conditions that must be true
  // in any match.  It returns -1 (all bits set) if no matches are possible.
  int startCond() {
    int flag = 0; // bitmask of EMPTY_* flags
    int pc = start;
    loop:
    for (; ; ) {
      Inst i = inst[pc];
      switch (i.op) {
        case Inst.EMPTY_WIDTH:
          flag |= i.arg;
          break;
        case Inst.FAIL:
          return -1;
        case Inst.CAPTURE:
        case Inst.NOP:
          break; // skip
        default:
          break loop;
      }
      pc = i.out;
    }
    return flag;
  }

  // --- Patch list ---

  // A patchlist is a list of instruction pointers that need to be filled in
  // (patched).  Because the pointers haven't been filled in yet, we can reuse
  // their storage to hold the list.  It's kind of sleazy, but works well in
  // practice.  See http://swtch.com/~rsc/regexp/regexp1.html for inspiration.

  // These aren't really pointers: they're integers, so we can reinterpret them
  // this way without using package unsafe.  A value l denotes p.inst[l>>1].out
  // (l&1==0) or .arg (l&1==1).  l == 0 denotes the empty list, okay because we
  // start every program with a fail instruction, so we'll never want to point
  // at its output link.

  int next(int l) {
    Inst i = inst[l >> 1];
    if ((l & 1) == 0) {
      return i.out;
    }
    return i.arg;
  }

  void patch(int l, int val) {
    while (l != 0) {
      Inst i = inst[l >> 1];
      if ((l & 1) == 0) {
        l = i.out;
        i.out = val;
      } else {
        l = i.arg;
        i.arg = val;
      }
    }
  }

  int append(int l1, int l2) {
    if (l1 == 0) {
      return l2;
    }
    if (l2 == 0) {
      return l1;
    }
    int last = l1;
    for (; ; ) {
      int next = next(last);
      if (next == 0) {
        break;
      }
      last = next;
    }
    Inst i = inst[last >> 1];
    if ((last & 1) == 0) {
      i.out = l2;
    } else {
      i.arg = l2;
    }
    return l1;
  }

  // ---

  @Override
  public String toString() {
    StringBuilder out = new StringBuilder();
    for (int pc = 0; pc < instSize; ++pc) {
      int len = out.length();
      out.append(pc);
      if (pc == start) {
        out.append('*');
      }
      // Use spaces not tabs since they're not always preserved in
      // Google Java source, such as our tests.
      out.append("        ".substring(out.length() - len)).append(inst[pc]).append('\n');
    }
    return out.toString();
  }
}
