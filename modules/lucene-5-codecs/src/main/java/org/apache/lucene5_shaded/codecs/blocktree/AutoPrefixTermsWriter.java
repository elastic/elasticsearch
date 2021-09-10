/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.codecs.blocktree;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene5_shaded.index.FilteredTermsEnum;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefBuilder;
import org.apache.lucene5_shaded.util.StringHelper;

// TODO: instead of inlining auto-prefix terms with normal terms,
// we could write them into their own virtual/private field.  This
// would make search time a bit more complex, since we'd need to
// merge sort between two TermEnums, but it would also make stats
// API (used by CheckIndex -verbose) easier to implement since we could
// just walk this virtual field and gather its stats)

/** Used in the first pass when writing a segment to locate
 *  "appropriate" auto-prefix terms to pre-compile into the index.
 *  This visits every term in the index to find prefixes that
 *  match {@code >= min} and {@code <= max} number of terms. */

class AutoPrefixTermsWriter {

  //static boolean DEBUG = BlockTreeTermsWriter.DEBUG;
  //static boolean DEBUG = false;
  //static boolean DEBUG2 = BlockTreeTermsWriter.DEBUG2;
  //static boolean DEBUG2 = true;

  /** Describes a range of term-space to match, either a simple prefix
   *  (foo*) or a floor-block range of a prefix (e.g. foo[a-m]*,
   *  foo[n-z]*) when there are too many terms starting with foo*. */
  public static final class PrefixTerm implements Comparable<PrefixTerm> {
    /** Common prefix */
    public final byte[] prefix;

    /** If this is -2, this is a normal prefix (foo *), else it's the minimum lead byte of the suffix (e.g. 'd' in foo[d-m]*). */
    public final int floorLeadStart;

    /** The lead byte (inclusive) of the suffix for the term range we match (e.g. 'm' in foo[d-m*]); this is ignored when
     *  floorLeadStart is -2. */
    public final int floorLeadEnd;

    public final BytesRef term;

    /** Sole constructor. */
    public PrefixTerm(byte[] prefix, int floorLeadStart, int floorLeadEnd) {
      this.prefix = prefix;
      this.floorLeadStart = floorLeadStart;
      this.floorLeadEnd = floorLeadEnd;
      this.term = toBytesRef(prefix, floorLeadStart);

      assert floorLeadEnd >= floorLeadStart;
      assert floorLeadEnd >= 0;
      assert floorLeadStart == -2 || floorLeadStart >= 0;

      // We should never create empty-string prefix term:
      assert prefix.length > 0 || floorLeadStart != -2 || floorLeadEnd != 0xff;
    }

    @Override
    public String toString() {
      String s = brToString(new BytesRef(prefix));
      if (floorLeadStart == -2) {
        s += "[-" + Integer.toHexString(floorLeadEnd) + "]";
      } else {
        s += "[" + Integer.toHexString(floorLeadStart) + "-" + Integer.toHexString(floorLeadEnd) + "]";
      }
      return s;
    }

    @Override
    public int compareTo(PrefixTerm other) {
      int cmp = term.compareTo(other.term);
      if (cmp == 0) {
        if (prefix.length != other.prefix.length) {
          return prefix.length - other.prefix.length;
        }

        // On tie, sort the bigger floorLeadEnd, earlier, since it
        // spans more terms, so during intersect, we want to encounter this one
        // first so we can use it if the automaton accepts the larger range:
        cmp = other.floorLeadEnd - floorLeadEnd;
      }

      return cmp;
    }

    /** Returns the leading term for this prefix term, e.g. "foo" (for
     *  the foo* prefix) or "foom" (for the foo[m-z]* case). */
    private static BytesRef toBytesRef(byte[] prefix, int floorLeadStart) {
      BytesRef br;
      if (floorLeadStart != -2) {
        assert floorLeadStart >= 0;
        br = new BytesRef(prefix.length+1);
      } else {
        br = new BytesRef(prefix.length);
      }
      System.arraycopy(prefix, 0, br.bytes, 0, prefix.length);
      br.length = prefix.length;
      if (floorLeadStart != -2) {
        assert floorLeadStart >= 0;
        br.bytes[br.length++] = (byte) floorLeadStart;
      }

      return br;
    }

    public int compareTo(BytesRef term) {
      return this.term.compareTo(term);
    }

    public TermsEnum getTermsEnum(TermsEnum in) {

      final BytesRef prefixRef = new BytesRef(prefix);

      return new FilteredTermsEnum(in) {
          {
            setInitialSeekTerm(term);
          }

          @Override
          protected AcceptStatus accept(BytesRef term) {
            if (StringHelper.startsWith(term, prefixRef) &&
                (floorLeadEnd == -1 || term.length == prefixRef.length || (term.bytes[term.offset + prefixRef.length] & 0xff) <= floorLeadEnd)) {
              return AcceptStatus.YES;
            } else {
              return AcceptStatus.END;
            }
          }
        };
    }
  }

  // for debugging
  static String brToString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      // If BytesRef isn't actually UTF8, or it's eg a
      // prefix of UTF8 that ends mid-unicode-char, we
      // fallback to hex:
      return b.toString();
    }
  }

  final List<PrefixTerm> prefixes = new ArrayList<>();
  private final int minItemsInPrefix;
  private final int maxItemsInPrefix;

  // Records index into pending where the current prefix at that
  // length "started"; for example, if current term starts with 't',
  // startsByPrefix[0] is the index into pending for the first
  // term/sub-block starting with 't'.  We use this to figure out when
  // to write a new block:
  private final BytesRefBuilder lastTerm = new BytesRefBuilder();
  private int[] prefixStarts = new int[8];
  private List<Object> pending = new ArrayList<>();

  //private final String segment;

  public AutoPrefixTermsWriter(Terms terms, int minItemsInPrefix, int maxItemsInPrefix) throws IOException {
    this.minItemsInPrefix = minItemsInPrefix;
    this.maxItemsInPrefix = maxItemsInPrefix;
    //this.segment = segment;

    TermsEnum termsEnum = terms.iterator();
    while (true) {
      BytesRef term = termsEnum.next();
      if (term == null) {
        break;
      }
      //if (DEBUG) System.out.println("pushTerm: " + brToString(term));
      pushTerm(term);
    }

    if (pending.size() > 1) {
      pushTerm(BlockTreeTermsWriter.EMPTY_BYTES_REF);

      // Also maybe save floor prefixes in root block; this can be a biggish perf gain for large ranges:
      /*
      System.out.println("root block pending.size=" + pending.size());
      for(Object o : pending) {
        System.out.println("  " + o);
      }
      */
      while (pending.size() >= minItemsInPrefix) {
        savePrefixes(0, pending.size());
      }
    }

    // Even though we visited terms in already-sorted order, the prefixes
    // can be slightly unsorted, e.g. aaaaa will be before aaa, so we
    // must sort here so our caller can do merge sort into actual terms
    // when writing.  Probably we should use CollectionUtil.timSort here?
    Collections.sort(prefixes);
  }

  /** Pushes the new term to the top of the stack, and writes new blocks. */
  private void pushTerm(BytesRef text) throws IOException {
    int limit = Math.min(lastTerm.length(), text.length);
    //if (DEBUG) System.out.println("\nterm: " + text.utf8ToString());

    // Find common prefix between last term and current term:
    int pos = 0;
    while (pos < limit && lastTerm.byteAt(pos) == text.bytes[text.offset+pos]) {
      pos++;
    }

    //if (DEBUG) System.out.println("  shared=" + pos + "  lastTerm.length=" + lastTerm.length());

    // Close the "abandoned" suffix now:
    for(int i=lastTerm.length()-1;i>=pos;i--) {

      // How many items on top of the stack share the current suffix
      // we are closing:
      int prefixTopSize = pending.size() - prefixStarts[i];

      while (prefixTopSize >= minItemsInPrefix) {       
        //if (DEBUG) System.out.println("  pop: i=" + i + " prefixTopSize=" + prefixTopSize + " minItemsInBlock=" + minItemsInPrefix);
        savePrefixes(i+1, prefixTopSize);
        //prefixStarts[i] -= prefixTopSize;
        //if (DEBUG) System.out.println("    after savePrefixes: " + (pending.size() - prefixStarts[i]) + " pending.size()=" + pending.size() + " start=" + prefixStarts[i]);

        // For large floor blocks, it's possible we should now re-run on the new prefix terms we just created:
        prefixTopSize = pending.size() - prefixStarts[i];
      }
    }

    if (prefixStarts.length < text.length) {
      prefixStarts = ArrayUtil.grow(prefixStarts, text.length);
    }

    // Init new tail:
    for(int i=pos;i<text.length;i++) {
      prefixStarts[i] = pending.size();
    }

    lastTerm.copyBytes(text);

    // Only append the first (optional) empty string, no the fake last one used to close all prefixes:
    if (text.length > 0 || pending.isEmpty()) {
      byte[] termBytes = new byte[text.length];
      System.arraycopy(text.bytes, text.offset, termBytes, 0, text.length);
      pending.add(termBytes);
    }
  }
  
  void savePrefixes(int prefixLength, int count) throws IOException {

    assert count > 0;

    /*
    if (DEBUG2) {
      BytesRef br = new BytesRef(lastTerm.bytes());
      br.length = prefixLength;
      //System.out.println("  savePrefixes: seg=" + segment + " " + brToString(br) + " count=" + count + " pending.size()=" + pending.size());
      System.out.println("  savePrefixes: " + brToString(br) + " count=" + count + " pending.size()=" + pending.size());
    }
    */

    int lastSuffixLeadLabel = -2;

    int start = pending.size()-count;
    assert start >=0;

    // Special case empty-string suffix case: we are being asked to build prefix terms for all aaa* terms, but 
    // the exact term aaa is here, and we must skip it (it is handled "higher", under the aa* terms):
    Object o = pending.get(start);
    boolean skippedEmptyStringSuffix = false;
    if (o instanceof byte[]) {
      if (((byte[]) o).length == prefixLength) {
        start++;
        count--;
        //if (DEBUG) System.out.println("  skip empty-string term suffix");
        skippedEmptyStringSuffix = true;
      }
    } else {
      PrefixTerm prefix = (PrefixTerm) o;
      if (prefix.term.bytes.length == prefixLength) {
        start++;
        count--;
        //if (DEBUG) System.out.println("  skip empty-string PT suffix");
        skippedEmptyStringSuffix = true;
      }
    }

    int end = pending.size();
    int nextBlockStart = start;
    int nextFloorLeadLabel = -1;
    int prefixCount = 0;

    PrefixTerm lastPTEntry = null;

    for (int i=start; i<end; i++) {

      byte[] termBytes;
      o = pending.get(i);
      PrefixTerm ptEntry;
      if (o instanceof byte[]) {
        ptEntry = null;
        termBytes = (byte[]) o;
      } else {
        ptEntry = (PrefixTerm) o;
        termBytes = ptEntry.term.bytes;
        if (ptEntry.prefix.length != prefixLength) {
          assert ptEntry.prefix.length > prefixLength;
          ptEntry = null;
        }
      }

      //if (DEBUG) System.out.println("    check term=" + brToString(new BytesRef(termBytes)) + " o=" + o);

      // We handled the empty-string suffix case up front:
      assert termBytes.length > prefixLength;

      int suffixLeadLabel = termBytes[prefixLength] & 0xff;

      //if (DEBUG) System.out.println("  i=" + i + " o=" + o + " suffixLeadLabel=" + Integer.toHexString(suffixLeadLabel) + " pendingCount=" + (i - nextBlockStart) + " min=" + minItemsInPrefix);

      if (suffixLeadLabel != lastSuffixLeadLabel) {
        // This is a boundary, a chance to make an auto-prefix term if we want:

        // When we are "recursing" (generating auto-prefix terms on a block of
        // floor'd auto-prefix terms), this assert is non-trivial because it
        // ensures the floorLeadEnd of the previous terms is in fact less
        // than the lead start of the current entry:
        assert suffixLeadLabel > lastSuffixLeadLabel: "suffixLeadLabel=" + suffixLeadLabel + " vs lastSuffixLeadLabel=" + lastSuffixLeadLabel;

        int itemsInBlock = i - nextBlockStart;

        if (itemsInBlock >= minItemsInPrefix && end-nextBlockStart > maxItemsInPrefix) {
          // The count is too large for one block, so we must break it into "floor" blocks, where we record
          // the leading label of the suffix of the first term in each floor block, so at search time we can
          // jump to the right floor block.  We just use a naive greedy segmenter here: make a new floor
          // block as soon as we have at least minItemsInBlock.  This is not always best: it often produces
          // a too-small block as the final block:

          // If the last entry was another prefix term of the same length, then it represents a range of terms, so we must use its ending
          // prefix label as our ending label:
          if (lastPTEntry != null) {
            //if (DEBUG) System.out.println("  use last");
            lastSuffixLeadLabel = lastPTEntry.floorLeadEnd;
          }
          savePrefix(prefixLength, nextFloorLeadLabel, lastSuffixLeadLabel);

          prefixCount++;
          nextFloorLeadLabel = suffixLeadLabel;
          nextBlockStart = i;
        }

        if (nextFloorLeadLabel == -1) {
          nextFloorLeadLabel = suffixLeadLabel;
          //if (DEBUG) System.out.println("set first lead label=" + nextFloorLeadLabel);
        }

        lastSuffixLeadLabel = suffixLeadLabel;
      }

      lastPTEntry = ptEntry;
    }

    // Write last block, if any:
    if (nextBlockStart < end) {
      //System.out.println("  lastPTEntry=" + lastPTEntry + " lastSuffixLeadLabel=" + lastSuffixLeadLabel);
      if (lastPTEntry != null) {
        lastSuffixLeadLabel = lastPTEntry.floorLeadEnd;
      }
      assert lastSuffixLeadLabel >= nextFloorLeadLabel: "lastSuffixLeadLabel=" + lastSuffixLeadLabel + " nextFloorLeadLabel=" + nextFloorLeadLabel;
      if (prefixCount == 0) {
        if (prefixLength > 0) {
          savePrefix(prefixLength, -2, 0xff);
          prefixCount++;
          
          // If we skipped empty string suffix, e.g. term aaa for prefix aaa*, since we
          // are now writing the full aaa* prefix term, we include it here:
          if (skippedEmptyStringSuffix) {
            count++;
          }
        } else {
          // Don't add a prefix term for all terms in the index!
        }
      } else {
        if (lastSuffixLeadLabel == -2) {
          // Special case when closing the empty string root block:
          lastSuffixLeadLabel = 0xff;
        }
        savePrefix(prefixLength, nextFloorLeadLabel, lastSuffixLeadLabel);
        prefixCount++;
      }
    }

    // Remove slice from the top of the pending stack, that we just wrote:

    pending.subList(pending.size()-count, pending.size()).clear();

    // Append prefix terms for each prefix, since these count like real terms that also need to be "rolled up":
    for(int i=0;i<prefixCount;i++) {
      PrefixTerm pt = prefixes.get(prefixes.size()-(prefixCount-i));
      pending.add(pt);
    }
  }

  private void savePrefix(int prefixLength, int floorLeadStart, int floorLeadEnd) {
    byte[] prefix = new byte[prefixLength];
    System.arraycopy(lastTerm.bytes(), 0, prefix, 0, prefixLength);
    assert floorLeadStart != -1;
    assert floorLeadEnd != -1;

    PrefixTerm pt = new PrefixTerm(prefix, floorLeadStart, floorLeadEnd); 
    //if (DEBUG2) System.out.println("    savePrefix: seg=" + segment + " " + pt + " count=" + count);
    //if (DEBUG) System.out.println("    savePrefix: " + pt);

    prefixes.add(pt);
  }
}
