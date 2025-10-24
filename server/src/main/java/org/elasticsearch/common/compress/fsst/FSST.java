// this software is distributed under the MIT License (http://www.opensource.org/licenses/MIT):
//
// Copyright 2018-2019, CWI, TU Munich, FSU Jena
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files
// (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify,
// merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// - The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
// You can contact the authors via the FSST source repository : https://github.com/cwida/fsst

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
 * This file is a Java port of the original library shown in the license above.
 * Original C++ library: https://github.com/cwida/fsst
 *
 * This file contains code derived from https://github.com/cwida/fsst and
 * also includes significant additions by parkertimmins.
 */

package org.elasticsearch.common.compress.fsst;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;

public class FSST {

    public static final VarHandle VH_NATIVE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    public static final VarHandle VH_NATIVE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
    static final int FSST_SAMPLELINE = 512;
    static final int maxStrLength = 8;
    static final int FSST_SAMPLETARGET = 1 << 14;
    static final int FSST_SAMPLEMAXSZ = 2 * FSST_SAMPLETARGET;
    static final int FSST_CODE_BITS = 9;
    static final int FSST_CODE_BASE = 256; // 0x100
    static final int FSST_CODE_MAX = 1 << FSST_CODE_BITS; // 512
    static final int FSST_CODE_MASK = FSST_CODE_MAX - 1; // 511 -> 0x1FF
    static final int FSST_HASH_LOG2SIZE = 10;

    // we represent codes in u16 (not u8). 12 bits code (of which 10 are used), 4 bits length
    // length is included because shortCodes can contain single-byte str during compressBulk
    static final int FSST_LEN_BIT_OFFSET = 12;
    static final int FSST_SHIFT = 15;
    static final long FSST_ICL_FREE = ((15L << 28) | ((FSST_CODE_MASK) << 16));
    private static final int ESCAPE_BYTE = 255;
    private static final long FSST_HASH_PRIME = 2971215073L;

    public static long hash(long w) {
        return (((w) * FSST_HASH_PRIME) ^ (((w) * FSST_HASH_PRIME) >>> FSST_SHIFT));
    }

    static char first1(long str) {
        return (char) (0xFF & str);
    }

    static char first2(long str) {
        return (char) str;
    }

    static char first1(long[] symbols, int idx) {
        return first1(getStr(symbols, idx));
    }

    static char first2(long[] symbols, int idx) {
        return first2(getStr(symbols, idx));
    }

    static long getStr(long[] symbols, int idx) {
        return symbols[(idx << 1) + 1];
    }

    static long getICL(long[] symbols, int idx) {
        return symbols[idx << 1];
    }

    static int getLen(long[] symbols, int idx) {
        return getLen(getICL(symbols, idx));
    }

    static int getLen(long icl) {
        return (int) (icl >>> 28);
    }

    static char getCode(long icl) {
        return (char) (FSST_CODE_MASK & (icl >>> 16));
    }

    static int getIgnored(long icl) {
        return (char) icl;
    }

    static long removeIgnored(long fullStr, long icl) {
        return fullStr & (0xFFFFFFFFFFFFFFFFL >>> getIgnored(icl));
    }

    // ignored bits:
    // icl = ignoredBits:16,code:12,length:4,unused:32 -- but we avoid exposing this bit-field notation
    static long toICL(int code, int len) {
        return (((long) len << 28) | ((long) code << 16) | ((8 - len) * 8L));
    }

    static void set(long[] symbols, int idx, long str, int code, int len) {
        symbols[(idx << 1)] = toICL(code, len);
        symbols[(idx << 1) + 1] = str;
    }

    static void set(long[] symbols, int idx, long str, long icl) {
        symbols[(idx << 1)] = icl;
        symbols[(idx << 1) + 1] = str;
    }

    static void setFree(long[] symbols, int idx) {
        symbols[(idx << 1)] = FSST_ICL_FREE;
        symbols[(idx << 1) + 1] = 0;
    }

    static void setICL(long[] symbols, int idx, int code, int len) {
        symbols[(idx << 1)] = toICL(code, len);
    }

    static char byteToCode(int b) {
        return (char) ((1 << FSST_LEN_BIT_OFFSET) | b);
    }

    public static long readLong(byte[] buf, int offset) {
        return (long) VH_NATIVE_LONG.get(buf, offset);
    }

    public static int readInt(byte[] buf, int offset) {
        return (int) VH_NATIVE_INT.get(buf, offset);
    }

    public static void writeLong(byte[] buf, int offset, long value) {
        VH_NATIVE_LONG.set(buf, offset, value);
    }

    @SuppressWarnings({ "fallthrough" })
    public static long readLong(byte[] str, int pos, int len) {
        long res = 0;

        len = Math.min(len, 8);
        switch (len) {
            case 8:
                res |= (str[pos + 7] & 0xFFL) << 56;
            case 7:
                res |= (str[pos + 6] & 0xFFL) << 48;
            case 6:
                res |= (str[pos + 5] & 0xFFL) << 40;
            case 5:
                res |= (str[pos + 4] & 0xFFL) << 32;
            case 4:
                res |= (str[pos + 3] & 0xFFL) << 24;
            case 3:
                res |= (str[pos + 2] & 0xFF) << 16;
            case 2:
                res |= (str[pos + 1] & 0xFF) << 8;
            case 1:
                res |= (str[pos] & 0xFF);
        }
        return res;
    }

    static boolean isEscapeCode(char pos) {
        return pos < FSST_CODE_BASE;
    }

    public static class SymbolTable {

        static final int hashTableSize = 1 << FSST_HASH_LOG2SIZE;
        static final int hashTableArraySize = hashTableSize * 2;
        static final int hashTableMask = hashTableSize - 1;

        // high bits of icl (len=8,code=FSST_CODE_MASK) indicates free bucket

        char[] shortCodes = new char[65536];
        char[] byteCodes = new char[256];

        // both ht and symbols contains symbols as two adjacent longs
        // value at 0 is icl:
        // value at 1 is symbol bytes
        long[] symbols = new long[FSST_CODE_MAX * 2];
        long[] ht = new long[hashTableArraySize];

        int nSymbols = 0;                 // amount of symbols in the map (max 255)
        int suffixLim = FSST_CODE_MAX;    // 512, codes higher than this do not have a longer suffix
        char terminator;                   // code of 1-byte symbol, that can be used as a terminator during compression
        char[] lenHisto = new char[8];     // lenHisto[x] is the amount of symbols of byte-length (x+1) in this SymbolTable

        void initialize() {
            // fill in [0, 256) with single byte codes
            for (int i = 0; i < 256; ++i) {
                set(symbols, i, i, byteToCode(i), 1); // i is index, str, and code
            }

            // fill [256, 512) with unused flag
            for (int i = 256; i < FSST_CODE_MAX; ++i) {
                set(symbols, i, 0, FSST_CODE_MASK, 1);
            }

            // set hash table empty
            for (int i = 0; i < hashTableSize; ++i) {
                setFree(ht, i);
            }

            // fill byteCodes[] with the pseudo code all bytes (escaped bytes)
            for (int i = 0; i < 256; ++i) {
                byteCodes[i] = byteToCode(i);
            }

            // fill shortCodes[] with the pseudo code for the first byte of each two-byte pattern
            for (int i = 0; i < 65536; ++i) {
                shortCodes[i] = (char) ((1 << FSST_LEN_BIT_OFFSET) | (i & 0xFF)); // byteToCode(i & 0xFF)
            }
        }

        private SymbolTable() {}

        static SymbolTable build() {
            var st = new SymbolTable();
            st.initialize();
            return st;
        }

        static SymbolTable buildUnitialized() {
            return new SymbolTable();
        }

        void clear() {
            Arrays.fill(lenHisto, (char) 0);

            for (int i = FSST_CODE_BASE; i < FSST_CODE_BASE + nSymbols; i++) {
                int len = getLen(symbols, i);
                if (len == 1) {
                    char val = first1(symbols, i);
                    byteCodes[val] = (char) ((1 << FSST_LEN_BIT_OFFSET) | val);
                } else if (len == 2) {
                    char val = first2(symbols, i);
                    shortCodes[val] = (char) ((1 << FSST_LEN_BIT_OFFSET) | (val & 0xFF));
                } else {
                    int idx = hashStr(getStr(symbols, i));
                    setFree(ht, idx);
                }
            }
            nSymbols = 0; // no need to clean symbols[] as no symbols are used
        }

        public void copyInto(SymbolTable copy) {
            copy.nSymbols = nSymbols;
            copy.suffixLim = suffixLim;
            copy.terminator = terminator;
            System.arraycopy(lenHisto, 0, copy.lenHisto, 0, lenHisto.length);
            System.arraycopy(byteCodes, 0, copy.byteCodes, 0, byteCodes.length);
            System.arraycopy(shortCodes, 0, copy.shortCodes, 0, shortCodes.length);
            System.arraycopy(symbols, 0, copy.symbols, 0, symbols.length);
            System.arraycopy(ht, 0, copy.ht, 0, ht.length);
        }

        char getShortCode(long str) {
            return (char) (shortCodes[first2(str)] & FSST_CODE_MASK);
        }

        // return index in hash table
        public static int hashStr(long str) {
            long first3 = str & 0xFFFFFF;
            return (int) (hash((int) first3) & hashTableMask);
        }

        // only called if string has at least 3 characters
        boolean hashInsert(long str, long icl) {
            // ignored prefix already removed
            assert removeIgnored(str, icl) == str;
            int idx = hashStr(str);
            long currICL = getICL(ht, idx);
            boolean taken = (currICL < FSST_ICL_FREE);
            if (taken) return false; // collision in hash table
            set(ht, idx, str, icl);
            return true;
        }

        /**
         * Add an existing symbol to a symbol table.
         * Used after candidates have been picked via priority queue and are re-added to a symbol table
         */
        boolean add(long str, long i_l) {
            assert (FSST_CODE_BASE + nSymbols < FSST_CODE_MAX);

            int len = getLen(i_l);

            int code = (char) (FSST_CODE_BASE + nSymbols);
            long icl = toICL(code, len);

            if (len == 1) {
                byteCodes[first1(str)] = (char) (code + (1 << FSST_LEN_BIT_OFFSET)); // len=1
            } else if (len == 2) {
                shortCodes[first2(str)] = (char) (code + (2 << FSST_LEN_BIT_OFFSET)); // len=2
            } else if (hashInsert(str, icl) == false) {
                return false; // already in hash table
            }

            set(symbols, code, str, icl);
            nSymbols++;
            lenHisto[len - 1]++;
            return true;
        }

        // Removes length prefix from 1 and 2 byte codes
        char findLongestSymbol(long inStr, int inLen) {
            // use default max value, but this is not used
            long inICL = toICL(FSST_CODE_MAX, inLen);

            // first check the hash table
            int idx = hashStr(inStr);
            long icl = getICL(ht, idx);
            long str = getStr(ht, idx);
            // check length in case there are 0x00 bytes in the data
            if (icl <= inICL && str == removeIgnored(inStr, icl)) {
                return getCode(icl);
            }

            if (inLen >= 2) {
                char code = getShortCode(inStr);
                if (code >= FSST_CODE_BASE) return code;
            }
            return (char) (byteCodes[first1(inStr)] & FSST_CODE_MASK);
        }

        char findLongestSymbol(byte[] line, int start, int len) {
            long str;
            if (len >= 8) {
                len = 8;
                str = readLong(line, start);
            } else {
                str = readLong(line, start, len);
            }
            return findLongestSymbol(str, len);
        }

        void print() {

            System.out.println("numSymbols: " + nSymbols);
            System.out.println("terminator: " + terminator);
            System.out.println("suffixLim: " + suffixLim);

            System.out.println("Hash table: ");
            for (int i = 0; i < hashTableSize; ++i) {
                long icl = getICL(ht, i);
                long str = getStr(ht, i);
                if (icl < FSST_ICL_FREE) {
                    System.out.println("idx: " + i + ", " + FSST.toString(str, icl));
                }
            }

            System.out.println("Symbol table: ");
            for (int i = FSST_CODE_BASE; i < FSST_CODE_BASE + nSymbols; ++i) {
                long icl = getICL(symbols, i);
                long str = getStr(symbols, i);
                System.out.println(FSST.toString(str, icl));
            }

            System.out.println("Symbol table final: ");
            for (int i = 0; i < nSymbols; ++i) {
                long icl = getICL(symbols, i);
                long str = getStr(symbols, i);
                System.out.println(FSST.toString(str, icl));
            }

            System.out.println("Short codes: ");
            for (int i = 0; i < nSymbols; ++i) {
                long icl = getICL(symbols, i);
                long str = getStr(symbols, i);

                if (getLen(icl) == 2) {
                    var strRep = fromBytes(toByteArray(str, getLen(icl)));
                    System.out.println("code: " + (int) getShortCode(str) + ", str: '" + strRep + "'");
                }
            }

            Map<Integer, Integer> counts = new HashMap<>();
            for (int i = 0; i < 65536; ++i) {
                int len = shortCodes[i] >> FSST_LEN_BIT_OFFSET;
                if (counts.containsKey(len)) {
                    counts.put(len, counts.get(len) + 1);
                } else {
                    counts.put(len, 1);
                }
            }
            System.out.println(counts);
        }

        // before finalize():
        // - The real symbols are symbols[256..256+nSymbols>. As we may have nSymbols > 255
        // - The first 256 codes are pseudo symbols (all escaped bytes)
        //
        // after finalize():
        // - table layout is symbols[0..nSymbols>, with nSymbols < 256.
        // - Real codes are [0,nSymbols>. 8-th bit not set.
        // - Escapes in shortCodes have the 8th bit set (value: 256+255=511). 255 because the code to be emitted is the escape byte 255
        // - symbols are grouped by length: 2,3,4,5,6,7,8, then 1 (single-byte codes last)
        // the two-byte codes are split in two sections:
        // - first section contains codes for symbols for which there is no longer symbol (no suffix).
        // It allows an early-out during compression
        //
        // finally, shortCodes[] is modified to also encode all single-byte symbols
        // (hence byteCodes[] is not required on a critical path anymore).
        void finalizeSymbolTable() {
            assert nSymbols <= 255;

            // maps original real code [0, 255] to new code
            char[] newCode = new char[256];
            char[] rsum = new char[8];
            int numSymbolsLen1 = lenHisto[0];
            int byteLim = nSymbols - numSymbolsLen1; // since single-byte comes last, byteLim is index of first single-byte code

            // compute running sum of code lengths (starting offsets for each length)
            rsum[0] = (char) byteLim; // byte 1-byte codes come last
            rsum[1] = 0; // 2-byte start at 0
            // [0] = num 1 byte code, [2] = num 1,2 byte codes, etc
            for (int i = 1; i < 7; i++) {
                rsum[i + 1] = (char) (rsum[i] + lenHisto[i]);
            }

            suffixLim = 0;
            for (int i = 0, j = rsum[2]; i < nSymbols; i++) {
                long s1_icl = getICL(symbols, FSST_CODE_BASE + i);
                long s1_str = getStr(symbols, FSST_CODE_BASE + i);
                int len = getLen(s1_icl);

                if (len == 2) {
                    boolean foundSuffix = false;
                    char s1_first2 = first2(s1_str);
                    for (int k = 0; k < nSymbols; k++) {
                        long s2_icl = getICL(symbols, FSST_CODE_BASE + k);
                        long s2_str = getStr(symbols, FSST_CODE_BASE + k);

                        // test if symbol k is a suffix of symbol i
                        if (k != i && getLen(s2_icl) > 1 && s1_first2 == first2(s2_str)) {
                            foundSuffix = true;
                            break;
                        }
                    }

                    // symbols without a larger suffix have a code < suffixLim
                    // opt == 0 means containing str found
                    newCode[i] = (char) (foundSuffix ? --j : suffixLim++);
                } else {
                    // now using rsum as range start counters
                    newCode[i] = rsum[len - 1]++;
                }

                // change code to newCode[i] and move to newCode[i] position
                set(symbols, newCode[i], getStr(symbols, FSST_CODE_BASE + i), newCode[i], len);
            }

            // renumber the codes in byteCodes[]
            for (int i = 0; i < 256; i++) {
                if ((byteCodes[i] & FSST_CODE_MASK) >= FSST_CODE_BASE) {
                    // & 0xFF here convert full code to real code, eg is equivalent to -256
                    byteCodes[i] = (char) (newCode[byteCodes[i] & 0xFF] + (1 << FSST_LEN_BIT_OFFSET));
                } else {
                    byteCodes[i] = 511 + (1 << FSST_LEN_BIT_OFFSET);
                }
            }

            // renumber the codes in shortCodes[]
            for (int i = 0; i < 65536; i++) {
                if ((shortCodes[i] & FSST_CODE_MASK) >= FSST_CODE_BASE) {
                    // mask out the original length, but could probably use 0x3 as max length should be 2
                    shortCodes[i] = (char) (newCode[shortCodes[i] & 0xFF] + (shortCodes[i] & (0xF << FSST_LEN_BIT_OFFSET)));
                } else {
                    // if there is no code, use the single-byte code for the first byte
                    // if there is no single byte code, values will be 511
                    shortCodes[i] = byteCodes[i & 0xFF];
                }
            }

            // replace the symbols in the hash table
            for (int i = 0; i < hashTableSize; i++) {
                long icl = getICL(ht, i);
                if (icl < FSST_ICL_FREE) {
                    char nc = newCode[getCode(icl) & 0xFF];
                    long icl1 = getICL(symbols, nc);
                    long str1 = getStr(symbols, nc);
                    set(ht, i, str1, icl1);
                }
            }
        }

        // aced: 1684366177

        // find terminator: least frequent byte
        public static char findTerminator(List<byte[]> lines) {
            // find terminator: least frequent byte
            char[] byteHisto = new char[256];
            for (int i = 0; i < lines.size(); i++) {
                byte[] line = lines.get(i);
                for (byte b : line) {
                    byteHisto[b & 0xFF]++;
                }
            }

            int minSize = FSST_SAMPLEMAXSZ;
            int terminator = 256;
            int i = terminator;
            while (i-- > 0) {
                if (byteHisto[i] <= minSize) {
                    terminator = (char) i;
                    minSize = byteHisto[i];
                }
            }

            assert (terminator < 256);
            return (char) terminator;
        }

        private static int compressCount(int sampleFrac, List<byte[]> lines, SymbolTable st, Counters counters) { // returns gain
            int gain = 0;

            var random = new Random(123); // use same see for reproducibility
            for (var line : lines) {
                int cur = 0, start = 0, end = line.length;
                // TODO if there are few lines (or rather chunks), sampleFrac skipping may cause all data to be skipped
                // Probably doesn't matter, since this means data is short
                if (sampleFrac < 128) {
                    // in earlier rounds (sampleFrac < 128) we skip data in the sample (reduces overall work ~2x)
                    if (random.nextInt(0, 128 + 1) > sampleFrac) continue;
                }
                if (cur < end) {
                    char code2 = 255;
                    char code1 = st.findLongestSymbol(line, cur, end - cur);
                    var len = getLen(st.symbols, code1);
                    cur += len;
                    gain += len - (1 + (isEscapeCode(code1) ? 1 : 0));
                    while (true) {
                        // count single symbol (i.e. an option is not extending it)
                        counters.count1Inc(code1);

                        // as an alternative, consider just using the next byte..
                        var len1 = getLen(st.symbols, code1);
                        if (len1 != 1) // .. but do not count single byte symbols doubly
                            counters.count1Inc(line[start] & 0xFF);

                        if (cur == end) {
                            break;
                        }

                        // now match a new symbol
                        start = cur;
                        if (cur < end - 7) { // add least 8 bytes left
                            long word = readLong(line, cur);

                            // find existing string matching same 3 letters (or hash collision)
                            int idx = hashStr(word);
                            long icl = getICL(st.ht, idx);
                            long str = getStr(st.ht, idx);

                            code2 = st.getShortCode(word);
                            word = removeIgnored(word, icl);
                            if ((icl < FSST_ICL_FREE) & (str == word)) {
                                code2 = getCode(icl);
                                cur += getLen(icl);
                            } else if (code2 >= FSST_CODE_BASE) {
                                cur += 2;
                            } else {
                                code2 = (char) (st.byteCodes[first1(word)] & FSST_CODE_MASK);
                                cur += 1;
                            }
                        } else {
                            code2 = st.findLongestSymbol(line, cur, end - cur);
                            cur += getLen(st.symbols, code2);
                        }

                        // compute compressed output size
                        gain += (cur - start) - (1 + (isEscapeCode(code1) ? 1 : 0));

                        if (sampleFrac < 128) { // no need to count pairs in final round
                            // consider the symbol that is the concatenation of the two last symbols
                            counters.count2Inc(code1, code2);

                            // as an alternative, consider just extending with the next byte..
                            if ((cur - start) > 1)  // ..but do not count single byte extensions doubly
                                counters.count2Inc(code1, line[start] & 0xFF);
                        }
                        code1 = code2;
                    }
                }
            }
            return gain;
        }

        static class QSymbol implements Comparable<QSymbol> {
            // symbol
            final long icl;
            final long str;
            int gain;

            QSymbol(long icl, long str, int gain) {
                this.icl = icl;
                this.str = str;
                this.gain = gain;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                QSymbol qSymbol = (QSymbol) o;
                // str representation, which has been truncated to correct length
                // Also include length, in case there are actual 0x00 values in one string
                return str == qSymbol.str && getLen(icl) == getLen(qSymbol.icl);
            }

            @Override
            public int hashCode() {
                return Objects.hash(str);
            }

            @Override
            public String toString() {
                return "QSymbol{"
                    + "code="
                    + getCode(icl)
                    + " ("
                    + (int) getCode(icl)
                    + ")"
                    + ", len="
                    + getLen(icl)
                    + ", str="
                    + fromBytes(toByteArray(str, getLen(icl)))
                    + '}';
            }

            @Override
            public int compareTo(QSymbol o) {
                // return element if higher gain, or equal gain and longer
                boolean firstBetter = gain > o.gain || gain == o.gain && str > o.str;
                // shouldn't compare equal elements due to deduplication
                assert str != o.str;
                return firstBetter ? -1 : 1;
            }
        }

        private static void addOrInc(int sampleFrac, Map<Long, QSymbol> cands, long icl, long str, int count) {
            // only accepts strings which have been truncated to correct length
            assert str == removeIgnored(str, icl);

            if (count < (5 * sampleFrac) / 128) return; // improves both compression speed (less candidates), but also quality!!

            int gain = count * getLen(icl);
            QSymbol existing = cands.get(str);
            if (existing == null) {
                cands.put(str, new QSymbol(icl, str, gain));
            } else {
                existing.gain += gain;
            }
        }

        record Symbol(long icl, long str) {
            @Override
            public String toString() {
                return "QSymbol{"
                    + "code="
                    + getCode(icl)
                    + " ("
                    + (int) getCode(icl)
                    + ")"
                    + ", len="
                    + getLen(icl)
                    + ", str="
                    + fromBytes(toByteArray(str, getLen(icl)))
                    + '}';
            }
        }

        // TODO do this without making object
        private static Symbol concat(long icl1, long str1, long icl2, long str2) {
            int len1 = getLen(icl1);
            int len2 = getLen(icl2);
            int length = len1 + len2;
            if (length > maxStrLength) length = maxStrLength;

            long icl = toICL(FSST_CODE_MASK, length);
            long str = (str2 << (8 * len1)) | str1;
            return new Symbol(icl, str);
        }

        /**
         * Use existing SymbolTable and counters to create priority queue of candidate symbols.
         * The clear symbol table and add top 255 symbols from pq
         */
        public static void makeTable(int sampleFrac, SymbolTable st, Counters counters) {
            // hashmap of c (needed because we can generate duplicate candidates)
            Map<Long, QSymbol> cands = new HashMap<>();

            // artificially make terminator the most frequent symbol so it gets included
            char terminator = st.nSymbols > 0 ? FSST_CODE_BASE : st.terminator;
            counters.count1Set(terminator, (char) 65535);

            // add candidate symbols based on counted frequency
            for (int pos1 = 0; pos1 < FSST_CODE_BASE + st.nSymbols; pos1++) {
                int cnt1 = counters.count1Get(pos1);
                if (cnt1 == 0) continue;

                // heuristic: promoting single-byte symbols (*8) helps reduce exception rates and increases [de]compression speed
                long icl = getICL(st.symbols, pos1);
                long str = getStr(st.symbols, pos1);
                int len = getLen(icl);
                addOrInc(sampleFrac, cands, icl, str, ((len == 1) ? 8 : 1) * cnt1);

                if (sampleFrac >= 128 || // last round we do not create new (combined) symbols
                    len == maxStrLength || // symbol cannot be extended
                    first1(str) == st.terminator) { // multi-byte symbols cannot contain the terminator byte
                    continue;
                }
                for (int pos2 = 0; pos2 < FSST_CODE_BASE + st.nSymbols; pos2++) {
                    int cnt2 = counters.count2Get(pos1, pos2);
                    if (cnt2 == 0) continue;

                    // create a new symbol
                    long icl2 = getICL(st.symbols, pos2);
                    long str2 = getStr(st.symbols, pos2);
                    Symbol s3 = concat(icl, str, icl2, str2);
                    if (first1(str2) != st.terminator) // multi-byte symbols cannot contain the terminator byte
                        addOrInc(sampleFrac, cands, s3.icl, s3.str, cnt2);
                }
            }

            // insert candidates into priority queue (by gain)
            var pq = new PriorityQueue<QSymbol>(cands.size());
            pq.addAll(cands.values());

            // Create new symbol map using best candidates
            st.clear();
            while (st.nSymbols < 255 && pq.isEmpty() == false) {
                QSymbol top = pq.poll();
                st.add(top.str, top.icl);
            }
        }

        public static SymbolTable buildSymbolTable(List<byte[]> lines) {
            var counters = new Counters();
            var st = SymbolTable.build();
            var bestTable = SymbolTable.buildUnitialized();
            long bestGain = -FSST_SAMPLEMAXSZ; // worst case (everything exception)
            int sampleFrac;

            st.terminator = findTerminator(lines);

            Counters bestCounters = new Counters();
            // we do 5 rounds (sampleFrac=8,38,68,98,128)
            for (sampleFrac = 8; true; sampleFrac += 30) {
                counters.clear();
                long gain = compressCount(sampleFrac, lines, st, counters);
                if (gain >= bestGain) { // a new best solution!
                    counters.copyInto(bestCounters);
                    st.copyInto(bestTable);
                    bestGain = gain;
                }
                if (sampleFrac >= 128) break; // don't build st on last loop
                makeTable(sampleFrac, st, counters);
            }
            makeTable(sampleFrac, bestTable, bestCounters);
            bestTable.finalizeSymbolTable(); // renumber codes for more efficient compression
            return bestTable;
        }

        public long compressBulk(
            int nlines,
            byte[] data, /* input string data */
            int[] offsets, /* string offset, length is nlines+1 */
            byte[] outBuf, // output buffer, multiple lines will be within buffer
            int[] outOffsets // compressed line start offsets within buffer, length known
        ) {
            boolean avoidBranch = false, noSuffixOpt = false;

            // if 2-byte symbols account for at least 65% percent of symbols
            if (100 * lenHisto[1] > 65 * nSymbols
                // and at least 95% of 2-byte symbols are have no longer symbol with matching prefix
                && 100 * suffixLim > 95 * lenHisto[1]) {
                // use noSuffixOpt - check shortCodes before checking hash table
                noSuffixOpt = true;

                // otherwise decide if should use branch to separate between 1 and 2 byte symbols
            } else if ((lenHisto[0] > 24 && lenHisto[0] < 92)
                && (lenHisto[0] < 43 || lenHisto[6] + lenHisto[7] < 29)
                && (lenHisto[0] < 72 || lenHisto[2] < 72)) {
                    avoidBranch = true;
                }

            if (noSuffixOpt == false && avoidBranch) {
                return compressBulk(nlines, data, offsets, outBuf, outOffsets, false, true);
            } else if (noSuffixOpt && avoidBranch == false) {
                return compressBulk(nlines, data, offsets, outBuf, outOffsets, true, false);
            } else {
                return compressBulk(nlines, data, offsets, outBuf, outOffsets, false, false);
            }
        }

        // optimized adaptive *scalar* compression method
        public long compressBulk(
            int numLines,
            byte[] data,  // input string data
            int[] offsets, // offsets of each string values, length is one more than numLines
            byte[] outBuf, // output buffer, multiple lines will be within buffer
            int[] outOffsets, // compressed line start offsets within buffer, length known
            boolean noSuffixOpt,
            boolean avoidBranch
        ) {
            int outCur = 0;
            int outLim = outBuf.length;
            int curLine = 0;
            int suffixLim = this.suffixLim;
            int byteLim = this.nSymbols - this.lenHisto[0];

            byte[] buf = new byte[512 + 8]; /* +8 sentinel is to avoid 8-byte unaligned-loads going beyond 511 out-of-bounds */

            for (; curLine < numLines; curLine++) {
                int lineLen = offsets[curLine + 1] - offsets[curLine];
                assert lineLen >= 0;
                int chunkLen = 0;
                int chunkStart = 0;
                outOffsets[curLine] = outCur;

                // a single str/line can be in multiple chunks, but a chunk contains at most 1 str
                do {
                    // we need to compress in chunks of 511 in order to be byte-compatible with simd-compressed FSST
                    chunkLen = Math.min(lineLen - chunkStart, 511);

                    int remaining = outLim - outCur;
                    if ((2 * chunkLen + 7) > remaining) {
                        return curLine; // out of memory
                    }

                    // copy the string to the 511-byte buffer
                    System.arraycopy(data, offsets[curLine] + chunkStart, buf, 0, chunkLen);
                    buf[chunkLen] = (byte) this.terminator;

                    int chunkCur = 0;
                    // compress variant
                    while (chunkCur < chunkLen) {
                        long word = readLong(buf, chunkCur);
                        char code = shortCodes[first2(word)];
                        if (noSuffixOpt && (code & 0xFF) < suffixLim) {
                            // 2 byte code without having to worry about longer matches
                            outBuf[outCur++] = (byte) code;
                            chunkCur += 2;
                        } else {
                            int idx = hashStr(word);
                            long icl = getICL(this.ht, idx);
                            long str = getStr(this.ht, idx);
                            outBuf[outCur + 1] = (byte) word; // speculatively write out escaped byte
                            word = removeIgnored(word, icl);
                            if ((icl < FSST_ICL_FREE) && str == word) {
                                outBuf[outCur++] = (byte) getCode(icl);
                                chunkCur += getLen(icl);
                            } else if (avoidBranch) {
                                // could be a 2-byte or 1-byte code, or miss
                                // handle everything with predication
                                outBuf[outCur] = (byte) code;
                                // if code has bit 9 set => move 2 spaces, because is escape code
                                outCur += 1 + ((code & FSST_CODE_BASE) >>> 8);
                                int symbolLen = code >>> FSST_LEN_BIT_OFFSET;
                                chunkCur += symbolLen;
                            } else if ((code & 0xFF) < byteLim) {
                                // 2 byte code after checking there is no longer pattern
                                outBuf[outCur++] = (byte) code;
                                chunkCur += 2;
                            } else {
                                // 1 byte code or miss.
                                outBuf[outCur] = (byte) code;
                                outCur += 1 + ((code & FSST_CODE_BASE) >>> 8); // predicated - tested with a branch, that was always worse
                                chunkCur++;
                            }
                        }
                    }
                } while ((chunkStart += chunkLen) < lineLen);
            }

            // set one more offset to provide last line length
            outOffsets[numLines] = outCur;
            return curLine;
        }

        public byte[] exportToBytes() {
            int totalStrLen = 0;
            for (int len = 1; len <= 8; len++) {
                char numWithLen = lenHisto[len - 1];
                totalStrLen += len * numWithLen;
            }

            int outLen = 8 + totalStrLen;  // 8 for len histo
            byte[] out = new byte[outLen];
            int offset = 0;
            for (char numWithLen : lenHisto) {
                out[offset++] = (byte) numWithLen;
            }

            int code = 0;
            // current order of the str lengths in codes
            for (int len : new int[] { 2, 3, 4, 5, 6, 7, 8, 1 }) {
                char numWithLen = lenHisto[len - 1];
                for (int i = 0; i < numWithLen; ++i) {
                    long str = getStr(symbols, code);
                    for (int byteIdx = 0; byteIdx < len; byteIdx++) {
                        out[offset++] = (byte) (str >>> (8 * byteIdx));
                    }
                    code++;
                }
            }

            return out;
        }
    }

    public static List<byte[]> makeSample(byte[] data, int[] offsets) {
        return makeSample(data, offsets, FSST_SAMPLETARGET, FSST_SAMPLELINE);
    }

    // quickly select a uniformly random set of lines such that we have between [FSST_SAMPLETARGET,FSST_SAMPLEMAXSZ) string bytes
    // return list of indices within input offsets?
    static List<byte[]> makeSample(byte[] data, int[] offsets, int sampleTargetLen, int sampleLineLen) {
        List<byte[]> sample = new ArrayList<>();
        int totalSize = offsets[offsets.length - 1];
        if (totalSize < sampleTargetLen) {
            for (int i = 0; i < offsets.length - 1; ++i) {
                sample.add(Arrays.copyOfRange(data, offsets[i], offsets[i + 1]));
            }
            return sample;
        }

        var random = new Random(456);
        int numLines = offsets.length - 1;
        int sampleSize = 0;
        while (sampleSize < sampleTargetLen) {
            int lineIdx = random.nextInt(numLines);

            // find next non-empty lines, wrapping around if necessary
            int len = offsets[lineIdx + 1] - offsets[lineIdx];
            while (len == 0) {
                if (++lineIdx == numLines) lineIdx = 0;
                len = offsets[lineIdx + 1] - offsets[lineIdx];
            }

            if (len <= sampleLineLen) {
                sample.add(Arrays.copyOfRange(data, offsets[lineIdx], offsets[lineIdx + 1]));
                sampleSize += len;
            } else {
                int chunks = len / sampleLineLen + (len % sampleLineLen == 0 ? 0 : 1);
                int chunk = random.nextInt(chunks);
                int off = chunk * sampleLineLen;
                int chunkLen = chunk == chunks - 1 ? len - off : sampleLineLen;
                byte[] bytes = Arrays.copyOfRange(data, offsets[lineIdx] + off, offsets[lineIdx] + off + chunkLen);
                sample.add(bytes);
                sampleSize += chunkLen;
            }
        }
        return sample;
    }

    static class Counters {
        char[] count1 = new char[FSST_CODE_MAX];  // array to count frequency of symbols as they occur in the sample

        char[] count2 = new char[FSST_CODE_MAX * FSST_CODE_MAX]; // array to count subsequent combinations of two symbols in the sample

        void count1Set(int pos1, char val) {
            count1[pos1] = val;
        }

        void count1Inc(int pos1) {
            count1[pos1]++;
        }

        void count2Inc(int pos1, int pos2) {
            count2[(pos1 << FSST_CODE_BITS) + pos2]++;
        }

        int count1Get(int pos1) {
            return count1[pos1];
        }

        int count2Get(int pos1, int pos2) {
            return count2[(pos1 << FSST_CODE_BITS) + pos2];
        }

        void clear() {
            Arrays.fill(count1, (char) 0);
            Arrays.fill(count2, (char) 0);
        }

        void copyInto(Counters other) {
            System.arraycopy(count1, 0, other.count1, 0, FSST_CODE_MAX);
            System.arraycopy(count2, 0, other.count2, 0, FSST_CODE_MAX * FSST_CODE_MAX);
        }
    }

    public static class Decoder {
        final byte[] lens; /* len[x] is the byte-length of the symbol x (1 < len[x] <= 8). */
        final long[] symbols; /* symbol[x] contains in LITTLE_ENDIAN the bytesequence that code x represents (0 <= x < 255). */

        Decoder(byte[] lens, long[] symbols) {
            this.lens = lens;
            this.symbols = symbols;
        }

        public static Decoder readFrom(byte[] exportedSymbolTable) throws IOException {
            final int[] i = { 0 };
            return readFrom(() -> exportedSymbolTable[i[0]++]);
        }

        public static Decoder readFrom(ByteReader in) throws IOException {
            int[] lenHisto = new int[8];
            int numSymbols = 0;
            for (int len = 1; len <= 8; len++) {
                int numWithLen = lenHisto[len - 1] = in.readByte() & 0xFF;
                numSymbols += numWithLen;
            }

            byte[] lens = new byte[numSymbols];
            long[] symbols = new long[numSymbols];
            int code = 0;
            for (int len : new int[] { 2, 3, 4, 5, 6, 7, 8, 1 }) {
                int numWithLen = lenHisto[len - 1];

                for (int i = 0; i < numWithLen; ++i) {
                    lens[code] = (byte) len;

                    long symbol = 0;
                    for (int byteIdx = 0; byteIdx < len; ++byteIdx) {
                        symbol |= (in.readByte() & 0xFFL) << (byteIdx * 8);
                    }
                    symbols[code] = symbol;
                    code++;
                }
            }

            return new Decoder(lens, symbols);
        }
    }

    // Assumes you know length to decompress
    // lenToConsume must not be longer than the compressed data length
    // output must be large enough to fit the
    // require that output buffer has 7 bytes more than required
    // return output length
    public static int decompress(byte[] in, int startOffset, int lenToConsume, Decoder decoder, byte[] output) throws IOException {
        int code;

        int outIdx = 0;
        int inIdx = startOffset;
        int limit = startOffset + lenToConsume;
        while (inIdx < limit) {
            if ((code = in[inIdx++] & 0xFF) == ESCAPE_BYTE) {
                output[outIdx++] = in[inIdx++];
            } else {
                var symbol = decoder.symbols[code];
                var len = decoder.lens[code];
                writeLong(output, outIdx, symbol);
                outIdx += len;
            }
        }

        return outIdx;
    }

    @SuppressWarnings({ "fallthrough", "checkstyle:OneStatementPerLine" })
    public static int decompressUnrolled(byte[] in, int lenToConsume, Decoder decoder, byte[] output) throws IOException {
        int posOut = 0;
        long limit = lenToConsume;
        int code;
        int offset = 0;
        while (offset + 4 <= limit) {
            int nextBlock = readInt(in, offset);
            int escapeMask = (nextBlock & 0x80808080) & ((((~nextBlock) & 0x7F7F7F7F) + 0x7F7F7F7F) ^ 0x80808080);
            if (escapeMask == 0) {
                code = nextBlock & 0xFF;
                nextBlock >>>= 8;
                writeLong(output, posOut, decoder.symbols[code]);
                posOut += decoder.lens[code];
                code = nextBlock & 0xFF;
                nextBlock >>>= 8;
                writeLong(output, posOut, decoder.symbols[code]);
                posOut += decoder.lens[code];
                code = nextBlock & 0xFF;
                nextBlock >>>= 8;
                writeLong(output, posOut, decoder.symbols[code]);
                posOut += decoder.lens[code];
                code = nextBlock & 0xFF;
                writeLong(output, posOut, decoder.symbols[code]);
                posOut += decoder.lens[code];
                offset += 4;
            } else {
                int firstEscapePos = Long.numberOfTrailingZeros((long) escapeMask) >> 3;
                switch (firstEscapePos) { /* Duff's device */
                    case 3:
                        code = nextBlock & 0xFF;
                        nextBlock >>>= 8;
                        offset++;
                        writeLong(output, posOut, decoder.symbols[code]);
                        posOut += decoder.lens[code];
                        // fall through
                    case 2:
                        code = nextBlock & 0xFF;
                        nextBlock >>>= 8;
                        offset++;
                        writeLong(output, posOut, decoder.symbols[code]);
                        posOut += decoder.lens[code];
                        // fall through
                    case 1:
                        code = nextBlock & 0xFF;
                        offset++;
                        writeLong(output, posOut, decoder.symbols[code]);
                        posOut += decoder.lens[code];
                        // fall through
                    case 0: /* decompress an escaped byte */
                        offset += 2;
                        output[posOut++] = in[offset - 1];
                }
            }
        }

        if (offset + 2 <= limit) {
            output[posOut] = in[offset + 1];
            if ((in[offset] & 0xFF) != ESCAPE_BYTE) {
                code = in[offset++] & 0xFF;
                writeLong(output, posOut, decoder.symbols[code]);
                posOut += decoder.lens[code];
                if ((in[offset] & 0xFF) != ESCAPE_BYTE) {
                    code = in[offset++] & 0xFF;
                    writeLong(output, posOut, decoder.symbols[code]);
                    posOut += decoder.lens[code];
                } else {
                    offset += 2;
                    output[posOut++] = in[offset - 1];
                }
            } else {
                offset += 2;
                posOut++;
            }
        }
        if (offset < limit) { // last code cannot be an escape
            code = in[offset++] & 0xFF;
            writeLong(output, posOut, decoder.symbols[code]);
            posOut += decoder.lens[code];
        }

        return posOut;
    }

    public static byte[] toBytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    public static String fromBytes(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static byte[] toByteArray(long str, int len) {
        byte[] arr = new byte[len];
        for (int i = 0; i < len; i++) {
            arr[i] = (byte) ((int) (str >>> (8 * i)));
        }
        return arr;
    }

    static String toString(long str, long icl) {
        var strRep = fromBytes(toByteArray(str, getLen(icl)));
        return "code: " + (int) getCode(icl) + ", len: " + getLen(icl) + ", str: '" + strRep + "'";
    }

    static String toString(long[] table, int idx) {
        return toString(getStr(table, idx), getICL(table, idx));
    }

    static String printStr(long[] table, int idx) {
        return fromBytes(toByteArray(getStr(table, idx), getLen(getICL(table, idx))));
    }

    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 100; i++) {
            roundTrip(args[0]);
        }
    }

    public static void roundTrip(String fileName) throws IOException {
        String content = Files.readString(Path.of(fileName), StandardCharsets.UTF_8);

        System.out.println("String length: " + content.length());

        byte[] bytes = FSST.toBytes(content);
        byte[] bytes2 = new byte[bytes.length + 8];
        System.arraycopy(bytes, 0, bytes2, 0, bytes.length);
        int[] offsets = { 0, bytes.length };
        bytes = bytes2;

        byte[] outBuf = new byte[bytes.length];
        int[] outOffsets = new int[2];

        List<byte[]> sample = FSST.makeSample(bytes, offsets);
        var symbolTable = SymbolTable.buildSymbolTable(sample);

        long startComp = System.nanoTime();
        long linesCompressed = symbolTable.compressBulk(1, bytes, offsets, outBuf, outOffsets);
        long endComp = System.nanoTime();

        assert linesCompressed == 1;
        long compressedLen = outOffsets[1];

        byte[] symbolTableBytes = symbolTable.exportToBytes();
        Decoder decoder = Decoder.readFrom(symbolTableBytes);

        long startDec = System.nanoTime();
        byte[] decompressBuf = new byte[bytes.length + 8];
        var decoded = FSST.decompress(outBuf, 0, outOffsets[1], decoder, decompressBuf);
        long endDec = System.nanoTime();

        String uncompressedString = FSST.fromBytes(Arrays.copyOfRange(decompressBuf, 0, decoded));
        assert content.equals(uncompressedString);

        System.out.println("Comp Duration: " + (endComp - startComp) / 1e6 + "ms");
        System.out.println("Dec  Duration: " + (endDec - startDec) / 1e6 + "ms");

        long compressMs = endComp - startComp;
        float compressMb = (float) bytes.length / (1 << 20);
        double compressMbPerSec = compressMb * 1e9 / compressMs;
        System.out.println("Comp rate: " + compressMbPerSec + " mb/s");

        long decMs = endDec - startDec;
        float decMb = (float) outOffsets[1] / (1 << 20);
        double decMbPerSec = decMb * 1e9 / decMs;
        System.out.println("Dec rate: " + decMbPerSec + " mb/s");

        System.out.println("Original length: " + bytes.length);
        System.out.println("Compressed length: " + compressedLen);
        System.out.println("Compressed ratio: " + compressedLen / (float) bytes.length);
    }

    public interface ByteReader {
        byte readByte() throws IOException;
    }

    public interface OffsetWriter {
        void addLen(int len) throws IOException;
    }
}
