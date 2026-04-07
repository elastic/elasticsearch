/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.Util;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A fork of Lucene's {@link SynonymMap.Builder} that integrates Elasticsearch's {@link CircuitBreaker}
 * into the {@link #build()} method. The original builder compiles all synonym rules into an FST without
 * any memory checks; this version periodically checks the circuit breaker during FST construction to
 * prevent unbounded memory growth.
 */
class ESSynonymMapBuilder {

    // Check the circuit breaker every 8192 rules during add() and every 1024 keys during build().
    private static final int ADD_CHECK_INTERVAL = 0x1FFF;
    private static final int BUILD_CHECK_INTERVAL = 0x3FF;

    private final HashMap<CharsRef, MapEntry> workingSet = new HashMap<>();
    private final BytesRefHash words = new BytesRefHash();
    private final BytesRefBuilder utf8Scratch = new BytesRefBuilder();
    private int maxHorizontalContext;
    private final boolean dedup;
    private final CircuitBreaker circuitBreaker;
    private int ruleCount = 0;

    private static class MapEntry {
        boolean includeOrig;
        // can't use Lucene's IntArrayList
        final List<Integer> ords = new ArrayList<>();
    }

    ESSynonymMapBuilder(boolean dedup, CircuitBreaker circuitBreaker) {
        this.dedup = dedup;
        this.circuitBreaker = circuitBreaker;
    }

    void add(CharsRef input, CharsRef output, boolean includeOrig) {
        int numInputWords = countWords(input);
        int numOutputWords = countWords(output);

        if (numInputWords <= 0) {
            throw new IllegalArgumentException("numInputWords must be > 0 (got " + numInputWords + ")");
        }
        if (input.length <= 0) {
            throw new IllegalArgumentException("input.length must be > 0 (got " + input.length + ")");
        }
        if (numOutputWords <= 0) {
            throw new IllegalArgumentException("numOutputWords must be > 0 (got " + numOutputWords + ")");
        }
        if (output.length <= 0) {
            throw new IllegalArgumentException("output.length must be > 0 (got " + output.length + ")");
        }

        if ((ruleCount++ & ADD_CHECK_INTERVAL) == 0) {
            circuitBreaker.addEstimateBytesAndMaybeBreak(0L, "Synonyms");
        }

        assert !hasHoles(input) : "input has holes: " + input;
        assert !hasHoles(output) : "output has holes: " + output;

        utf8Scratch.copyChars(output.chars, output.offset, output.length);
        int ord = words.add(utf8Scratch.get());
        if (ord < 0) {
            ord = (-ord) - 1;
        }

        MapEntry e = workingSet.get(input);
        if (e == null) {
            e = new MapEntry();
            workingSet.put(CharsRef.deepCopyOf(input), e);
        }

        e.ords.add(ord);
        e.includeOrig |= includeOrig;
        maxHorizontalContext = Math.max(maxHorizontalContext, numInputWords);
        maxHorizontalContext = Math.max(maxHorizontalContext, numOutputWords);
    }

    SynonymMap build() throws IOException {
        ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
        // TODO: default suffixRAMLimitMB is 32MB regardless of heap size -- consider scaling to circuit breaker limit
        FSTCompiler<BytesRef> fstCompiler = new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE4, outputs).build();

        BytesRefBuilder scratch = new BytesRefBuilder();
        ByteArrayDataOutput scratchOutput = new ByteArrayDataOutput();

        // can't use Lucene's IntHashSet
        final Set<Integer> dedupSet;
        if (dedup) {
            dedupSet = new HashSet<>();
        } else {
            dedupSet = null;
        }

        final byte[] spare = new byte[5];

        Set<CharsRef> keys = workingSet.keySet();
        CharsRef[] sortedKeys = keys.toArray(new CharsRef[0]);
        Arrays.sort(sortedKeys, CharsRef.getUTF16SortedAsUTF8Comparator());

        final IntsRefBuilder scratchIntsRef = new IntsRefBuilder();

        for (int keyIdx = 0; keyIdx < sortedKeys.length; keyIdx++) {
            // Check real memory circuit breaker during FST compilation.
            // Catches memory growth from FST construction for synonym sets that passed
            // through add() without tripping the breaker.
            if ((keyIdx & BUILD_CHECK_INTERVAL) == 0) {
                circuitBreaker.addEstimateBytesAndMaybeBreak(0L, "Synonyms");
            }

            CharsRef input = sortedKeys[keyIdx];
            MapEntry output = workingSet.get(input);

            int numEntries = output.ords.size();
            int estimatedSize = 5 + numEntries * 5;

            scratch.grow(estimatedSize);
            scratchOutput.reset(scratch.bytes());

            int count = 0;
            for (int i = 0; i < numEntries; i++) {
                if (dedupSet != null) {
                    int ent = output.ords.get(i);
                    if (dedupSet.contains(ent)) {
                        continue;
                    }
                    dedupSet.add(ent);
                }
                scratchOutput.writeVInt(output.ords.get(i));
                count++;
            }

            final int pos = scratchOutput.getPosition();
            scratchOutput.writeVInt(count << 1 | (output.includeOrig ? 0 : 1));
            final int pos2 = scratchOutput.getPosition();
            final int vIntLen = pos2 - pos;

            // spare is sized for a 32-bit VInt; resize if Lucene widens this encoding.
            assert vIntLen <= spare.length : "count VInt encoding exceeded spare buffer: " + vIntLen + " > " + spare.length;
            System.arraycopy(scratch.bytes(), pos, spare, 0, vIntLen);
            System.arraycopy(scratch.bytes(), 0, scratch.bytes(), vIntLen, pos);
            System.arraycopy(spare, 0, scratch.bytes(), 0, vIntLen);

            if (dedupSet != null) {
                dedupSet.clear();
            }

            scratch.setLength(scratchOutput.getPosition());
            fstCompiler.add(Util.toUTF32(input, scratchIntsRef), scratch.toBytesRef());
        }

        FST<BytesRef> fst = FST.fromFSTReader(fstCompiler.compile(), fstCompiler.getFSTReader());
        return new SynonymMap(fst, words, maxHorizontalContext);
    }

    private static boolean hasHoles(CharsRef chars) {
        final int end = chars.offset + chars.length;
        for (int idx = chars.offset + 1; idx < end; idx++) {
            if (chars.chars[idx] == SynonymMap.WORD_SEPARATOR && chars.chars[idx - 1] == SynonymMap.WORD_SEPARATOR) {
                return true;
            }
        }
        if (chars.chars[chars.offset] == SynonymMap.WORD_SEPARATOR) {
            return true;
        }
        if (chars.chars[chars.offset + chars.length - 1] == SynonymMap.WORD_SEPARATOR) {
            return true;
        }
        return false;
    }

    private static int countWords(CharsRef chars) {
        int wordCount = 1;
        int upto = chars.offset;
        final int limit = chars.offset + chars.length;
        while (upto < limit) {
            if (chars.chars[upto++] == SynonymMap.WORD_SEPARATOR) {
                wordCount++;
            }
        }
        return wordCount;
    }
}
