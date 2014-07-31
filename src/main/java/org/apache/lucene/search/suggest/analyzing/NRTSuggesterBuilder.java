/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.search.suggest.analyzing;

import org.apache.lucene.search.suggest.analyzing.ContextAwareScorer.OutputConfiguration;
import org.apache.lucene.search.suggest.analyzing.LookupPostingsFormat.LookupBuildConfiguration;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.*;
import org.apache.lucene.util.fst.*;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Builder for {@link org.apache.lucene.search.suggest.analyzing.NRTSuggester}
 *
 * @param <W> score type
 */
public class NRTSuggesterBuilder<W extends Comparable<W>> {
    public static final int SERIALIZE_PRESERVE_SEPARATORS = 1;
    public static final int SERIALIZE_PRESERVE_POSITION_INCREMENTS = 2;


    public static final int PAYLOAD_SEP = '\u001F';
    public static final int HOLE_CHARACTER = '\u001E';
    /**
     * Represents the separation between tokens, if
     * PRESERVE_SEP was specified
     */
    public static final int SEP_LABEL = '\u001F';

    /**
     * Marks end of the analyzed input and start of dedup
     * byte.
     */
    public static final int END_BYTE = 0x0;

    private int payloadSep = PAYLOAD_SEP;
    private int sepLabel = SEP_LABEL;
    private int endByte = END_BYTE;
    private int holeCharacter = HOLE_CHARACTER;

    private FSTBuilder<W> builder;
    private final PayloadProcessor<W> payloadProcessor;
    private final String scorerName;
    private final boolean preserveSep;
    private final boolean preservePositionIncrements;
    private int maxAnalyzedPathsPerLeaf = 0;

    private NRTSuggesterBuilder(ContextAwareScorer<W> contextAwareScorer, boolean preserveSep, boolean preservePositionIncrements, PayloadProcessor<W> payloadProcessor) {
        this.scorerName = contextAwareScorer.name();
        this.preserveSep = preserveSep;
        this.preservePositionIncrements = preservePositionIncrements;
        this.payloadProcessor = payloadProcessor;
        this.builder = new FSTBuilder<>(contextAwareScorer.getOutputConfiguration(), payloadSep, endByte);
        NRTSuggester.registerScorer(scorerName, contextAwareScorer);

    }

    /**
     * Constructs a {@code NRTSuggesterBuilder} from {@link LookupBuildConfiguration}
     */
    public static <W extends Comparable<W>> NRTSuggesterBuilder<W> fromConfiguration(LookupBuildConfiguration lookupBuildConfiguration) {
        return new NRTSuggesterBuilder<>(lookupBuildConfiguration.scorer,
                lookupBuildConfiguration.preserveSep,
                lookupBuildConfiguration.preservePositionIncrements,
                lookupBuildConfiguration.payloadProcessor);
    }

    /**
     * Initializes an FST input term to add entries against
     */
    public void startTerm(BytesRef term) {
        builder.startTerm(term);
    }

    /**
     * Adds an entry for the latest input term, should be called after
     * {@link #startTerm(org.apache.lucene.util.BytesRef)} on the desired input
     */
    public void addEntry(int docID, BytesRef payload) throws IOException {
        payloadProcessor.parse(payload);
        builder.addEntry(docID, payloadProcessor.surfaceForm(), payloadProcessor.weight());
    }

    /**
     * Writes all the entries for the FST input term
     */
    public void finishTerm() throws IOException {
        maxAnalyzedPathsPerLeaf = Math.max(maxAnalyzedPathsPerLeaf, builder.finishTerm());
    }

    /**
     * Builds and stores a FST that can be loaded with
     * {@link NRTSuggester#load(org.apache.lucene.store.IndexInput)}
     */
    public boolean store(DataOutput output) throws IOException {
        final FST<? extends PairOutputs.Pair<W, BytesRef>> build = builder.build();
        if (build == null) {
            return false;
        }
        output.writeString(scorerName);
        build.save(output);

        /* write some more meta-info */
        assert maxAnalyzedPathsPerLeaf > 0;
        output.writeVInt(maxAnalyzedPathsPerLeaf);
        int options = 0;
        options |= preserveSep ? SERIALIZE_PRESERVE_SEPARATORS : 0;
        options |= preservePositionIncrements ? SERIALIZE_PRESERVE_POSITION_INCREMENTS : 0;
        output.writeVInt(options);
        output.writeVInt(sepLabel);
        output.writeVInt(endByte);
        output.writeVInt(payloadSep);
        output.writeVInt(holeCharacter);
        return true;
    }

    final static class FSTBuilder<W extends Comparable<W>> {
        private PairOutputs<W, BytesRef> outputs;
        private Builder<PairOutputs.Pair<W, BytesRef>> builder;
        private final OutputConfiguration<W> outputConfiguration;
        private final IntsRefBuilder scratchInts = new IntsRefBuilder();
        private final BytesRefBuilder analyzed = new BytesRefBuilder();
        private final PriorityQueue<Entry<W>> entries;
        private final int payloadSep;
        private final int endByte;

        FSTBuilder(OutputConfiguration<W> outputConfiguration, int payloadSep, int endByte) {
            this.payloadSep = payloadSep;
            this.endByte = endByte;
            this.outputs = new PairOutputs<>(outputConfiguration.outputSingleton(), ByteSequenceOutputs.getSingleton());
            this.outputConfiguration = outputConfiguration;
            this.entries = new PriorityQueue<>();
            this.builder = new Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
        }


        void startTerm(BytesRef analyzed) {
            this.analyzed.copyBytes(analyzed);
            this.analyzed.append((byte) endByte);
        }

        void addEntry(int docID, BytesRef surface, W weight) throws IOException {
            BytesRef payloadRef = NRTSuggester.PayLoadProcessor.make(surface, docID, payloadSep);
            entries.add(new Entry<>(payloadRef, outputConfiguration.encode(weight)));
        }

        int finishTerm() throws IOException {
            int numArcs = 0;
            int numDedupBytes = 1;
            int entryCount = entries.size();
            analyzed.grow(analyzed.length() + 1);
            analyzed.setLength(analyzed.length() + 1);
            for (Entry<W> entry : entries) {
                //TODO: make this a balanced tree for context-aware scoring
                if (numArcs == maxNumArcsForDedupByte(numDedupBytes)) {
                    analyzed.setByteAt(analyzed.length() - 1, (byte) (numArcs));
                    analyzed.grow(analyzed.length() + 1);
                    analyzed.setLength(analyzed.length() + 1);
                    numArcs = 0;
                    numDedupBytes++;
                }
                analyzed.setByteAt(analyzed.length() - 1, (byte) numArcs++);
                Util.toIntsRef(analyzed.get(), scratchInts);
                builder.add(scratchInts.get(), outputs.newPair(entry.weight, entry.payload));
            }
            entries.clear();
            return entryCount;
        }

        /**
         * Num arcs for nth dedup byte:
         *  if n <= 5: 1 + (2 * n)
         *  else: (1 + (2 * n)) * n
         */
        private static int maxNumArcsForDedupByte(int currentNumDedupBytes) {
            int maxArcs = 1 + (2 * currentNumDedupBytes);
            if (currentNumDedupBytes > 5) {
                maxArcs *= currentNumDedupBytes;
            }
            return Math.min(maxArcs, 255);
        }

        FST<PairOutputs.Pair<W, BytesRef>> build() throws IOException {
            return builder.finish();
        }

        private final static class Entry<W extends Comparable<W>> implements Comparable<Entry<W>> {
            final BytesRef payload;
            final W weight;

            public Entry(BytesRef payload, W weight) {
                this.payload = payload;
                this.weight = weight;
            }

            @Override
            public int compareTo(Entry<W> o) {
                return weight.compareTo(o.weight);
            }
        }
    }
}
