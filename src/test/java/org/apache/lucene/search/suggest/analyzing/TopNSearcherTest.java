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

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.*;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.fst.*;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TopNSearcherTest extends LuceneTestCase {

    @Test
    public void testSimple() throws Exception {
        Entry[] inputs = {new Entry("term1", 1l, 1), new Entry("term1", 2l, 2), new Entry("term2", 3l, 3)};
        Map.Entry<Integer, FST<PairOutputs.Pair<Long, BytesRef>>> build = build(outputConfiguration, inputs);
        TestTopNSearcher testTopNSearcher = new TestTopNSearcher(build.getValue(), 2, 2, build.getKey());
        Map<BytesRef, List<Entry>> result = query(testTopNSearcher, "term");
        assertResults(mapByTerm(inputs), result);
    }

    @Test
    public void testLeafPruning() throws Exception {
        int nterms = 10;
        int nLeaf = 10;
        Entry[] inputs = new Entry[nterms * nLeaf];
        for (int i = 0; i < nterms; i++) {
            String term = "term_" + String.valueOf(i);
            for (int l = 0; l < nLeaf; l++) {
                inputs[(i * nterms) + l] = new Entry(term, (i * nterms) + l, (i * nterms) + l);
            }
        }
        int pruneNFirstLeaves = 3;

        Map.Entry<Integer, FST<PairOutputs.Pair<Long, BytesRef>>> build = build(outputConfiguration, inputs);
        TestTopNSearcher testTopNSearcher = new TestTopNSearcher(build.getValue(), nterms, nLeaf, build.getKey(), pruneNFirstLeaves);
        Map<BytesRef, List<Entry>> result = query(testTopNSearcher, "term");
        Map<BytesRef, List<Entry>> stringListMap = mapByTerm(inputs);
        for (BytesRef term : stringListMap.keySet()) {
            List<Entry> entries = stringListMap.get(term);
            stringListMap.put(term, entries.subList(pruneNFirstLeaves, entries.size()));
        }
        assertResults(stringListMap, result);
    }

    @Test
    public void testFullTermSearch() throws Exception {
        Entry[] inputs = {new Entry("term1", 1l, 1), new Entry("term1", 2l, 2), new Entry("term1", 3l, 3)};
        Map.Entry<Integer, FST<PairOutputs.Pair<Long, BytesRef>>> build = build(outputConfiguration, inputs);
        TestTopNSearcher testTopNSearcher = new TestTopNSearcher(build.getValue(), 1, 3, build.getKey());
        Map<BytesRef, List<Entry>> result = query(testTopNSearcher, "term1");
        assertResults(mapByTerm(inputs), result);
    }

    private void assertResults(Map<BytesRef, List<Entry>> expected, Map<BytesRef, List<Entry>> actual) {
        assertEquals(expected.size(), actual.size());
        for (Map.Entry<BytesRef, List<Entry>> expectedEntrySet : expected.entrySet()) {
            List<Entry> actualEntries = actual.get(expectedEntrySet.getKey());
            assertNotNull(actualEntries);
            assertEquals(expectedEntrySet.getValue().size(), actualEntries.size());
            int i = 0;
            for (Entry expectedEntry : expectedEntrySet.getValue()) {
                Entry actualEntry = actualEntries.get(i);
                assertEquals(expectedEntry.term, actualEntry.term);
                assertEquals(expectedEntry.weight, actualEntry.weight);
                assertEquals(expectedEntry.docID, actualEntry.docID);
                i++;
            }
        }
    }

    private static Map<BytesRef, List<Entry>> query(TestTopNSearcher searcher, String term) throws IOException {
        Automaton automaton = Automata.makeString(term);
        List<FSTUtil.Path<PairOutputs.Pair<Long, BytesRef>>> paths = FSTUtil.intersectPrefixPaths(automaton, searcher.fst);
        for (FSTUtil.Path<PairOutputs.Pair<Long, BytesRef>> path : paths) {
            searcher.addStartPaths(path.fstNode, path.output, false, path.input);
        }
        searcher.search();
        return searcher.getResults();
    }

    ContextAwareScorer.OutputConfiguration<Long> outputConfiguration = new ContextAwareScorer.OutputConfiguration<Long>() {
        @Override
        public Outputs<Long> outputSingleton() {
            return PositiveIntOutputs.getSingleton();
        }

        @Override
        public Long encode(Long input) {
            if (input < 0 || input > Integer.MAX_VALUE) {
                throw new UnsupportedOperationException("cannot encode value: " + input);
            }
            return Integer.MAX_VALUE - input;
        }

        @Override
        public Long decode(Long output) {
            return (Integer.MAX_VALUE - output);
        }
    };

    private static class Entry {
        private final String term;
        private final long weight;
        private final int docID;

        public Entry(String term, long weight, int docID) {
            this.term = term;
            this.weight = weight;
            this.docID = docID;
        }

    }

    private Map<BytesRef, List<Entry>> mapByTerm(Entry... entries) {
        Map<BytesRef, List<Entry>> entriesByTerm = new TreeMap<>(new Comparator<BytesRef>() {
            @Override
            public int compare(BytesRef o1, BytesRef o2) {
                return o1.compareTo(o2);
            }
        });
        for (Entry entry : entries) {
            final List<Entry> entryList;
            BytesRef entryTerm = new BytesRef(entry.term);
            if (!entriesByTerm.containsKey(entryTerm)) {
                entryList = new ArrayList<>();
                entriesByTerm.put(entryTerm, entryList);
            } else {
                entryList = entriesByTerm.get(entryTerm);
            }
            entryList.add(entry);
        }

        for (Map.Entry<BytesRef, List<Entry>> stringListEntry : entriesByTerm.entrySet()) {

            Collections.sort(stringListEntry.getValue(), new Comparator<Entry>() {
                @Override
                public int compare(Entry o1, Entry o2) {
                    return Long.compare(o2.weight, o1.weight);
                }
            });
        }
        return entriesByTerm;

    }

    private Map.Entry<Integer, FST<PairOutputs.Pair<Long, BytesRef>>> build(ContextAwareScorer.OutputConfiguration<Long> outputConfiguration, Entry... entries) throws IOException {
      NRTSuggesterBuilder.FSTBuilder<Long> builder = new NRTSuggesterBuilder.FSTBuilder<>(outputConfiguration, NRTSuggesterBuilder.PAYLOAD_SEP, NRTSuggesterBuilder.END_BYTE);
        Map<BytesRef, List<Entry>> entriesByTerm = mapByTerm(entries);
        int maxFormPerLeaf = 0;
        for (Map.Entry<BytesRef, List<Entry>> termEntries : entriesByTerm.entrySet()) {
            BytesRef term = termEntries.getKey();
            builder.startTerm(term);
            for (Entry entry : termEntries.getValue()) {
                builder.addEntry(entry.docID, term, entry.weight);
            }
            builder.finishTerm();
            maxFormPerLeaf = Math.max(maxFormPerLeaf, termEntries.getValue().size());
        }

        return new AbstractMap.SimpleEntry<>(maxFormPerLeaf, builder.build());
    }

    private class TestTopNSearcher extends TopNSearcher<PairOutputs.Pair<Long, BytesRef>> {

        private Map<BytesRef, List<Entry>> results = new HashMap<>();
        private List<Entry> currentResults = new ArrayList<>();

        private BytesRefBuilder currentKeyBuilder = new BytesRefBuilder();
        private CharsRefBuilder spare = new CharsRefBuilder();
        private final FST<PairOutputs.Pair<Long, BytesRef>> fst;
        private final int pruneFirstNLeaves;
        private int leafCount;

        public TestTopNSearcher(FST<PairOutputs.Pair<Long, BytesRef>> fst, int topN, int nLeaf, int maxQueueDepth) {
            this(fst, topN, nLeaf, maxQueueDepth, 0);
        }
        public TestTopNSearcher(FST<PairOutputs.Pair<Long, BytesRef>> fst, int topN, int nLeaf, int maxQueueDepth, int pruneFirstNLeaves) {
            super(fst, topN, nLeaf, NRTSuggesterBuilder.END_BYTE, topN * maxQueueDepth, new Comparator<PairOutputs.Pair<Long, BytesRef>>() {
                @Override
                public int compare(PairOutputs.Pair<Long, BytesRef> o1, PairOutputs.Pair<Long, BytesRef> o2) {
                    return o1.output1.compareTo(o2.output1);
                }
            });
            this.fst = fst;
            this.pruneFirstNLeaves = pruneFirstNLeaves;
        }

        @Override
        protected void onLeafNode(IntsRef input, PairOutputs.Pair<Long, BytesRef> output) {
            Util.toBytesRef(input, currentKeyBuilder);
            leafCount = 0;
        }

        @Override
        protected boolean collectOutput(PairOutputs.Pair<Long, BytesRef> output) throws IOException {
            leafCount++;
            if (leafCount <= pruneFirstNLeaves) {
                return false;
            }
            long weight = outputConfiguration.decode(output.output1);

            int surfaceFormLen = -1;
            for (int i = 0; i < output.output2.length; i++) {
                if (output.output2.bytes[output.output2.offset + i] == NRTSuggesterBuilder.PAYLOAD_SEP) {
                    surfaceFormLen = i;
                    break;
                }
            }
            assert surfaceFormLen != -1 : "no payloadSep found, unable to determine surface form";
            spare.copyUTF8Bytes(output.output2.bytes, output.output2.offset, surfaceFormLen);
            ByteArrayDataInput input = new ByteArrayDataInput(output.output2.bytes, surfaceFormLen + output.output2.offset + 1, output.output2.length - (surfaceFormLen + output.output2.offset));
            currentResults.add(new Entry(spare.toString(), weight, input.readVInt()));
            return true;
        }

        @Override
        protected void onFinishNode() throws IOException {
            results.put(currentKeyBuilder.toBytesRef(), new ArrayList<>(currentResults));
            currentResults.clear();
        }

        public Map<BytesRef, List<Entry>> getResults() {
            return results;
        }
    }
}
