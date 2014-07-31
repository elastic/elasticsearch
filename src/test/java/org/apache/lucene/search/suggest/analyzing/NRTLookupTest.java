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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static com.carrotsearch.randomizedtesting.RandomizedTest.*;
import static org.hamcrest.Matchers.*;

public class NRTLookupTest extends LuceneTestCase {

    private String generateRandomSuggestions(int len) {

        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            char c = randomUnicodeOfLengthBetween(1, 1).charAt(0);
            while(isReservedChar(c)) {
                c = randomUnicodeOfLengthBetween(1,1).charAt(0);
            }
            chars[i] = c;
        }
        return new String(chars);
    }

    private static boolean isReservedChar(char c) {
        switch(c) {
            case  NRTSuggesterBuilder.END_BYTE:
            case  NRTSuggesterBuilder.SEP_LABEL:
            case  NRTSuggesterBuilder.HOLE_CHARACTER:
                return true;
            default:
                return false;
        }
    }

    @Test
    public void testSameAnalyzedMultipleSurfaceForm() throws Exception {
        final StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
        CompletionProvider completionProvider = new CompletionProvider(standardAnalyzer);
        String[] inputs = {"BMW", "BMW ", "bmw", "bmw "};
        long[] weights = {10l, 4l, 2l, 5l};
        completionProvider.indexCompletions(inputs, weights);

        final IndexReader reader = completionProvider.getReader();
        final Map.Entry<SegmentLookup.LongBased, LeafReader> lookup = completionProvider.getLookup(reader);
        final SegmentLookup.LongBased segmentLookup = lookup.getKey();
        final LeafReader leafReader = lookup.getValue();

        List<DefaultCollector<Long>.Result<Long>> results = segmentLookup.lookup(leafReader, "b", 3, 4);

        assertResults(results,
                new ExpectedResult("bmw", 10l, 5l, 4l, 2l));

        results = segmentLookup.lookup(leafReader, "bmw", 3, 4);

        assertResults(results,
                new ExpectedResult("bmw", 10l, 5l, 4l, 2l));
    }

    @Test
    public void testSameSurfaceForms() throws Exception {
        final StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
        CompletionProvider completionProvider = new CompletionProvider(standardAnalyzer);
        String[] inputs = {"apple", "apple", "apple", "application"};
        long[] weights = {10l, 4l, 2l, 5l};
        completionProvider.indexCompletions(inputs, weights);

        final IndexReader reader = completionProvider.getReader();
        final Map.Entry<SegmentLookup.LongBased, LeafReader>  lookup = completionProvider.getLookup(reader);
        final SegmentLookup.LongBased segmentLookup = lookup.getKey();
        final LeafReader leafReader = lookup.getValue();

        // lookup for 'app'
        List<DefaultCollector<Long>.Result<Long>> results = segmentLookup.lookup(leafReader, "app", 3, 3);

        assertResults(results,
                new ExpectedResult("apple", 10l, 4l, 2l),
                new ExpectedResult("application", 5l));
    }

    @Test
    public void testSimple() throws Exception {
        final StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
        CompletionProvider completionProvider = new CompletionProvider(standardAnalyzer);
        String[] inputs = {"apple", "apeal", "apples", "application"};
        long[] weights = {1l, 10l, 3l, 5l};
        completionProvider.indexCompletions(inputs, weights);

        final IndexReader reader = completionProvider.getReader();
        final Map.Entry<SegmentLookup.LongBased, LeafReader>  lookup = completionProvider.getLookup(reader);
        final SegmentLookup.LongBased segmentLookup = lookup.getKey();
        final LeafReader leafReader = lookup.getValue();

        List<DefaultCollector<Long>.Result<Long>> results = segmentLookup.lookup(leafReader, "app", 3);

        assertResults(results,
                new ExpectedResult("application", 5l),
                new ExpectedResult("apples", 3l),
                new ExpectedResult("apple", 1l));
        assertThat(results.size(), equalTo(3));

        results = segmentLookup.lookup(leafReader, "ap", 3);

        assertResults(results,
                new ExpectedResult("apeal", 10l),
                new ExpectedResult("application", 5l),
                new ExpectedResult("apples", 3l));
    }

    @Test
    public void testMultipleLookup() throws Exception {
        final StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
        String[] firstInput = {"food", "fun", "foobar"};
        long[] firstWeight = {1l, 3l, 5l};
        String[] secondInput = {"foo", "fooo", "foobar"};
        long[] secondWeight = {1l, 10l, 3l};
        CompletionProvider firstCompletion = new CompletionProvider(standardAnalyzer);
        firstCompletion.indexCompletions(firstInput, firstWeight);
        CompletionProvider secondCompletion = new CompletionProvider(standardAnalyzer);
        secondCompletion.indexCompletions(secondInput, secondWeight);

        final IndexReader firstReader = firstCompletion.getReader();
        Map.Entry<SegmentLookup.LongBased, LeafReader>  lookup = firstCompletion.getLookup(firstReader);
        final SegmentLookup.LongBased firstLookup = lookup.getKey();
        final LeafReader leafReader = lookup.getValue();

        final IndexReader secondReader = secondCompletion.getReader();
        lookup = secondCompletion.getLookup(secondReader);
        final SegmentLookup.LongBased segmentLookup = lookup.getKey();
        final LeafReader secondleafReader = lookup.getValue();

        List<DefaultCollector<Long>.Result<Long>> results = firstLookup.lookup(leafReader, "foo", 3);

        assertResults(results,
                new ExpectedResult("foobar", 5l),
                new ExpectedResult("food", 1l));

        results = segmentLookup.lookup(leafReader, "foo", 3);

        assertResults(results,
                new ExpectedResult("fooo", 10l),
                new ExpectedResult("foobar", 3l),
                new ExpectedResult("foo", 1l));
    }
    private static class ExpectedResult {
        private final String key;
        private final long[] weights;

        public ExpectedResult(String key, long... weights) {
            this.key = key;
            this.weights = weights;
        }

    }

    public void assertResults(List<DefaultCollector<Long>.Result<Long>> results, ExpectedResult... expectedResults) {
        assertThat(results.size(), equalTo(expectedResults.length));
        int i = 0;
        for (DefaultCollector<Long>.Result<Long> result : results) {
            ExpectedResult expectedResult = expectedResults[i];
            assertThat(result.key().toString(), equalTo(expectedResult.key));
            assertThat(result.resultMetaDataList().size(), equalTo(expectedResult.weights.length));
            int count = 0;
            for (SegmentLookup.Collector.ResultMetaData<Long> longResultMetaData : result.resultMetaDataList()) {
                assertThat(longResultMetaData.score(), equalTo(expectedResult.weights[count]));
                count++;
            }
            i++;
        }
    }

    @Test
    public void testDuplicateSurfaceForm() throws Exception {
        String prefixStr = generateRandomSuggestions(randomIntBetween(4, 6));
        int num = scaledRandomIntBetween(256, 800);
        final String[] titles = new String[num];
        final long[] weights = new long[num];
        String suffix = generateRandomSuggestions(randomIntBetween(1, 2));//scaledRandomIntBetween(30, 100)));

        for(int i = 0; i < num; i++) {
            titles[i] = prefixStr + suffix;
            weights[i] = num - i;
        }

        CompletionProvider completionProvider = new CompletionProvider(new StandardAnalyzer());
        completionProvider.indexCompletions(titles, weights);

        final IndexReader reader = completionProvider.getReader();
        final Map.Entry<SegmentLookup.LongBased, LeafReader>  lookup = completionProvider.getLookup(reader);
        final SegmentLookup.LongBased segmentLookup = lookup.getKey();
        final LeafReader leafReader = lookup.getValue();

        final StringBuilder builder = new StringBuilder();
        analyze(new StandardAnalyzer().tokenStream("foo", prefixStr + suffix), new TokenConsumer() {
            @Override
            public void nextToken() throws IOException {
                builder.append(this.charTermAttr.toString());
            }
        });
        assertResults(segmentLookup.lookup(leafReader, prefixStr, 1, num),
                new ExpectedResult(builder.toString(), weights));

    }

    @Test
    public void testDefaultScoring() throws Exception {
        String prefixStr = generateRandomSuggestions(randomIntBetween(4, 6));
        int num = scaledRandomIntBetween(500, 1000);
        final String[] titles = new String[num];
        final long[] weights = new long[num];

        String suffix = generateRandomSuggestions(randomIntBetween(4, scaledRandomIntBetween(30, 100)));

        for (int i = 0; i < titles.length; i++) {
            titles[i] = prefixStr + suffix;
            if (rarely()) {
                suffix = generateRandomSuggestions(randomIntBetween(4, scaledRandomIntBetween(30, 100)));
            }
            weights[i] = between(0, 1000);
        }

        CompletionProvider completionProvider = new CompletionProvider(new StandardAnalyzer());
        completionProvider.indexCompletions(titles, weights);

        final IndexReader reader = completionProvider.getReader();
        final Map.Entry<SegmentLookup.LongBased, LeafReader>  lookup = completionProvider.getLookup(reader);
        final SegmentLookup.LongBased segmentLookup = lookup.getKey();
        final LeafReader leafReader = lookup.getValue();
        for (String title : titles) {
            int res = between(1, 10);
            final StringBuilder builder = new StringBuilder();
            analyze(new StandardAnalyzer().tokenStream("foo", title), new TokenConsumer() {
                @Override
                public void nextToken() throws IOException {
                    if (builder.length() == 0) {
                        builder.append(this.charTermAttr.toString());
                    }
                }
            });
            String firstTerm = builder.toString();
            String prefix = firstTerm.isEmpty() ? "" : firstTerm.substring(0, between(1, firstTerm.length()));

            final List<DefaultCollector<Long>.Result<Long>> results = segmentLookup.lookup(leafReader, prefix, res);

            assertThat(results.size(), greaterThan(0));
            long topScorePerKey = -1;
            for (DefaultCollector<Long>.Result<Long> result : results) {
                long innerTopScore = -1;
                for (SegmentLookup.Collector.ResultMetaData<Long> longResultMetaData : result.resultMetaDataList()) {
                    if (topScorePerKey == -1) {
                        topScorePerKey = longResultMetaData.score();
                    } else {
                        assertThat(longResultMetaData.score(), lessThanOrEqualTo(topScorePerKey));
                    }
                    if (innerTopScore == -1) {
                        innerTopScore = longResultMetaData.score();
                    } else {
                        assertThat(longResultMetaData.score(), lessThanOrEqualTo(innerTopScore));
                        innerTopScore = longResultMetaData.score();
                    }
                }
            }
        }

    }

    private class CompletionProvider {
        final IndexWriterConfig indexWriterConfig;
        IndexWriter writer;
        int deletedDocCounter = 0;
        RAMDirectory dir = new RAMDirectory();
        Map<String, List<Integer>> termsToDocID = new HashMap<>();
        Set<Integer> docIDSet = new HashSet<>();
        final LookupFieldGenerator fieldGenerator;


        public CompletionProvider(final Analyzer indexAnalyzer) throws IOException {
            fieldGenerator = LookupFieldGenerator.create("foo", indexAnalyzer, false, false);
            Codec filterCodec = new Lucene50Codec() {
                @Override
                public PostingsFormat getPostingsFormatForField(String field) {
                    if ("foo".equals(field)) {
                        return new LookupPostingsFormat(super.getPostingsFormatForField(field));
                    }
                    return super.getPostingsFormatForField(field);
                }
            };
            this.indexWriterConfig = new IndexWriterConfig(indexAnalyzer);
            indexWriterConfig.setCodec(filterCodec);
        }

        private Field makeField(String name, Object value, Class type) throws Exception {
            if (type == String.class) {
                return new StoredField(name, (String) value);
            } else if (type == BytesRef.class) {
                return new StoredField(name, (BytesRef) value);
            } else if (type == Integer.class) {
                return new StoredField(name, (int) value);
            } else if (type == Float.class) {
                return new StoredField(name, (float) value);
            } else if (type == Double.class) {
                return new StoredField(name, (double) value);
            } else if (type == Long.class) {
                return new StoredField(name, (long) value);
            }
            throw new Exception("Unsupported Type "+ type);
        }

        private void indexCompletions(String[] terms, long[] weights, String[] storedFieldNames, Class[] types, Object[]... storedFieldValues) throws Exception {
            writer = new IndexWriter(dir, indexWriterConfig);
            for (int i = 0; i < weights.length; i++) {
                Document doc = new Document();
                doc.add(fieldGenerator.generate(terms[i], weights[i]));
                for (int j = 0; j < storedFieldNames.length; j++) {
                    doc.add(makeField(storedFieldNames[j], storedFieldValues[i][j], types[j]));
                }
                if (randomBoolean()) {
                    writer.commit();
                }
                writer.addDocument(doc);
                writer.forceMerge(1, true);
                writer.commit();
                DirectoryReader reader = DirectoryReader.open(writer, true);
                for (LeafReaderContext ctx : reader.leaves()) {
                    LeafReader atomicReader = ctx.reader();
                    for (int docID = 0; docID < atomicReader.numDocs(); docID++) {
                        if (!docIDSet.contains(docID + ctx.docBase)) {
                            docIDSet.add(docID + ctx.docBase);
                            List<Integer> docIDs = termsToDocID.get(terms[i]);
                            if (docIDs == null) {
                                docIDs = new ArrayList<>();
                            }
                            docIDs.add(docID + ctx.docBase);
                            termsToDocID.put(terms[i], docIDs);
                            break;
                        }
                    }
                }
                reader.close();
            }
            writer.commit();

            assertThat(docIDSet.size(), equalTo(weights.length));
        }

        public void indexCompletions(String[] terms, long[] weights) throws Exception {
            indexCompletions(terms, weights, new String[0], new Class[0]);
        }

        public void deleteDoc(IndexReader reader, String term) throws IOException {
            assertThat(reader.numDeletedDocs(), equalTo(deletedDocCounter));
            List<Integer> docIDs = termsToDocID.get(term);
            assert docIDs != null;
            assertThat(docIDs.size(), greaterThanOrEqualTo(1));
            int docID = docIDs.remove(randomIntBetween(0, docIDs.size() - 1));
            int initialNumDocs = writer.numDocs();
            assertTrue(writer.tryDeleteDocument(reader, docID));
            writer.commit();
            assertThat(initialNumDocs, equalTo(writer.numDocs() + 1));
            deletedDocCounter++;
        }

        public IndexReader getReader() throws IOException {
            if (writer != null) {
                return DirectoryReader.open(writer, true);
            } else {
                return null;
            }
        }

        public Map.Entry<SegmentLookup.LongBased, LeafReader> getLookup(IndexReader reader) throws IOException {
            assertThat(reader.leaves().size(), equalTo(1));
            LeafReader atomicReader = reader.leaves().get(0).reader();
            Terms luceneTerms = atomicReader.terms("foo");
            SegmentLookup.LongBased lookup = null;
            if (luceneTerms instanceof LookupPostingsFormat.LookupTerms) {
                lookup = ((LookupPostingsFormat.LookupTerms) luceneTerms).longScoreLookup();
            }
            assertFalse(lookup == null);
            return new AbstractMap.SimpleEntry<>(lookup, atomicReader);
        }

        public void close() throws IOException {
            writer.close();
            dir.close();
        }
    }


    public static int analyze(TokenStream stream, TokenConsumer consumer) throws IOException {
        stream.reset();
        consumer.reset(stream);
        int numTokens = 0;
        while (stream.incrementToken()) {
            consumer.nextToken();
            numTokens++;
        }
        consumer.end();
        stream.close();
        return numTokens;
    }


    public static abstract class TokenConsumer {
        protected CharTermAttribute charTermAttr;
        protected PositionIncrementAttribute posIncAttr;
        protected OffsetAttribute offsetAttr;

        public void reset(TokenStream stream) {
            charTermAttr = stream.addAttribute(CharTermAttribute.class);
            posIncAttr = stream.addAttribute(PositionIncrementAttribute.class);
            offsetAttr = stream.addAttribute(OffsetAttribute.class);
        }

        protected BytesRef fillBytesRef(BytesRefBuilder spare) {
            spare.copyChars(charTermAttr);
            return spare.get();
        }

        public abstract void nextToken() throws IOException;

        public void end() {}
    }
}
