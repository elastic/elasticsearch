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

package org.elasticsearch.search.suggest.completion;

import com.google.common.collect.Lists;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.search.suggest.analyzing.XNRTSuggester;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.postingsformat.Elasticsearch090PostingsFormat;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PreBuiltPostingsFormatProvider;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.index.similarity.SimilarityProvider;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.context.ContextMapping;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.*;

public class NRTCompletionPostingsFormatTest extends ElasticsearchTestCase {

    private String generateRandomSuggestions(int len) {

        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            char c = randomUnicodeOfLengthBetween(1,1).charAt(0);
            while(NRTCompletionFieldMapper.isReservedChar(c)) {
                c = randomUnicodeOfLengthBetween(1,1).charAt(0);
            }
            chars[i] = c;
        }
        return new String(chars);
    }

    @Test
    public void testNRTDeletedDocFiltering() throws IOException {
        final boolean preserveSeparators = getRandom().nextBoolean();
        final boolean preservePositionIncrements = getRandom().nextBoolean();
        final boolean usePayloads = getRandom().nextBoolean();
        PostingsFormatProvider provider = new PreBuiltPostingsFormatProvider(new Elasticsearch090PostingsFormat());
        NamedAnalyzer namedAnalzyer = new NamedAnalyzer("foo", new StandardAnalyzer(TEST_VERSION_CURRENT));
        final NRTCompletionFieldMapper mapper = new NRTCompletionFieldMapper(new Names("foo"), namedAnalzyer, namedAnalzyer, provider, null, usePayloads,
                preserveSeparators, preservePositionIncrements, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING);

        String prefixStr = generateRandomSuggestions(randomIntBetween(2, 6));
        int num = scaledRandomIntBetween(200, 500);
        final String[] titles = new String[num];
        final long[] weights = new long[num];
        String suffix = generateRandomSuggestions(randomIntBetween(4, scaledRandomIntBetween(30, 100)));
        for (int i = 0; i < titles.length; i++) {
            boolean duplicate = rarely() && i != 0;
            suffix = (duplicate) ? suffix : generateRandomSuggestions(randomIntBetween(4, scaledRandomIntBetween(30, 100)));
            titles[i] = prefixStr + suffix;
            weights[i] = between(0, 100);
        }

        CompletionProvider completionProvider = new CompletionProvider(mapper);
        completionProvider.indexCompletions(titles, titles, weights);

        int res = between(10, num/10);
        final StringBuilder builder = new StringBuilder();
        SuggestUtils.analyze(namedAnalzyer.tokenStream("foo", prefixStr), new SuggestUtils.TokenConsumer() {
            @Override
            public void nextToken() throws IOException {
                if (builder.length() == 0) {
                    builder.append(this.charTermAttr.toString());
                }
            }
        });
        String firstTerm = builder.toString();
        String prefix = firstTerm.isEmpty() ? "" : firstTerm.substring(0, between(1, firstTerm.length()));

        // delete some suggestions
        Map<String, Integer> deletedTerms = new HashMap<>();
        IndexReader reader = completionProvider.getReader();
        Lookup lookup = completionProvider.getLookup(reader).v1();
        reader.close();
        assertTrue(lookup instanceof XNRTSuggester);
        XNRTSuggester suggester = (XNRTSuggester) lookup;

        List<LookupResult> lookupResults = suggester.lookup(prefix, null, false, res);

        for (LookupResult result : lookupResults) {
            if (randomBoolean()) {
                String key = result.key.toString();
                IndexReader indexReader = completionProvider.getReader();
                completionProvider.deleteDoc(indexReader, key);
                indexReader.close();
                Integer counter = deletedTerms.get(key);
                if (counter == null) {
                    counter = 0;
                }
                deletedTerms.put(key, ++counter);
            }
        }

        // check if deleted suggestions are suggested
        reader = completionProvider.getReader();
        Tuple<Lookup, AtomicReader> lookupAndReaderTuple = completionProvider.getLookup(reader);
        lookup = lookupAndReaderTuple.v1();
        AtomicReader atomicReader = lookupAndReaderTuple.v2();

        assertTrue(lookup instanceof XNRTSuggester);
        XNRTSuggester suggesterWithDeletes = (XNRTSuggester) lookup;

        lookupResults = suggesterWithDeletes.lookup(prefix, res, atomicReader);

        if (lookupResults.size() > 0) {
            for(LookupResult result : lookupResults) {
                final String key = result.key.toString();
                Integer counter = deletedTerms.get(key);
                if (counter != null) {
                    assertThat(counter, greaterThanOrEqualTo(0));
                    deletedTerms.put(key, counter);
                }
            }
        }
        reader.close();
        completionProvider.close();
    }

    @Test
    public void testDedupSurfaceForms() throws Exception {
        final boolean preserveSeparators = getRandom().nextBoolean();
        final boolean preservePositionIncrements = getRandom().nextBoolean();
        final boolean usePayloads = getRandom().nextBoolean();
        PostingsFormatProvider provider = new PreBuiltPostingsFormatProvider(new Elasticsearch090PostingsFormat());
        NamedAnalyzer namedAnalzyer = new NamedAnalyzer("foo", new StandardAnalyzer(TEST_VERSION_CURRENT));
        final NRTCompletionFieldMapper mapper = new NRTCompletionFieldMapper(new Names("foo"), namedAnalzyer, namedAnalzyer, provider, null, usePayloads,
                preserveSeparators, preservePositionIncrements, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING);

        String prefixStr = generateRandomSuggestions(randomIntBetween(4, 6));
        int num = scaledRandomIntBetween(50, 500); // Note 256 is the limit
        final String[] titles = new String[num];
        final long[] weights = new long[num];
        final String suffix = generateRandomSuggestions(randomIntBetween(4, scaledRandomIntBetween(30, 100)));
        long topScore = -1l;
        for (int i = 0; i < titles.length; i++) {
            titles[i] = prefixStr + suffix;
            weights[i] = between(0, 100);
            if (weights[i] > topScore) {
                topScore = weights[i];
            }
        }

        CompletionProvider completionProvider = new CompletionProvider(mapper);
        completionProvider.indexCompletions(titles, titles, weights);


        final StringBuilder builder = new StringBuilder();
        SuggestUtils.analyze(namedAnalzyer.tokenStream("foo", prefixStr), new SuggestUtils.TokenConsumer() {
            @Override
            public void nextToken() throws IOException {
                if (builder.length() == 0) {
                    builder.append(this.charTermAttr.toString());
                }
            }
        });
        String firstTerm = builder.toString();
        String prefix = firstTerm.isEmpty() ? "" : firstTerm.substring(0, between(1, firstTerm.length()));

        Map<String, Integer> deletedTerms = new HashMap<>(1);
        deletedTerms.put(prefixStr + suffix, Math.min(num, 256));

        // suggest for the same prefix n times, deleting one copy each time
        for (int i = 0; i < Math.min(num, 256); i++) {
            int res = between(1, Math.min(num, 256));
            IndexReader reader = completionProvider.getReader();
            final Tuple<Lookup, AtomicReader> lookupAndReader = completionProvider.getLookup(reader);
            Lookup lookup = lookupAndReader.v1();
            AtomicReader atomicReader = lookupAndReader.v2();
            assertTrue(lookup instanceof XNRTSuggester);
            XNRTSuggester suggester = (XNRTSuggester) lookup;
            List<LookupResult> lookupResults = suggester.lookup(prefix, res, atomicReader);
            reader.close();
            for (LookupResult result : lookupResults) {
                String key = result.key.toString();
                // check weight should be highest to lowest
                assertThat(result.value, lessThanOrEqualTo(topScore));
                topScore = result.value;
                IndexReader indexReader = completionProvider.getReader();
                completionProvider.deleteDoc(indexReader, key);
                indexReader.close();
                Integer counter = deletedTerms.get(key);
                assertThat(counter, notNullValue());
                assertThat(counter, greaterThan(0));
                deletedTerms.put(key, --counter);
            }
        }
        completionProvider.close();
    }


    @Test
    public void testDuellCompletions() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException {
        final boolean preserveSeparators = getRandom().nextBoolean();
        final boolean preservePositionIncrements = getRandom().nextBoolean();
        final boolean usePayloads = getRandom().nextBoolean();
        final int options = preserveSeparators ? AnalyzingSuggester.PRESERVE_SEP : 0;

        XAnalyzingSuggester reference = new XAnalyzingSuggester(new StandardAnalyzer(TEST_VERSION_CURRENT), null, new StandardAnalyzer(
                TEST_VERSION_CURRENT), options, 256, -1, preservePositionIncrements, null, false, 1, XAnalyzingSuggester.SEP_LABEL, XAnalyzingSuggester.PAYLOAD_SEP, XAnalyzingSuggester.END_BYTE, XAnalyzingSuggester.HOLE_CHARACTER);
        LineFileDocs docs = new LineFileDocs(getRandom());
        int num = scaledRandomIntBetween(150, 300);
        final String[] titles = new String[num];
        final long[] weights = new long[num];
        for (int i = 0; i < titles.length; i++) {
            Document nextDoc = docs.nextDoc();
            IndexableField field = nextDoc.getField("title");
            titles[i] = field.stringValue();
            weights[i] = between(0, 100);

        }
        docs.close();
        final InputIterator primaryIter = new InputIterator() {
            int index = 0;
            long currentWeight = -1;

            @Override
            public Comparator<BytesRef> getComparator() {
                return null;
            }

            @Override
            public BytesRef next() throws IOException {
                if (index < titles.length) {
                    currentWeight = weights[index];
                    return new BytesRef(titles[index++]);
                }
                return null;
            }

            @Override
            public long weight() {
                return currentWeight;
            }

            @Override
            public BytesRef payload() {
                return null;
            }

            @Override
            public boolean hasPayloads() {
                return false;
            }

            @Override
            public Set<BytesRef> contexts() {
                return null;
            }

            @Override
            public boolean hasContexts() {
                return false;
            }

        };
        InputIterator iter;
        if (usePayloads) {
            iter = new InputIterator() {
                @Override
                public long weight() {
                    return primaryIter.weight();
                }

                @Override
                public Comparator<BytesRef> getComparator() {
                    return primaryIter.getComparator();
                }

                @Override
                public BytesRef next() throws IOException {
                    return primaryIter.next();
                }

                @Override
                public BytesRef payload() {
                    return new BytesRef(Long.toString(weight()));
                }

                @Override
                public boolean hasPayloads() {
                    return true;
                }

                @Override
                public Set<BytesRef> contexts() {
                    return null;
                }

                @Override
                public boolean hasContexts() {
                    return false;
                }
            };
        } else {
            iter = primaryIter;
        }
        reference.build(iter);
        PostingsFormatProvider provider = new PreBuiltPostingsFormatProvider(new Elasticsearch090PostingsFormat());

        NamedAnalyzer namedAnalzyer = new NamedAnalyzer("foo", new StandardAnalyzer(TEST_VERSION_CURRENT));
        final CompletionFieldMapper mapper = new NRTCompletionFieldMapper(new Names("foo"), namedAnalzyer, namedAnalzyer, provider, null, usePayloads,
                preserveSeparators, preservePositionIncrements, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING);
        CompletionProvider completionProvider = new CompletionProvider(mapper);
        completionProvider.indexCompletions(titles, titles, weights);
        IndexReader reader = completionProvider.getReader();
        Lookup nrtLookup = completionProvider.getLookup(reader).v1();
        assertThat(nrtLookup, instanceOf(XNRTSuggester.class));
        reader.close();

        for (String title : titles) {
            int res = between(1, 10);
            final StringBuilder builder = new StringBuilder();
            SuggestUtils.analyze(namedAnalzyer.tokenStream("foo", title), new SuggestUtils.TokenConsumer() {
                @Override
                public void nextToken() throws IOException {
                    if (builder.length() == 0) {
                        builder.append(this.charTermAttr.toString());
                    }
                }
            });
            String firstTerm = builder.toString();
            String prefix = firstTerm.isEmpty() ? "" : firstTerm.substring(0, between(1, firstTerm.length()));
            List<LookupResult> refLookup = reference.lookup(prefix, false, res);
            List<LookupResult> lookup = nrtLookup.lookup(prefix, false, res);
            assertThat(refLookup.toString(), lookup.size(), equalTo(refLookup.size()));
            for (int j = 0; j < refLookup.size(); j++) {
                assertThat(lookup.get(j).key, equalTo(refLookup.get(j).key));
                assertThat("prefix: " + prefix + " " + j + " -- missmatch cost: " + lookup.get(j).key + " - " + lookup.get(j).value + " | " + refLookup.get(j).key + " - " + refLookup.get(j).value,
                        lookup.get(j).value, equalTo(refLookup.get(j).value));
                assertThat(lookup.get(j).payload, equalTo(refLookup.get(j).payload));
                if (usePayloads) {
                    assertThat(lookup.get(j).payload.utf8ToString(), equalTo(Long.toString(lookup.get(j).value)));
                }
            }
        }
        completionProvider.close();
    }

    private class CompletionProvider {
        final IndexWriterConfig indexWriterConfig;
        final CompletionFieldMapper mapper;
        IndexWriter writer;
        int deletedDocCounter = 0;
        RAMDirectory dir = new RAMDirectory();
        Map<String, List<Integer>> termsToDocID = new HashMap<>();
        Set<Integer> docIDSet = new HashSet<>();


        public CompletionProvider(final CompletionFieldMapper mapper) throws IOException {
            FilterCodec filterCodec = new FilterCodec("filtered", Codec.getDefault()) {
                public PostingsFormat postingsFormat() {
                    return mapper.postingsFormatProvider().get();
                }
            };
            this.indexWriterConfig = new IndexWriterConfig(TEST_VERSION_CURRENT, mapper.indexAnalyzer());
            indexWriterConfig.setCodec(filterCodec);
            this.mapper = mapper;
        }

        public void indexCompletions(String[] terms, String[] surfaces, long[] weights) throws IOException {
            writer = new IndexWriter(dir, indexWriterConfig);
            for (int i = 0; i < weights.length; i++) {
                Document doc = new Document();
                BytesRef payload = mapper.buildPayload(new BytesRef(surfaces[i]), weights[i], new BytesRef(Long.toString(weights[i])));
                doc.add(mapper.getCompletionField(ContextMapping.EMPTY_CONTEXT, terms[i], payload));
                if (randomBoolean()) {
                    writer.commit();
                }
                writer.addDocument(doc);
                writer.forceMerge(1, true);
                writer.commit();
                DirectoryReader reader = DirectoryReader.open(writer, true);
                for (AtomicReaderContext ctx : reader.leaves()) {
                    AtomicReader atomicReader = ctx.reader();
                    for (int docID = 0; docID < atomicReader.numDocs(); docID++) {
                        if (!docIDSet.contains(docID + ctx.docBase)) {
                            docIDSet.add(docID + ctx.docBase);
                            List<Integer> docIDs = termsToDocID.get(surfaces[i]);
                            if (docIDs == null) {
                                docIDs = new ArrayList<>();
                            }
                            docIDs.add(docID + ctx.docBase);
                            termsToDocID.put(surfaces[i], docIDs);
                            break;
                        }
                    }
                }
                reader.close();
            }
            writer.commit();

            assertThat(docIDSet.size(), equalTo(weights.length));

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

        public Tuple<Lookup, AtomicReader> getLookup(IndexReader reader) throws IOException {
            assertThat(reader.leaves().size(), equalTo(1));
            AtomicReader atomicReader = reader.leaves().get(0).reader();
            Terms luceneTerms = atomicReader.terms(mapper.name());
            Lookup lookup = null;
            if (luceneTerms instanceof Completion090PostingsFormat.CompletionTerms) {
                lookup = ((Completion090PostingsFormat.CompletionTerms) luceneTerms).getLookup(mapper, new CompletionSuggestionContext(null));
            }
            assertFalse(lookup == null);
            return new Tuple<>(lookup, atomicReader);
        }

        public void close() throws IOException {
            writer.close();
            dir.close();
        }
    }

   static class NRTCompletionFieldMapper extends CompletionFieldMapper {

       public NRTCompletionFieldMapper(Names names, NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer, PostingsFormatProvider postingsProvider, SimilarityProvider similarity, boolean payloads, boolean preserveSeparators, boolean preservePositionIncrements, int maxInputLength, MultiFields multiFields, CopyTo copyTo, SortedMap<String, ContextMapping> contextMappings) {
           super(names, indexAnalyzer, searchAnalyzer, postingsProvider, similarity, payloads, preserveSeparators, preservePositionIncrements, maxInputLength, multiFields, copyTo, contextMappings);
       }

       @Override
       protected CompletionLookupProvider buildLookupProvider() {
           return new NRTCompletionLookupProvider(preserveSeparators, false, preservePositionIncrements, payloads);
       }
   }
}
