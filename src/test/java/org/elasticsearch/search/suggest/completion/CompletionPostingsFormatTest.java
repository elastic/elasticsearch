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
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.context.ContextMapping;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.*;

public class CompletionPostingsFormatTest extends ElasticsearchTestCase {

    @Test
    public void testCompletionPostingsFormat() throws IOException {
        AnalyzingCompletionLookupProviderV1 providerV1 = new AnalyzingCompletionLookupProviderV1(true, false, true, true);
        AnalyzingCompletionLookupProvider currentProvider = new AnalyzingCompletionLookupProvider(true, false, true, true);
        List<CompletionLookupProvider> providers = Lists.newArrayList(providerV1, currentProvider);

        CompletionLookupProvider randomProvider = providers.get(getRandom().nextInt(providers.size()));
        RAMDirectory dir = new RAMDirectory();
        writeData(dir, randomProvider);

        IndexInput input = dir.openInput("foo.txt", IOContext.DEFAULT);
        LookupFactory load = currentProvider.load(input);
        PostingsFormatProvider format = new PreBuiltPostingsFormatProvider(new Elasticsearch090PostingsFormat());
        NamedAnalyzer analyzer = new NamedAnalyzer("foo", new StandardAnalyzer(TEST_VERSION_CURRENT));
        Lookup lookup = load.getLookup(new CompletionFieldMapper(new Names("foo"), analyzer, analyzer, format, null, true, true, true, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING), new CompletionSuggestionContext(null));
        List<LookupResult> result = lookup.lookup("ge", false, 10);
        assertThat(result.get(0).key.toString(), equalTo("Generator - Foo Fighters"));
        assertThat(result.get(0).payload.utf8ToString(), equalTo("id:10"));
        dir.close();
    }

    @Test
    public void testProviderBackwardCompatibilityForVersion1() throws IOException {
        AnalyzingCompletionLookupProviderV1 providerV1 = new AnalyzingCompletionLookupProviderV1(true, false, true, true);
        AnalyzingCompletionLookupProvider currentProvider = new AnalyzingCompletionLookupProvider(true, false, true, true);

        RAMDirectory dir = new RAMDirectory();
        writeData(dir, providerV1);

        IndexInput input = dir.openInput("foo.txt", IOContext.DEFAULT);
        LookupFactory load = currentProvider.load(input);
        PostingsFormatProvider format = new PreBuiltPostingsFormatProvider(new Elasticsearch090PostingsFormat());
        NamedAnalyzer analyzer = new NamedAnalyzer("foo", new StandardAnalyzer(TEST_VERSION_CURRENT));
        AnalyzingCompletionLookupProvider.AnalyzingSuggestHolder analyzingSuggestHolder = load.getAnalyzingSuggestHolder(new CompletionFieldMapper(new Names("foo"), analyzer, analyzer, format, null, true, true, true, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING));
        assertThat(analyzingSuggestHolder.sepLabel, is(AnalyzingCompletionLookupProviderV1.SEP_LABEL));
        assertThat(analyzingSuggestHolder.payloadSep, is(AnalyzingCompletionLookupProviderV1.PAYLOAD_SEP));
        assertThat(analyzingSuggestHolder.endByte, is(AnalyzingCompletionLookupProviderV1.END_BYTE));
        dir.close();
    }

    @Test
    public void testProviderVersion2() throws IOException {
        AnalyzingCompletionLookupProvider currentProvider = new AnalyzingCompletionLookupProvider(true, false, true, true);

        RAMDirectory dir = new RAMDirectory();
        writeData(dir, currentProvider);

        IndexInput input = dir.openInput("foo.txt", IOContext.DEFAULT);
        LookupFactory load = currentProvider.load(input);
        PostingsFormatProvider format = new PreBuiltPostingsFormatProvider(new Elasticsearch090PostingsFormat());
        NamedAnalyzer analyzer = new NamedAnalyzer("foo", new StandardAnalyzer(TEST_VERSION_CURRENT));
        AnalyzingCompletionLookupProvider.AnalyzingSuggestHolder analyzingSuggestHolder = load.getAnalyzingSuggestHolder(new CompletionFieldMapper(new Names("foo"), analyzer, analyzer, format, null, true, true, true, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING));
        assertThat(analyzingSuggestHolder.sepLabel, is(XAnalyzingSuggester.SEP_LABEL));
        assertThat(analyzingSuggestHolder.payloadSep, is(XAnalyzingSuggester.PAYLOAD_SEP));
        assertThat(analyzingSuggestHolder.endByte, is(XAnalyzingSuggester.END_BYTE));
        dir.close();
    }

    private String generateRandomSuggestions(int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            char c = randomUnicodeOfLengthBetween(1,1).charAt(0);
            while(CompletionFieldMapper.isReservedChar(c)) {
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
        final CompletionFieldMapper mapper = new CompletionFieldMapper(new Names("foo"), namedAnalzyer, namedAnalzyer, provider, null, usePayloads,
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
        assertTrue(lookup instanceof XAnalyzingSuggester);
        XAnalyzingSuggester suggester = (XAnalyzingSuggester) lookup;

        List<LookupResult> lookupResults = suggester.lookup(prefix, null, false, res, null);

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
        Tuple<Lookup, Bits> lookupAndLiveBitsTuple = completionProvider.getLookup(reader);
        lookup = lookupAndLiveBitsTuple.v1();
        Bits liveDocs = lookupAndLiveBitsTuple.v2();

        assertTrue(lookup instanceof XAnalyzingSuggester);
        XAnalyzingSuggester suggesterWithDeletes = (XAnalyzingSuggester) lookup;

        lookupResults = suggesterWithDeletes.lookup(prefix, null, false, res, liveDocs);

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
        final CompletionFieldMapper mapper = new CompletionFieldMapper(new Names("foo"), namedAnalzyer, namedAnalzyer, provider, null, usePayloads,
                preserveSeparators, preservePositionIncrements, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING);

        String prefixStr = generateRandomSuggestions(randomIntBetween(2, 6));
        int num = scaledRandomIntBetween(1, 50);
        final String[] titles = new String[num];
        final long[] weights = new long[num];
        final String suffix = generateRandomSuggestions(randomIntBetween(4, scaledRandomIntBetween(30, 100)));
        for (int i = 0; i < titles.length; i++) {
            titles[i] = prefixStr + suffix;
            weights[i] = between(0, 100);
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
        deletedTerms.put(prefixStr + suffix, num);

        // suggest for the same prefix n times, deleting one copy each time
        for (int i = 0; i < num; i++) {
            int res = between(1, num);
            IndexReader reader = completionProvider.getReader();
            final Tuple<Lookup, Bits> lookupAndLiveBits = completionProvider.getLookup(reader);
            Lookup lookup = lookupAndLiveBits.v1();
            Bits liveDocs = lookupAndLiveBits.v2();
            assertTrue(lookup instanceof XAnalyzingSuggester);
            XAnalyzingSuggester suggester = (XAnalyzingSuggester) lookup;
            List<LookupResult> lookupResults = suggester.lookup(prefix, null, false, res, liveDocs);
            reader.close();
            assertThat(lookupResults.size(), equalTo(1));
            for (LookupResult result : lookupResults) {
                String key = result.key.toString();
                IndexReader indexReader = completionProvider.getReader();
                completionProvider.deleteDoc(indexReader, key);
                indexReader.close();
                Integer counter = deletedTerms.get(key);
                assertThat(counter, notNullValue());
                assertThat(counter, greaterThanOrEqualTo(1));
                deletedTerms.put(key, --counter);
            }
        }
        assertThat(deletedTerms.get(prefixStr+suffix), equalTo(0));
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
        final CompletionFieldMapper mapper = new CompletionFieldMapper(new Names("foo"), namedAnalzyer, namedAnalzyer, provider, null, usePayloads,
                preserveSeparators, preservePositionIncrements, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING);
        Lookup buildAnalyzingLookup = buildAnalyzingLookup(mapper, titles, titles, weights);
        // NOTE: as the suggestion entries are now tied to the lucene docs, all of the surface forms
        // are stored; hence the TopNSearcher queue has to be made deeper
        //Field field = buildAnalyzingLookup.getClass().getDeclaredField("maxAnalyzedPathsForOneInput");
        //field.setAccessible(true);
        //Field refField = reference.getClass().getDeclaredField("maxAnalyzedPathsForOneInput");
        //refField.setAccessible(true);
        //assertThat(refField.get(reference), equalTo(field.get(buildAnalyzingLookup)));

        for (int i = 0; i < titles.length; i++) {
            int res = between(1, 10);
            final StringBuilder builder = new StringBuilder();
            SuggestUtils.analyze(namedAnalzyer.tokenStream("foo", titles[i]), new SuggestUtils.TokenConsumer() {
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
            List<LookupResult> lookup = buildAnalyzingLookup.lookup(prefix, false, res);
            assertThat(refLookup.toString(),lookup.size(), equalTo(refLookup.size()));
            for (int j = 0; j < refLookup.size(); j++) {
                assertThat(lookup.get(j).key, equalTo(refLookup.get(j).key));
                assertThat("prefix: " + prefix + " " + j + " -- missmatch cost: " + lookup.get(j).key + " - " + lookup.get(j).value + " | " + refLookup.get(j).key + " - " + refLookup.get(j).value ,
                        lookup.get(j).value, equalTo(refLookup.get(j).value));
                assertThat(lookup.get(j).payload, equalTo(refLookup.get(j).payload));
                if (usePayloads) {
                    assertThat(lookup.get(j).payload.utf8ToString(),  equalTo(Long.toString(lookup.get(j).value)));
                }
            }
        }
    }

    private IndexWriter setupCompletionIndexWriter(final CompletionFieldMapper mapper, RAMDirectory dir) throws IOException {
        FilterCodec filterCodec = new FilterCodec("filtered", Codec.getDefault()) {
            public PostingsFormat postingsFormat() {
                return mapper.postingsFormatProvider().get();
            }
        };
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(TEST_VERSION_CURRENT, mapper.indexAnalyzer());
        indexWriterConfig.setCodec(filterCodec);
        return new IndexWriter(dir, indexWriterConfig);

    }

    public Lookup buildAnalyzingLookup(final CompletionFieldMapper mapper, String[] terms, String[] surfaces, long[] weights)
            throws IOException {
        RAMDirectory dir = new RAMDirectory();
        IndexWriter writer = setupCompletionIndexWriter(mapper, dir);
        for (int i = 0; i < weights.length; i++) {
            Document doc = new Document();
            BytesRef payload = mapper.buildPayload(new BytesRef(surfaces[i]), weights[i], new BytesRef(Long.toString(weights[i])));
            doc.add(mapper.getCompletionField(ContextMapping.EMPTY_CONTEXT, terms[i], payload));
            if (randomBoolean()) {
                writer.commit();
            }
            writer.addDocument(doc);
        }
        writer.commit();
        writer.forceMerge(1, true);
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer, true);
        assertThat(reader.leaves().size(), equalTo(1));
        assertThat(reader.leaves().get(0).reader().numDocs(), equalTo(weights.length));
        AtomicReaderContext atomicReaderContext = reader.leaves().get(0);
        Terms luceneTerms = atomicReaderContext.reader().terms(mapper.name());
        Lookup lookup = ((Completion090PostingsFormat.CompletionTerms) luceneTerms).getLookup(mapper, new CompletionSuggestionContext(null));
        reader.close();
        writer.close();
        dir.close();
        return lookup;
    }
    
    @Test
    public void testNoDocs() throws IOException {
        AnalyzingCompletionLookupProvider provider = new AnalyzingCompletionLookupProvider(true, false, true, true);
        RAMDirectory dir = new RAMDirectory();
        IndexOutput output = dir.createOutput("foo.txt", IOContext.DEFAULT);
        FieldsConsumer consumer = provider.consumer(output);
        FieldInfo fieldInfo = new FieldInfo("foo", true, 1, false, true, true, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
                DocValuesType.SORTED, DocValuesType.BINARY, -1, new HashMap<String, String>());
        TermsConsumer addField = consumer.addField(fieldInfo);
        addField.finish(0, 0, 0);
        consumer.close();
        output.close();

        IndexInput input = dir.openInput("foo.txt", IOContext.DEFAULT);
        LookupFactory load = provider.load(input);
        PostingsFormatProvider format = new PreBuiltPostingsFormatProvider(new Elasticsearch090PostingsFormat());
        NamedAnalyzer analyzer = new NamedAnalyzer("foo", new StandardAnalyzer(TEST_VERSION_CURRENT));
        assertNull(load.getLookup(new CompletionFieldMapper(new Names("foo"), analyzer, analyzer, format, null, true, true, true, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING), new CompletionSuggestionContext(null)));
        dir.close();
    }

    // TODO ADD more unittests
    private void writeData(Directory dir, CompletionLookupProvider provider) throws IOException {
        IndexOutput output = dir.createOutput("foo.txt", IOContext.DEFAULT);
        FieldsConsumer consumer = provider.consumer(output);
        FieldInfo fieldInfo = new FieldInfo("foo", true, 1, false, true, true, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
                DocValuesType.SORTED, DocValuesType.BINARY, -1, new HashMap<String, String>());
        TermsConsumer addField = consumer.addField(fieldInfo);

        PostingsConsumer postingsConsumer = addField.startTerm(new BytesRef("foofightersgenerator"));
        postingsConsumer.startDoc(0, 1);
        postingsConsumer.addPosition(256 - 2, provider.buildPayload(new BytesRef("Generator - Foo Fighters"), 9, new BytesRef("id:10")), 0,
                1);
        postingsConsumer.finishDoc();
        addField.finishTerm(new BytesRef("foofightersgenerator"), new TermStats(1, 1));
        addField.startTerm(new BytesRef("generator"));
        postingsConsumer.startDoc(0, 1);
        postingsConsumer.addPosition(256 - 1, provider.buildPayload(new BytesRef("Generator - Foo Fighters"), 9, new BytesRef("id:10")), 0,
                1);
        postingsConsumer.finishDoc();
        addField.finishTerm(new BytesRef("generator"), new TermStats(1, 1));
        addField.finish(1, 1, 1);
        consumer.close();
        output.close();

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

        public Tuple<Lookup, Bits> getLookup(IndexReader reader) throws IOException {
            assertThat(reader.leaves().size(), equalTo(1));
            AtomicReader atomicReader = reader.leaves().get(0).reader();
            Terms luceneTerms = atomicReader.terms(mapper.name());
            Lookup lookup = null;
            if (luceneTerms instanceof Completion090PostingsFormat.CompletionTerms) {
                lookup = ((Completion090PostingsFormat.CompletionTerms) luceneTerms).getLookup(mapper, new CompletionSuggestionContext(null));
            }
            assertFalse(lookup == null);
            return new Tuple<>(lookup, atomicReader.getLiveDocs());
        }

        public void close() throws IOException {
            writer.close();
            dir.close();
        }
    }
}
