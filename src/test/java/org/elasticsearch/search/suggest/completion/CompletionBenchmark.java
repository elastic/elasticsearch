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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene49.Lucene49Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.search.suggest.analyzing.XLookup;
import org.apache.lucene.search.suggest.analyzing.XNRTSuggester;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.postingsformat.Elasticsearch090PostingsFormat;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PreBuiltPostingsFormatProvider;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.context.ContextMapping;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;


public class CompletionBenchmark extends ElasticsearchTestCase {

    private final static int rounds = 15;
    private final static int warmup = 5;

    private int num = 7;

    private final static int maxDataSize = 5000;

    private final static Random random = new Random(0xdeadbeef);

    private final static boolean useTopWiki = false; // for some reason Top50KWiki gives way higher QPS!

    private static class Input {
        private String term;
        private int weight;

        Input(String term, int weight) {
            this.term = term;
            this.weight = weight;
        }
    }

    private static class StoredFieldInput {
        private String name;
        private BytesRef value;
        private Class type;

        private StoredFieldInput(String name, BytesRef value) {
            this.name = name;
            this.value = value;
            this.type = BytesRef.class;
        }
    }

    private static List<Input> benchmarkInput = new ArrayList<>();

    private static List<Input> lookupInput = new ArrayList<>();

    final Analyzer analyzer = new MockAnalyzer(random);
    final NamedAnalyzer namedAnalzyer = new NamedAnalyzer("foo", analyzer);
    final PostingsFormatProvider provider = new PreBuiltPostingsFormatProvider(new Elasticsearch090PostingsFormat());

    static List<String> SUGGEST_FIELD_NAMES = new ArrayList<>(2);
    static final String SUGGEST_FIELD_NAME = "foo";
    static final String SUGGEST_FIELD_WITH_PAYLOAD_NAME = "foo_payload";

    static {
        SUGGEST_FIELD_NAMES.add(SUGGEST_FIELD_NAME);
        SUGGEST_FIELD_NAMES.add(SUGGEST_FIELD_WITH_PAYLOAD_NAME);
    }

    @BeforeClass
    public static void setup() throws Exception {
        if (!useTopWiki) {
            // data set with longer terms to suggest
            LineFileDocs docs = new LineFileDocs(null);
            Set<String> seen = new HashSet<>();
            for (int i = 0; i < maxDataSize; i++) {
                Document nextDoc = docs.nextDoc();
                IndexableField field = nextDoc.getField("title");
                String term = field.stringValue();
                if (seen.contains(term)) {
                    continue;
                } else {
                    seen.add(term);
                }
                int weight;
                if ((weight = random.nextInt()) < 0) {
                    // force positive weight
                    weight *= -1;
                }
                Input input = new Input(term, weight);
                CompletionBenchmark.benchmarkInput.add(input);
                CompletionBenchmark.lookupInput.add(input);
            }
            Collections.shuffle(CompletionBenchmark.lookupInput, random);
            assert CompletionBenchmark.benchmarkInput.size() == CompletionBenchmark.lookupInput.size();
            docs.close();
        } else {
            List<Input> input = readTop50KWiki();
            Collections.shuffle(input, random);
            CompletionBenchmark.lookupInput = input;
            Collections.shuffle(input, random);
            CompletionBenchmark.benchmarkInput = input;
        }
    }

    static final Charset UTF_8 = StandardCharsets.UTF_8;

    /**
     * Collect the multilingual input for benchmarks/ tests.
     */
    public static List<Input> readTop50KWiki() throws Exception {
        List<Input> input = new ArrayList<>();

        BufferedReader br = new BufferedReader(new InputStreamReader(
                CompletionBenchmark.class.getResourceAsStream("/org/elasticsearch/search.suggest.completion/Top50KWiki.utf8"), UTF_8));

        String line = null;
        while ((line = br.readLine()) != null) {
            int tab = line.indexOf('|');
            assert tab >= 0 : "No | separator?: " + line;
            int weight = Integer.parseInt(line.substring(tab + 1));
            String key = line.substring(0, tab);
            input.add(new Input(key, weight));
        }
        br.close();
        return input;
    }

    /**
     * Test performance of lookup on full hits.
     */
    @Test
    public void testPerformanceOnFullHits() throws Exception {
        final int minPrefixLen = 100;
        final int maxPrefixLen = 200;
        runPerformanceTest(minPrefixLen, maxPrefixLen, num);
    }

    /**
     * Test performance of lookup on longer term prefixes (6-9 letters or shorter).
     */
    @Test
    public void testPerformanceOnPrefixes6_9() throws Exception {
        final int minPrefixLen = 6;
        final int maxPrefixLen = 9;
        runPerformanceTest(minPrefixLen, maxPrefixLen, num);
    }

    /**
     * Test performance of lookup on short term prefixes (2-4 letters or shorter).
     */
    @Test
    public void testPerformanceOnPrefixes2_4() throws Exception {
        final int minPrefixLen = 2;
        final int maxPrefixLen = 4;
        runPerformanceTest(minPrefixLen, maxPrefixLen, num);
    }

    /**
     * Test NRT lookup performance with varying percentage of deleted documents
     */
    @Test
    public void testNRTDeletedDocFiltering() throws Exception {
        System.out.println("-- NRT Lookup performance with deleted doc filtering");
        int[][] prefixLengths = {{100,200}, {6,9}, {2, 4}};
        for(int[] prefixLength : prefixLengths) {
            final int minPrefixLen = prefixLength[0];
            final int maxPrefixLen = prefixLength[1];
            List<String> inputs = generateInputs(minPrefixLen, maxPrefixLen);
            System.out.println(String.format(Locale.ROOT,
                    "  -- prefixes: %d-%d, num: %d",
                    minPrefixLen, maxPrefixLen, num));
            for (float delDocRatio = 0.0f; delDocRatio < 1.0f; delDocRatio += 0.2f) {
                System.out.print(String.format(Locale.ROOT, "   [%2.0f%% deleted docs] ", delDocRatio * 100));
                final CompletionProvider completionProvider = buildNRTLookup(delDocRatio);
                final Map.Entry<Lookup, AtomicReader> nrtLookupEntry = completionProvider.getNRTLookup();
                Lookup nrtLookup = nrtLookupEntry.getKey();
                AtomicReader reader = nrtLookupEntry.getValue();
                runNRTPerfTest(nrtLookup, reader, inputs);
                reader.close();
                completionProvider.close();
            }
        }
    }

    /**
     * Test RAM consumption
     */
    @Test
    public void testStorageNeeds() throws Exception {
        System.out.println("-- RAM consumption");
        final Lookup analyzingLookup = buildAnalyzingLookup(null, true);
        final Lookup xAnalyzingLookup = buildXAnalyzingLookup(null);
        final Lookup xNRTLookup = buildXNRTLookup(null).getKey();

        runStorageNeeds("AnalyzingSuggester", analyzingLookup);
        runStorageNeeds("XAnalyzingSuggester", xAnalyzingLookup);
        runStorageNeeds("XNRTSuggester", xNRTLookup);
    }


    /**
     * Test time to build
     */
    @Test
    public void testBuildPerformance() throws Exception {
        System.out.println("-- Build time");
        String[] names = {"AnalyzingSuggester", "XAnalyzingSuggester", "XNRTSuggester"};
        for (final String name : names) {
            BenchmarkResult result = measure(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    final Lookup lookup = buildLookup(name);
                    return lookup.hashCode();
                }
            });

            System.out.println(
                    String.format(Locale.ROOT, "  %-15s input: %d, time[ms]: %s",
                            name,
                            lookupInput.size(),
                            result.average.toString()));
        }
    }

    @Test
    public void testStoredFieldRetrievalPerformance() throws Exception {
        System.out.println("-- Stored Field Retrieval Performance");
        int[][] prefixLengthBounds = {{2, 4}, {3, 6}, {100, 200}};
        final String returnFieldName = "payload";
        Lookup analyzingLookup = null;
        Lookup xAnalyzingLookup = null;
        Map.Entry<Lookup, AtomicReader> xNRTLookupAndReader = null;
        Set<String> returnFieldNames = new HashSet<>(1);
        returnFieldNames.add(returnFieldName);
        final List<StoredFieldInput> storedFieldInputs = generateStoredFieldInputs(returnFieldName);
        for (int[] prefixLengthBound : prefixLengthBounds) {
            int minPrefixLength = prefixLengthBound[0];
            int maxPrefixLength = prefixLengthBound[1];
            final List<String> inputs = generateInputs(minPrefixLength, maxPrefixLength);
            analyzingLookup = buildAnalyzingLookup(storedFieldInputs, true);
            xAnalyzingLookup = buildXAnalyzingLookup(storedFieldInputs);
            xNRTLookupAndReader = buildXNRTLookup(storedFieldInputs);

            for (int num : new int[]{2, 4, 6}) {
                System.out.println(String.format(Locale.ROOT,
                        "  -- prefixes: %d-%d, num: %d", minPrefixLength, maxPrefixLength, num));

                runPerfTest("  AnalyzingSuggester", analyzingLookup, inputs, null, null, num);
                runPerfTest("  XAnalyzingSuggester", xAnalyzingLookup, inputs, null, null, num);
                runPerfTest("  XNRTSuggester", xNRTLookupAndReader.getKey(), inputs, xNRTLookupAndReader.getValue(), returnFieldNames, num);
            }
        }

        assert xNRTLookupAndReader != null;
        System.out.println("\n - Storage benchmark");
        runStorageNeeds("  AnalyzingSuggester (with payload)", analyzingLookup);
        runStorageNeeds("  XAnalyzingSuggester (with payload)", xAnalyzingLookup);
        runStorageNeeds("  XNRTSuggester", xNRTLookupAndReader.getKey());

    }

    private List<StoredFieldInput> generateStoredFieldInputs(String name) {
        List<StoredFieldInput> storedFieldInputs = new ArrayList<>(benchmarkInput.size());
        for (Input input : benchmarkInput) {
            storedFieldInputs.add(new StoredFieldInput(name, new BytesRef(input.term)));
        }
        return storedFieldInputs;
    }

    public void runPerformanceTest(final int minPrefixLen, final int maxPrefixLen,
                                   final int num) throws Exception {
        System.out.println(String.format(Locale.ROOT,
                "-- Lookup performance (prefixes: %d-%d, num: %d)",
                minPrefixLen, maxPrefixLen, num));

        final List<String> inputs = generateInputs(minPrefixLen, maxPrefixLen);
        final Lookup analyzingLookup = buildAnalyzingLookup(null, true);
        final Lookup xAnalyzingLookup = buildXAnalyzingLookup(null);
        final Lookup xNRTLookup = buildXNRTLookup(null).getKey();

        runPerfTest("AnalyzingSuggester", analyzingLookup, inputs, null, null, num);
        runPerfTest("XAnalyzingSuggester", xAnalyzingLookup, inputs, null, null, num);
        runPerfTest("XNRTSuggester", xNRTLookup, inputs, null, null, num);
    }

    public Lookup buildLookup(String name) throws Exception {
        switch (name) {
            case "AnalyzingSuggester":
                return buildAnalyzingLookup(null, false);
            case "XAnalyzingSuggester":
                return buildXAnalyzingLookup(null);
            default:
                return buildXNRTLookup(null).getKey();
        }
    }

    /**
     * Test memory required for the storage.
     */
    public void runStorageNeeds(String name, Lookup lookup) throws Exception {
        long sizeInBytes = lookup.ramBytesUsed();
        System.out.println(
                String.format(Locale.ROOT, "  %-15s size[B]:%,13d",
                        name,
                        sizeInBytes));
    }

    private Lookup buildAnalyzingLookup(List<StoredFieldInput> payloads, boolean validate) throws IOException {
        AnalyzingSuggester suggester = new AnalyzingSuggester(analyzer);
        suggester.build(constructInputIterator(lookupInput, payloads, validate));
        return suggester;
    }

    private InputIterator constructInputIterator(final List<Input> inputs, final List<StoredFieldInput> payloads, boolean validate) throws IOException {
        InputIterator inputIterator = new InputIterator() {
            int index = -1;

            @Override
            public long weight() {
                return inputs.get(index).weight;
            }

            @Override
            public BytesRef payload() {
                if (payloads != null) {
                    return payloads.get(index).value;
                }
                return null;
            }

            @Override
            public boolean hasPayloads() {
                return payloads != null;
            }

            @Override
            public Set<BytesRef> contexts() {
                return null;
            }

            @Override
            public boolean hasContexts() {
                return false;
            }

            @Override
            public BytesRef next() throws IOException {
                if (++index < inputs.size()) {
                    return new BytesRef(inputs.get(index).term);
                } else {
                    return null;
                }
            }

            @Override
            public Comparator<BytesRef> getComparator() {
                return null;
            }
        };


        if (validate) {
            BytesRef term;
            int count = 0;
            Iterator<Input> listIterator = inputs.iterator();
            while((term = inputIterator.next())!= null) {
                Input listItem = listIterator.next();
                assert term.utf8ToString().equals(listItem.term);
                assert ((int) inputIterator.weight()) == listItem.weight;
                count++;
            }
            assert count == inputs.size();
            // used up constructed input iterator; construct it again without validation
            return constructInputIterator(inputs, payloads, false);
        }

        return inputIterator;
    }

    private Lookup buildXAnalyzingLookup(final List<StoredFieldInput> storedFieldInputs) throws Exception {
        String fieldName = (storedFieldInputs==null) ? SUGGEST_FIELD_NAME : SUGGEST_FIELD_WITH_PAYLOAD_NAME;
        final CompletionFieldMapper analyzingCompletionFieldMapper = new CompletionFieldMapper(new FieldMapper.Names(fieldName), namedAnalzyer, namedAnalzyer, provider, null, storedFieldInputs!=null,
                false, false, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING);
        CompletionProvider completionProvider = null;
        try {
            completionProvider = new CompletionProvider(analyzingCompletionFieldMapper);
            completionProvider.indexCompletions(lookupInput, storedFieldInputs);
            return completionProvider.getLookup();
        } finally {
            assert completionProvider != null;
            completionProvider.close();
        }
    }

    private Map.Entry<Lookup, AtomicReader> buildXNRTLookup(final List<StoredFieldInput> storedFieldInputs) throws Exception {
        String fieldName = (storedFieldInputs==null) ? SUGGEST_FIELD_NAME : SUGGEST_FIELD_WITH_PAYLOAD_NAME;
        final CompletionFieldMapper nrtCompletionFieldMapper = new NRTCompletionPostingsFormatTest.NRTCompletionFieldMapper(new FieldMapper.Names(fieldName), namedAnalzyer, namedAnalzyer, provider, null, storedFieldInputs!=null,
                false, false, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING);
        CompletionProvider completionProvider = null;
        try {
            completionProvider = new CompletionProvider(nrtCompletionFieldMapper);
            completionProvider.indexCompletions(lookupInput, storedFieldInputs);
            return completionProvider.getNRTLookup();
        } finally {
            assert completionProvider != null;
            completionProvider.close();
        }
    }

    private CompletionProvider buildNRTLookup(float deletedDocRatio) throws Exception {
        final CompletionFieldMapper nrtCompletionFieldMapper = new NRTCompletionPostingsFormatTest.NRTCompletionFieldMapper(new FieldMapper.Names(SUGGEST_FIELD_NAME), namedAnalzyer, namedAnalzyer, provider, null, false,
                false, false, Integer.MAX_VALUE, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING);
        int deletedDocs = (int) (deletedDocRatio * lookupInput.size());
        final CompletionProvider completionProvider = new CompletionProvider(nrtCompletionFieldMapper);
        completionProvider.indexCompletions(lookupInput);
        IndexReader reader = completionProvider.getReader();
        completionProvider.deleteDocs(reader, deletedDocs);
        reader.close();
        return completionProvider;
    }

    private List<String> generateInputs(int minPrefixLen, int maxPrefixLen) {
        final List<String> inputs = new ArrayList<>();
        for (Input input : benchmarkInput) {
            String s = input.term;
            String sub = s.substring(0, Math.min(s.length(),
                    minPrefixLen + random.nextInt(maxPrefixLen - minPrefixLen + 1)));
            inputs.add(sub);
        }
        return inputs;
    }

    private void runPerfTest(final String name, final Lookup lookup, final List<String> inputs, final AtomicReader reader, final Set<String> returnStoredFields, final int num) {
        BenchmarkResult result;
        if (lookup instanceof XLookup) {
            final XLookup xLookup = (XLookup) lookup;
            if (returnStoredFields != null) {
                result = measure(new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        int v = 0;
                        for (String term : inputs) {
                            v += xLookup.lookup(term, num, reader, returnStoredFields).size();
                        }
                        return v;
                    }
                });
            } else {
                result = measure(new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        int v = 0;
                        for (String term : inputs) {
                            v += xLookup.lookup(term, num).size();
                        }
                        return v;
                    }
                });
            }
        } else {
            assert returnStoredFields == null : "returnStoredFields has to be null for non-nrt suggesters";
            result = measure(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    int v = 0;
                    for (String term : inputs) {
                        v += lookup.lookup(term, false, num).size();
                    }
                    return v;
                }
            });
        }
        System.out.println(
                String.format(Locale.ROOT, "  %-15s queries: %d, time[ms]: %s, ~kQPS: %.0f",
                        name,
                        inputs.size(),
                        result.average.toString(),
                        inputs.size() / result.average.avg));
    }

    private void runNRTPerfTest(final Lookup lookup, final AtomicReader reader, final List<String> inputs) {
        assert lookup instanceof XNRTSuggester;
        final XNRTSuggester suggester = (XNRTSuggester) lookup;
        BenchmarkResult result = measure(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                int v = 0;
                for (String term : inputs) {
                    v += suggester.lookup(term, num, reader).size();
                }
                return v;
            }
        });

        System.out.println(
                String.format(Locale.ROOT, "%-15s queries: %d, time[ms]: %s, ~kQPS: %.0f",
                        "XNRTSuggester",
                        inputs.size(),
                        result.average.toString(),
                        inputs.size() / result.average.avg));
    }

    /**
     * Do the measurements.
     */
    private BenchmarkResult measure(Callable<Integer> callable) {
        final double NANOS_PER_MS = 1000000;

        try {
            List<Double> times = new ArrayList<>();
            for (int i = 0; i < warmup + rounds; i++) {
                final long start = System.nanoTime();
                guard = callable.call().intValue();
                times.add((System.nanoTime() - start) / NANOS_PER_MS);
            }
            return new BenchmarkResult(times, warmup, rounds);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);

        }
    }

    /** Guard against opts. */
    @SuppressWarnings("unused")
    private static volatile int guard;

    private static class BenchmarkResult {
        /** Average time per round (ms). */
        public final Average average;

        public BenchmarkResult(List<Double> times, int warmup, int rounds) {
            this.average = Average.from(times.subList(warmup, times.size()));
        }
    }


    private class CompletionProvider {
        final IndexWriterConfig indexWriterConfig;
        final CompletionFieldMapper mapper;
        IndexWriter writer;
        RAMDirectory dir = new RAMDirectory();

        public CompletionProvider(final CompletionFieldMapper mapper) throws IOException {
            Codec filterCodec = new Lucene49Codec() {
                @Override
                public PostingsFormat getPostingsFormatForField(String field) {
                    if (SUGGEST_FIELD_NAMES.contains(field)) {
                        return mapper.postingsFormatProvider().get();
                    }
                    return super.getPostingsFormatForField(field);
                }
            };
            this.indexWriterConfig = new IndexWriterConfig(Version.LUCENE_4_9, mapper.indexAnalyzer());
            indexWriterConfig.setCodec(filterCodec);
            this.mapper = mapper;
        }

        public void indexCompletions(List<Input> inputs) throws Exception {
            indexCompletions(inputs, null);
        }

        public void indexCompletions(List<Input> inputs, List<StoredFieldInput> storedFieldInputs) throws Exception {
            writer = new IndexWriter(dir, indexWriterConfig);
            final boolean hasStoredFieldInputs = storedFieldInputs != null;
            if (hasStoredFieldInputs) {
                assert storedFieldInputs.size() == inputs.size();
            }
            for (int i = 0; i < inputs.size(); i++) {
                Input input = inputs.get(i);
                Document doc = new Document();
                BytesRef payload = mapper.buildPayload(new BytesRef(input.term), input.weight, new BytesRef());
                doc.add(mapper.getCompletionField(ContextMapping.EMPTY_CONTEXT, input.term, payload));
                if (hasStoredFieldInputs) {
                    StoredFieldInput storedFieldInput = storedFieldInputs.get(i);
                    doc.add(makeField(storedFieldInput.name, storedFieldInput.value, storedFieldInput.type));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1, true);
            writer.commit();
        }

        private Set<Integer> generateDocIDsToDelete(int numDocs, int numDocsToDelete) {
            Set<Integer> docIDsToDelete = new HashSet<>(numDocsToDelete);
            for (int i = 0; i < numDocsToDelete; i++) {
                while (true) {
                    int docID = random.nextInt() % numDocs;
                    if (docID < 0) {
                        docID += numDocs;
                    }
                    if (!docIDsToDelete.contains(docID)) {
                        assert docID >= 0 && docID < numDocs :
                                String.format(Locale.ROOT, "docID=%d, numDocs=%d", docID, numDocs);
                        docIDsToDelete.add(docID);
                        break;
                    }
                }
            }
            return docIDsToDelete;
        }

        public IndexReader getReader() throws IOException {
            assert writer != null;
            return DirectoryReader.open(writer, true);
        }

        public void deleteDocs(IndexReader reader, int numDocsToDelete) throws Exception {
            assert reader.leaves().size() == 1;
            Set<Integer> docIDsToDelete = generateDocIDsToDelete(reader.numDocs(), numDocsToDelete);

            for (Integer docID : docIDsToDelete) {
                int initialNumDocs = writer.numDocs();
                assert writer.tryDeleteDocument(reader, docID);
                writer.commit();
                assert initialNumDocs == writer.numDocs() + 1;
            }
        }

        public Map.Entry<Lookup, AtomicReader> getNRTLookup() throws Exception {
            IndexReader reader = getReader();
            //readers.add(reader); // clean up on close
            assert reader.leaves().size() == 1;
            AtomicReader atomicReader = reader.leaves().get(0).reader();
            Terms luceneTerms = atomicReader.terms(mapper.name());
            Lookup lookup = null;
            if (luceneTerms instanceof Completion090PostingsFormat.CompletionTerms) {
                lookup = ((Completion090PostingsFormat.CompletionTerms) luceneTerms).getLookup(mapper, new CompletionSuggestionContext(null));
            }
            assert lookup != null;
            return new AbstractMap.SimpleEntry<>(lookup, atomicReader);
        }

        public Lookup getLookup() throws IOException {
            IndexReader reader = DirectoryReader.open(writer, true);
            assert reader.leaves().size() == 1;
            AtomicReader atomicReader = reader.leaves().get(0).reader();
            Terms luceneTerms = atomicReader.terms(mapper.name());
            Lookup lookup = null;
            if (luceneTerms instanceof Completion090PostingsFormat.CompletionTerms) {
                lookup = ((Completion090PostingsFormat.CompletionTerms) luceneTerms).getLookup(mapper, new CompletionSuggestionContext(null));
            }
            assert lookup != null;
            return lookup;
        }

        public void close() throws IOException {
            writer.close();
            dir.close();
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
            throw new Exception("Unsupported Type " + type);
        }
    }
    // copied from Lucene (org/apache/lucene/search/suggest/Average.java)
    final static class Average {
        /**
         * Average (in milliseconds).
         */
        public final double avg;

        /**
         * Standard deviation (in milliseconds).
         */
        public final double stddev;

        /**
         *
         */
        Average(double avg, double stddev)
        {
            this.avg = avg;
            this.stddev = stddev;
        }

        @Override
        public String toString()
        {
            return String.format(Locale.ROOT, "%.0f [+- %2.2f]",
                    avg, stddev);
        }

        static Average from(List<Double> values) {
            double sum = 0;
            double sumSquares = 0;

            for (double l : values)
            {
                sum += l;
                sumSquares += l * l;
            }

            double avg = sum / (double) values.size();
            return new Average(
                    (sum / (double) values.size()),
                    Math.sqrt(sumSquares / (double) values.size() - avg * avg));
        }
    }
}