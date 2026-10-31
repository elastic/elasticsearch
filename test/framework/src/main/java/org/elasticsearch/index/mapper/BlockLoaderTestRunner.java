/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.codec.ElasticsearchStoredFieldsFormat;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.codec.bwc.ES93TSDBDefaultCompressionLucene103Codec;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.lucene.tests.util.LuceneTestCase.newDirectory;
import static org.apache.lucene.tests.util.LuceneTestCase.newLogMergePolicy;
import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test helper for {@link BlockLoader}. Run it like:
 * <pre>{@code
 *  BlockLoaderTestRunner runner = new BlockLoaderTestRunner(params);
 *  runner.mapperService(createMapperService...);
 *  runner.fieldName("field").document(Map.of("field", 1));
 *  runner.run(1);
 * }</pre>
 */
public class BlockLoaderTestRunner {
    public interface ResultMatcher {
        void match(Object expected, Object actual);
    }

    private final BlockLoaderTestCase.Params params;
    private boolean allowDummyDocs;
    private MapperService mapperService;
    private CircuitBreaker breaker;
    private String fieldName;
    private ParsedDocument doc;
    private Map<String, Object> mapDoc;
    private ResultMatcher matcher = (expected, actual) -> assertThat(actual, PrettyEqual.prettyEqualTo(expected));

    public BlockLoaderTestRunner(BlockLoaderTestCase.Params params) {
        this.params = params;
    }

    /**
     * Allow dummy documents in test index. This defaults to {@code false} but many callers
     * are fine with these and call this.
     */
    public BlockLoaderTestRunner allowDummyDocs() {
        this.allowDummyDocs = true;
        return this;
    }

    /**
     * Set the {@link MapperService} to use for the test. This must be provided before
     * calling {@link #run}.
     */
    public BlockLoaderTestRunner mapperService(MapperService mapperService) {
        this.mapperService = mapperService;
        return this;
    }

    /**
     * Set the {@link CircuitBreaker} used for the test. This must be provided before
     * calling {@link #run}.
     */
    public BlockLoaderTestRunner breaker(CircuitBreaker breaker) {
        this.breaker = breaker;
        return this;
    }

    /**
     * The name of the field to load. The test sends this to {@link MapperService#fieldType}.
     */
    public String fieldName() {
        return fieldName;
    }

    /**
     * Set the name of the field to load. The test sends this to {@link MapperService#fieldType}.
     * This is required before calling {@link #run}
     */
    public BlockLoaderTestRunner fieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    /**
     * Configuration a non-standard matcher. The default matcher is usually fine, and
     * you often don't have to call this.
     */
    public BlockLoaderTestRunner matcher(ResultMatcher matcher) {
        this.matcher = matcher;
        return this;
    }

    private ParsedDocument emptyDocument() {
        if (mapperService == null) {
            throw new IllegalStateException("need to set mapperService");
        }
        final Map<String, Object> emptyPayloadDoc = Map.of(
            "@timestamp",
            ESTestCase.randomMillisUpToYear9999(),
            "sensor_id",
            "empty_sensor_id"
        );
        return parseDocument(emptyPayloadDoc);
    }

    /**
     * The document being parsed.
     */
    public ParsedDocument document() {
        if (doc != null) {
            return doc;
        }
        if (mapDoc != null) {
            if (mapperService == null) {
                throw new IllegalStateException("need to set mapperService");
            }
            doc = parseDocument(mapDoc);
            return doc;
        }
        throw new IllegalStateException("need to set doc");
    }

    private ParsedDocument parseDocument(Map<String, Object> docToParse) {
        try {
            var documentXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(docToParse);
            var source = new SourceToParse(
                // "1",
                // In Time series the _id is derived from document
                params.indexMode() == IndexMode.TIME_SERIES ? null : "1",
                new BytesSource(BytesReference.bytes(documentXContent), XContentType.JSON, true),
                // null,
                // params.indexMode() == IndexMode.TIME_SERIES ? "temp_sensor_1" : null,
                params.indexMode() == IndexMode.TIME_SERIES ? (String) docToParse.get("sensor_id") : null,
                // params.indexMode() == IndexMode.TIME_SERIES ? TimeSeriesRoutingHashFieldMapper.DUMMY_ENCODED_VALUE : null,
                Map.of(),
                Map.of(),
                XContentMeteringParserDecorator.NOOP,
                null
            );
            return mapperService.documentMapper().parse(source);
        } catch (IOException e) {
            throw new RuntimeException("shouldn't be possible", e);
        }
    }

    /**
     * The document to be parsed as a {@link Map}. This will only work if the document
     * was set with {@link #document(Map)}.
     */
    public Map<String, Object> mapDoc() {
        if (mapDoc == null) {
            throw new IllegalStateException("need to set doc to a map");
        }
        return mapDoc;
    }

    /**
     * Set the document to be parsed. A method with this name must be called before
     * calling {@link #run}.
     */
    public void document(Map<String, Object> doc) throws IOException {
        this.mapDoc = decorateWithRequiredFields(new HashMap<>(doc), params.indexMode());
    }

    private static Map<String, Object> decorateWithRequiredFields(Map<String, Object> document, IndexMode indexMode) {
        if (indexMode == IndexMode.LOGSDB || indexMode == IndexMode.LOGSDB_COLUMNAR) {
            document.putIfAbsent("@timestamp", ESTestCase.randomMillisUpToYear9999());
        }
        if (indexMode == IndexMode.TIME_SERIES) {
            document.putIfAbsent("@timestamp", ESTestCase.randomMillisUpToYear9999());
            document.putIfAbsent("sensor_id", ESTestCase.randomAlphanumericOfLength(10));
            // document.putIfAbsent("my_temporality_field", "cumulative");
        }
        return document;
    }

    /**
     * Set the document to be parsed. A method with this name must be called before
     * calling {@link #run}.
     */
    public void document(ParsedDocument doc) throws IOException {
        this.doc = doc;
    }

    /**
     * Run the test and compare to {@code expected}.
     */
    public void run(Object expected) throws IOException {
        matcher.match(expected, setupAndInvokeBlockLoader());
    }

    /**
     * With synthetic {@code _id} the codec wraps the stored fields reader in a {@link TSDBStoredFieldsFormat.TSDBStoredFieldsReader}
     * that materializes the {@code _id} from doc values, which is asserted against on merge threads. In production
     * {@code PruningMergePolicy} unwraps it during merges (so Lucene can also bulk-merge); this does the same for the test writer.
     */
    private static MergePolicy unwrapSyntheticIdOnMerge(MergePolicy in) {
        return new OneMergeWrappingMergePolicy(in, toWrap -> new MergePolicy.OneMerge(toWrap.segments) {
            @Override
            public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                return new FilterCodecReader(toWrap.wrapForMerge(reader)) {
                    @Override
                    public StoredFieldsReader getFieldsReader() {
                        StoredFieldsReader fieldsReader = super.getFieldsReader();
                        return fieldsReader instanceof TSDBStoredFieldsFormat.TSDBStoredFieldsReader tsdbReader
                            ? tsdbReader.getStoredFieldsReader()
                            : fieldsReader;
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return null;
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return null;
                    }
                };
            }
        });
    }

    private Object setupAndInvokeBlockLoader() throws IOException {
        if (mapperService == null) {
            throw new IllegalStateException("need to set mapperService");
        }
        if (breaker == null) {
            throw new IllegalStateException("need to set breaker");
        }
        if (fieldName == null) {
            throw new IllegalStateException("need to set fieldName");
        }
        try (Directory directory = newDirectory()) {
            // disable index checks on directory close, this is needed
            // for time_series that requires pseudo-empty docs, docs that
            // contains only the @timestamp and the dimensions and doesn't
            // have any terms.
            if (mapperService.getIndexSettings().useTimeSeriesSyntheticId()) {
                if (directory instanceof BaseDirectoryWrapper directoryWrapper) {
                    directoryWrapper.setCheckIndexOnClose(false);
                }
            }

            final RandomIndexWriter iw;
            Random rg = random();
            if (params.indexMode() == IndexMode.TIME_SERIES) {
                IndexWriterConfig indexWriterConfig = setupIndexConfigForTimeSeries(rg);
                iw = new RandomIndexWriter(rg, directory, indexWriterConfig);
            } else {
                iw = new RandomIndexWriter(rg, directory);
            }

            LuceneDocument doc = this.document().rootDoc();

            /*
             * Add three documents with doc id 0, 1, 2. The real document is 1.
             * The other two are empty documents.
             */
            if (params.indexMode() == IndexMode.TIME_SERIES) {
                iw.addDocuments(List.of(emptyDocument().rootDoc(), doc, emptyDocument().rootDoc()));
            } else {
                iw.addDocuments(List.of(List.of(), doc, List.of()));
            }
            iw.close();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext context = reader.leaves().getFirst();
                return load(createBlockLoader(fieldName), context);
            }
        }
    }

    private @NonNull IndexWriterConfig setupIndexConfigForTimeSeries(Random rg) {
        IndexWriterConfig indexWriterConfig = LuceneTestCase.newIndexWriterConfig(rg, new MockAnalyzer(rg));
        Codec codec = new ES93TSDBDefaultCompressionLucene103Codec(
            new PerFieldMapperCodec(
                Lucene104Codec.Mode.BEST_SPEED,
                ElasticsearchStoredFieldsFormat.Mode.LUCENE,
                ElasticsearchStoredFieldsFormat.Mode.LUCENE,
                mapperService,
                BigArrays.NON_RECYCLING_INSTANCE,
                null
            )
        );

        // Codec codec = TestUtil.alwaysDocValuesFormat(new ES87TSDBDocValuesFormat());
        indexWriterConfig.setCodec(codec);
        indexWriterConfig.setMergePolicy(unwrapSyntheticIdOnMerge(newLogMergePolicy()));
        // The random test config may install a SimpleMergedSegmentWarmer, which reads every stored field of the merged
        // segment on the merge thread, materializing the synthetic _id. Production doesn't set a warmer.
        indexWriterConfig.setMergedSegmentWarmer(null);
        return indexWriterConfig;
    }

    private Object load(BlockLoader blockLoader, LeafReaderContext context) throws IOException {
        // `columnAtATimeReader` is tried first, we mimic `ValuesSourceReaderOperator`
        var columnAtATimeReaderSource = blockLoader.columnAtATimeReader(context);
        if (columnAtATimeReaderSource != null) {
            int[] docArray;
            int offset;
            if (randomBoolean()) {
                // Half the time we load a single document. Nice and simple.
                docArray = new int[] { 1 };
                offset = 0;
            } else {
                /*
                 * The other half the time we emulate loading a larger page,
                 * starting part way through the page.
                 */
                docArray = new int[between(2, 10)];
                offset = between(0, docArray.length - 1);
                for (int i = 0; i < docArray.length; i++) {
                    if (i < offset) {
                        docArray[i] = 0;
                    } else if (i == offset) {
                        docArray[i] = 1;
                    } else {
                        docArray[i] = allowDummyDocs ? 2 : 1;
                    }
                }
            }
            BlockLoader.Docs docs = TestBlock.docs(docArray);
            try (BlockLoader.ColumnAtATimeReader reader = columnAtATimeReaderSource.apply(breaker)) {
                var block = (TestBlock) reader.read(TestBlock.factory(), docs, offset, false);
                assertThat(block.size(), equalTo(docArray.length - offset));
                return block.get(0);
            }
        }

        StoredFieldsSpec storedFieldsSpec = blockLoader.rowStrideStoredFieldSpec();
        SourceLoader.Leaf leafSourceLoader = null;
        if (storedFieldsSpec.requiresSource()) {
            var sourceLoader = mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP, null);
            leafSourceLoader = sourceLoader.leaf(context, null);
            storedFieldsSpec = storedFieldsSpec.merge(
                new StoredFieldsSpec(true, storedFieldsSpec.requiresMetadata(), sourceLoader.requiredStoredFields())
            );
        }
        BlockLoaderStoredFieldsFromLeafLoader storedFieldsLoader = new BlockLoaderStoredFieldsFromLeafLoader(
            StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(context, null),
            leafSourceLoader
        );
        storedFieldsLoader.advanceTo(1);

        BlockLoader.Builder builder = blockLoader.builder(TestBlock.factory(), 1);
        try (BlockLoader.RowStrideReader reader = blockLoader.rowStrideReader(breaker, context)) {
            reader.read(1, storedFieldsLoader, builder);
            var block = (TestBlock) builder.build();
            assertThat(block.size(), equalTo(1));
            return block.get(0);
        }
    }

    private BlockLoader createBlockLoader(String fieldName) {
        return mapperService.fieldType(fieldName).blockLoader(new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapperService) {
            @Override
            public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                return params.preference();
            }
        });
    }

    // Copied from org.hamcrest.core.IsEqual and modified to pretty print failure when bytesref
    static class PrettyEqual<T> extends BaseMatcher<T> {

        private final Object expectedValue;

        PrettyEqual(T equalArg) {
            expectedValue = equalArg;
        }

        @Override
        public boolean matches(Object actualValue) {
            return areEqual(actualValue, expectedValue);
        }

        @Override
        public void describeTo(Description description) {
            description.appendValue(attemptMakeReadable(expectedValue));
        }

        @Override
        public void describeMismatch(Object item, Description description) {
            super.describeMismatch(attemptMakeReadable(item), description);
        }

        private static boolean areEqual(Object actual, Object expected) {
            if (actual == null) {
                return expected == null;
            }

            if (expected != null && isArray(actual)) {
                return isArray(expected) && areArraysEqual(actual, expected);
            }

            return actual.equals(expected);
        }

        private static boolean areArraysEqual(Object actualArray, Object expectedArray) {
            return areArrayLengthsEqual(actualArray, expectedArray) && areArrayElementsEqual(actualArray, expectedArray);
        }

        private static boolean areArrayLengthsEqual(Object actualArray, Object expectedArray) {
            return Array.getLength(actualArray) == Array.getLength(expectedArray);
        }

        private static boolean areArrayElementsEqual(Object actualArray, Object expectedArray) {
            for (int i = 0; i < Array.getLength(actualArray); i++) {
                if (areEqual(Array.get(actualArray, i), Array.get(expectedArray, i)) == false) {
                    return false;
                }
            }
            return true;
        }

        private static boolean isArray(Object o) {
            return o.getClass().isArray();
        }

        // Attempt to make assertions readable:
        static Object attemptMakeReadable(Object expected) {
            try {
                if (expected instanceof BytesRef bytesRef) {
                    expected = bytesRef.utf8ToString();
                } else if (expected instanceof List<?> list && list.getFirst() instanceof BytesRef) {
                    List<String> expectedList = new ArrayList<>(list.size());
                    for (Object e : list) {
                        expectedList.add(((BytesRef) e).utf8ToString());
                    }
                    expected = expectedList;
                }
                return expected;
            } catch (Exception | AssertionError e) {
                // ip/geo fields can't be converted to strings:
                return expected;
            }
        }

        public static <T> Matcher<T> prettyEqualTo(T operand) {
            return new PrettyEqual<>(operand);
        }

    }
}
