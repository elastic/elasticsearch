/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.ElasticsearchStoredFieldsFormat;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Shared coverage of the null contract of the strict columnar index modes for one field type at a time: a null array element is
 * dropped after {@code null_value}, a field with no remaining value is absent from {@code exists} and from the reconstructed
 * {@code _source}, and a single remaining value is reconstructed as a scalar.
 *
 * <p>Each helper indexes the same documents with an {@link IndexingStrategy}, in each strict mode and with the synthetic and the
 * {@code columnar_stored} source. Batch indexing is the path
 * {@code InternalEngine} takes. Any exception from the mappers makes the chunk fall back to document indexing, as it does in
 * production, so a scenario states which column shapes it expects to execute as a batch today ({@link Scenario#requireBatchFor}) and a
 * helper fails when one of them falls back. A shape that is not listed may execute either way, which keeps the tests valid when batch
 * indexing learns to map more shapes. A test of a scenario that lists no shape exercises the fallback, not batch indexing.
 *
 * <p>Order and multiplicity of the remaining values belong to the array-order tests. The source check compares the distinct values that
 * remain, and {@link #assertSourceKeepsTheOrderOfTheRemainingValues} only checks that dropping a null does not move or lose a distinct
 * value that remains, so that each of the two fails for its own reason.
 */
public abstract class AbstractColumnarNullContractTestCase extends MapperServiceTestCase {

    protected static final String FIELD = "field";
    private static final long TIMESTAMP = 1735689600000L;
    private static final List<IndexMode> MODES = List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR, IndexMode.VECTORDB_COLUMNAR);

    protected static final Predicate<Configuration> ALL = configuration -> true;

    private static final String SOURCE_MODE = IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey();

    protected static final Predicate<Configuration> SYNTHETIC_SOURCE = configuration -> configuration
        .sourceMode() == SourceFieldMapper.Mode.SYNTHETIC;
    protected static final Predicate<Configuration> COLUMNAR_STORED_SOURCE = configuration -> configuration
        .sourceMode() == SourceFieldMapper.Mode.COLUMNAR_STORED;

    private static final String NUMERIC_CODEC = IndexSettings.BINARY_NUMERIC_DOC_VALUES_ENABLED_SETTING.getKey();
    protected static final Predicate<Configuration> NUMERIC_CODEC_ENABLED = Configuration::numericCodec;

    protected enum Shape {
        SCALARS,
        ARRAYS,
        BARE_NULL,
        NULL_ELEMENTS
    }

    protected record DocumentGroup(Shape shape, List<TestDocument> documents) {}

    private enum Kind {
        ABSENT,
        BARE_NULL,
        SCALAR,
        ARRAY
    }

    protected enum IndexSetting {
        ENABLED,
        DISABLED,
        DEFAULT
    }

    /**
     * @param index        the value of the {@code index} parameter, or {@code DEFAULT} when the mapping leaves it to the mapper
     * @param sourceMode   how the {@code _source} is kept
     * @param numericCodec whether a numeric field writes the numeric codec
     */
    protected record Configuration(IndexMode mode, IndexSetting index, SourceFieldMapper.Mode sourceMode, boolean numericCodec) {
        Settings settings() {
            final Settings.Builder settings = Settings.builder().put(SOURCE_MODE, sourceMode.toString());
            if (numericCodec) {
                settings.put(NUMERIC_CODEC, true);
            }
            return settings.build();
        }
    }

    protected record TestDocument(Kind kind, List<Object> slots) {
        String json() throws IOException {
            final XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startObject().field("@timestamp", TIMESTAMP);
            switch (kind) {
                case ABSENT -> {
                }
                case BARE_NULL -> builder.nullField(FIELD);
                case SCALAR -> builder.field(FIELD, slots.get(0));
                case ARRAY -> {
                    builder.startArray(FIELD);
                    for (final Object slot : slots) {
                        if (slot == null) {
                            builder.nullValue();
                        } else {
                            builder.value(slot);
                        }
                    }
                    builder.endArray();
                }
            }
            return Strings.toString(builder.endObject());
        }
    }

    /**
     * A field to test.
     *
     * @param type          the mapping type
     * @param parameters    further mapping parameters of the field
     * @param values        a source of values of the type, in the form the mapper accepts them
     * @param nullValue     the {@code null_value} of the field, or {@code null} when it has none
     * @param canonical     the form in which a value is compared, applied to the expected values and to those in the reconstructed source
     * @param varyIndex     whether the tests run the {@code index} parameter both ways, or leave it to the mapper default
     * @param batchRequired the configurations and column shapes that are known to execute as a batch today
     * @param singleValued  whether the type rejects a document with more than one value, which the documents then respect
     * @param indexable     whether a value is indexed, which is false for a value that {@code ignore_malformed} keeps in the source only
     */
    protected record Scenario(
        String type,
        Map<String, Object> parameters,
        Supplier<Object> values,
        Object nullValue,
        Function<Object, String> canonical,
        boolean varyIndex,
        BiPredicate<Configuration, Shape> batchRequired,
        boolean singleValued,
        Predicate<Object> indexable
    ) {
        public Scenario withParameters(Map<String, Object> parameters) {
            return new Scenario(type, parameters, values, nullValue, canonical, varyIndex, batchRequired, singleValued, indexable);
        }

        public Scenario withNullValue(Object nullValue) {
            final Map<String, Object> withNullValue = new HashMap<>(parameters);
            withNullValue.put("null_value", nullValue);
            return new Scenario(type, withNullValue, values, nullValue, canonical, varyIndex, batchRequired, singleValued, indexable);
        }

        public Scenario withValues(Supplier<Object> values) {
            return new Scenario(type, parameters, values, nullValue, canonical, varyIndex, batchRequired, singleValued, indexable);
        }

        public Scenario withCanonical(Function<Object, String> canonical) {
            return new Scenario(type, parameters, values, nullValue, canonical, varyIndex, batchRequired, singleValued, indexable);
        }

        public Scenario withDefaultIndexSetting() {
            return new Scenario(type, parameters, values, nullValue, canonical, false, batchRequired, singleValued, indexable);
        }

        public Scenario asSingleValued() {
            return new Scenario(type, parameters, values, nullValue, canonical, varyIndex, batchRequired, true, indexable);
        }

        public Scenario withIndexable(Predicate<Object> indexable) {
            return new Scenario(type, parameters, values, nullValue, canonical, varyIndex, batchRequired, singleValued, indexable);
        }

        public Scenario requireBatchFor(Set<Shape> shapes) {
            return requireBatchFor((configuration, shape) -> shapes.contains(shape));
        }

        public Scenario requireBatchFor(BiPredicate<Configuration, Shape> batchRequired) {
            return new Scenario(type, parameters, values, nullValue, canonical, varyIndex, batchRequired, singleValued, indexable);
        }

        boolean hasValue(TestDocument document) {
            return expected(document).stream().anyMatch(indexable);
        }

        List<Object> expected(TestDocument document) {
            if (document.kind() == Kind.BARE_NULL) {
                return nullValue == null ? List.of() : List.of(nullValue);
            }
            final List<Object> expected = new ArrayList<>();
            for (final Object slot : document.slots()) {
                if (slot != null) {
                    expected.add(slot);
                } else if (nullValue != null) {
                    expected.add(nullValue);
                }
            }
            return expected;
        }
    }

    protected static Scenario scenario(String type, Supplier<Object> values) {
        return new Scenario(type, Map.of(), values, null, String::valueOf, true, (configuration, group) -> false, false, value -> true);
    }

    protected static TestDocument absent() {
        return new TestDocument(Kind.ABSENT, List.of());
    }

    protected static TestDocument bareNull() {
        return new TestDocument(Kind.BARE_NULL, List.of());
    }

    protected static TestDocument scalar(Object value) {
        return new TestDocument(Kind.SCALAR, Collections.singletonList(value));
    }

    protected static TestDocument array(Object... slots) {
        return new TestDocument(Kind.ARRAY, Arrays.asList(slots));
    }

    // NOTE: each group is one column shape. Batch indexing maps a column of mixed scalars and arrays, or one that holds a null, as a
    // UNION, which most mappers refuse, so a group that mixes shapes would hide whether the others execute as a batch.
    private static List<DocumentGroup> documentGroups(Scenario scenario) {
        final Supplier<Object> values = scenario.values();
        if (scenario.singleValued()) {
            return List.of(
                new DocumentGroup(Shape.SCALARS, List.of(scalar(values.get()), absent(), scalar(values.get()))),
                new DocumentGroup(Shape.ARRAYS, List.of(array(values.get()), array())),
                new DocumentGroup(Shape.BARE_NULL, List.of(scalar(values.get()), bareNull())),
                new DocumentGroup(
                    Shape.NULL_ELEMENTS,
                    List.of(
                        array((Object) null),
                        array(null, null),
                        array(values.get(), null),
                        array(null, values.get()),
                        array(null, values.get(), null)
                    )
                )
            );
        }
        final Object repeated = values.get();
        return List.of(
            new DocumentGroup(Shape.SCALARS, List.of(scalar(values.get()), absent(), scalar(values.get()))),
            new DocumentGroup(
                Shape.ARRAYS,
                List.of(array(values.get()), array(), array(values.get(), values.get()), array(repeated, values.get(), repeated))
            ),
            new DocumentGroup(Shape.BARE_NULL, List.of(scalar(values.get()), bareNull())),
            new DocumentGroup(
                Shape.NULL_ELEMENTS,
                List.of(
                    array((Object) null),
                    array(null, null),
                    array(values.get(), null),
                    array(null, values.get()),
                    array(values.get(), null, values.get()),
                    array(null, values.get(), null, values.get(), null)
                )
            )
        );
    }

    private static List<DocumentGroup> orderedDocumentGroups(Supplier<Object> values) {
        final Object first = values.get();
        Object second = values.get();
        for (int attempt = 0; attempt < 100 && first.equals(second); attempt++) {
            second = values.get();
        }
        assertNotEquals("the values of the scenario must offer two distinct values", first, second);
        return List.of(
            new DocumentGroup(
                Shape.NULL_ELEMENTS,
                List.of(
                    array(first, null, second),
                    array(null, first, second),
                    array(first, second, null),
                    array(second, null, first, null, null)
                )
            )
        );
    }

    /**
     * The configurations that every check runs in: each strict mode, with and without {@code index} where the type has it, with
     * the {@code _source} reconstructed from doc values and stored by {@code columnar_stored}, and for a {@code long} with the numeric
     * codec when it is available.
     */
    protected List<Configuration> configurations(Scenario scenario) {
        final List<Configuration> configurations = new ArrayList<>();
        for (final IndexMode mode : MODES) {
            for (final SourceFieldMapper.Mode sourceMode : List.of(
                SourceFieldMapper.Mode.SYNTHETIC,
                SourceFieldMapper.Mode.COLUMNAR_STORED
            )) {
                if (scenario.varyIndex()) {
                    configurations.add(new Configuration(mode, IndexSetting.ENABLED, sourceMode, false));
                    configurations.add(new Configuration(mode, IndexSetting.DISABLED, sourceMode, false));
                } else {
                    configurations.add(new Configuration(mode, IndexSetting.DEFAULT, sourceMode, false));
                }
            }
        }
        // NOTE: only a long field writes the numeric codec so far. Its index setting exists while the feature flag is enabled.
        if (scenario.type().equals("long") && IndexSettings.BINARY_NUMERIC_DOC_VALUES_FEATURE_FLAG.isEnabled()) {
            for (final Configuration configuration : List.copyOf(configurations)) {
                configurations.add(new Configuration(configuration.mode(), configuration.index(), configuration.sourceMode(), true));
            }
        }
        return configurations;
    }

    protected MapperService createMapperService(Scenario scenario, Configuration configuration) throws IOException {
        return createMapperService(
            IndexVersion.current(),
            Settings.builder().put(IndexSettings.MODE.getKey(), configuration.mode().getName()).put(configuration.settings()).build(),
            mapping(b -> {
                b.startObject("@timestamp").field("type", "date").endObject();
                b.startObject(FIELD).field("type", scenario.type());
                if (configuration.index() != IndexSetting.DEFAULT) {
                    b.field("index", configuration.index() == IndexSetting.ENABLED);
                }
                for (final Map.Entry<String, Object> parameter : scenario.parameters().entrySet()) {
                    b.field(parameter.getKey(), parameter.getValue());
                }
                b.endObject();
            })
        );
    }

    private static IndexWriterConfig indexWriterConfig(MapperService mapperService) {
        return new IndexWriterConfig(IndexShard.buildIndexAnalyzer(mapperService)).setMergePolicy(new LogDocMergePolicy())
            .setCodec(
                new PerFieldMapperCodec(
                    Lucene104Codec.Mode.BEST_SPEED,
                    ElasticsearchStoredFieldsFormat.Mode.LUCENE,
                    ElasticsearchStoredFieldsFormat.Mode.LUCENE,
                    mapperService,
                    BigArrays.NON_RECYCLING_INSTANCE,
                    null
                )
            );
    }

    private static List<String> sources(List<TestDocument> documents) throws IOException {
        final List<String> sources = new ArrayList<>(documents.size());
        for (final TestDocument document : documents) {
            sources.add(document.json());
        }
        return sources;
    }

    private interface IndexedAssertion {
        void accept(Configuration configuration, MapperService mapperService, IndexWriter indexWriter, List<TestDocument> documents)
            throws IOException;
    }

    private static void assertExecutedAs(
        Scenario scenario,
        IndexingStrategy strategy,
        Configuration configuration,
        Shape shape,
        IndexingStrategy.Outcome outcome
    ) {
        switch (strategy) {
            case DOCUMENT -> assertDocument(scenario, configuration, shape, outcome);
            case BATCH -> assertBatch(scenario, configuration, shape, outcome);
        }
    }

    private static void assertDocument(Scenario scenario, Configuration configuration, Shape shape, IndexingStrategy.Outcome outcome) {
        assertFalse(
            scenario.type() + " " + configuration + " column shape " + shape + " was indexed as a batch by DOCUMENT",
            outcome.batched()
        );
    }

    private static void assertBatch(Scenario scenario, Configuration configuration, Shape shape, IndexingStrategy.Outcome outcome) {
        if (scenario.batchRequired().test(configuration, shape)) {
            assertTrue(
                scenario.type()
                    + " "
                    + configuration
                    + " column shape "
                    + shape
                    + " is expected to execute as a batch but fell back to document indexing: "
                    + outcome.fallbackReason(),
                outcome.batched()
            );
        }
    }

    private static void indexGroups(
        Scenario scenario,
        IndexingStrategy strategy,
        Configuration configuration,
        MapperService mapperService,
        IndexWriter indexWriter,
        List<DocumentGroup> groups
    ) throws IOException {
        int firstSeqNo = 0;
        for (final DocumentGroup group : groups) {
            final IndexingStrategy.Outcome outcome = strategy.index(mapperService, indexWriter, sources(group.documents()), firstSeqNo);
            assertExecutedAs(scenario, strategy, configuration, group.shape(), outcome);
            indexWriter.commit();
            firstSeqNo += group.documents().size();
        }
        try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
            assertEquals("one segment per group", groups.size(), reader.leaves().size());
        }
    }

    private void forEachConfiguration(
        Scenario scenario,
        IndexingStrategy strategy,
        Predicate<Configuration> slice,
        List<DocumentGroup> groups,
        IndexedAssertion assertion
    ) throws IOException {
        final List<TestDocument> documents = groups.stream().flatMap(group -> group.documents().stream()).toList();
        final List<String> failures = new ArrayList<>();
        final List<Configuration> configurations = configurations(scenario).stream().filter(slice).toList();
        assertFalse("the slice selects no configuration of " + scenario.type(), configurations.isEmpty());
        for (final Configuration configuration : configurations) {
            final MapperService mapperService = createMapperService(scenario, configuration);
            // NOTE: newDirectory() keeps every directory and its creation stack referenced until the suite ends. These tests create one
            // per configuration, which exhausts a small heap under -Dtests.iters. This wrapper still checks the index on close.
            try (
                Directory directory = new MockDirectoryWrapper(random(), new ByteBuffersDirectory());
                IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig(mapperService))
            ) {
                indexGroups(scenario, strategy, configuration, mapperService, indexWriter, groups);
                try {
                    assertion.accept(configuration, mapperService, indexWriter, documents);
                } catch (AssertionError e) {
                    failures.add(configuration + ": " + e.getMessage());
                }
            }
        }
        if (failures.isEmpty() == false) {
            fail(failures.size() + " of " + configurations.size() + " configurations failed:\n" + String.join("\n", failures));
        }
    }

    protected final void assertExistsMatchesOnlyDocumentsWithValue(Scenario scenario, IndexingStrategy strategy) throws IOException {
        assertExistsMatchesOnlyDocumentsWithValue(scenario, strategy, ALL);
    }

    protected final void assertExistsMatchesOnlyDocumentsWithValue(
        Scenario scenario,
        IndexingStrategy strategy,
        Predicate<Configuration> slice
    ) throws IOException {
        forEachConfiguration(scenario, strategy, slice, documentGroups(scenario), (configuration, mapperService, indexWriter, docs) -> {
            // NOTE: the deleted document is the last one, which sits in the segment of the null elements, and the documents before it
            // keep their ids when the deletion is expunged by the merge.
            final int deleted = docs.size() - 1;
            final Query exists = mapperService.fieldType(FIELD).existsQuery(createSearchExecutionContext(mapperService));
            for (final String stage : List.of("indexed", "deleted", "merged")) {
                if (stage.equals("deleted")) {
                    indexWriter.deleteDocuments(new Term(IdFieldMapper.NAME, Uid.encodeId("d" + deleted)));
                } else if (stage.equals("merged")) {
                    indexWriter.forceMerge(1);
                }
                final List<Integer> expected = new ArrayList<>();
                for (int i = 0; i < docs.size(); i++) {
                    if (scenario.hasValue(docs.get(i)) && (i != deleted || stage.equals("indexed"))) {
                        expected.add(i);
                    }
                }
                try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
                    assertEquals(configuration + " " + stage, stage.equals("deleted"), reader.numDeletedDocs() > 0);
                    final List<Integer> matched = new ArrayList<>();
                    for (final ScoreDoc hit : new IndexSearcher(reader).search(exists, docs.size()).scoreDocs) {
                        matched.add(hit.doc);
                    }
                    Collections.sort(matched);
                    assertEquals(configuration + " " + stage + " documents=" + docs, expected, matched);
                }
            }
        });
    }

    protected final void assertExistsCountAgreesWithHits(Scenario scenario, IndexingStrategy strategy) throws IOException {
        assertExistsCountAgreesWithHits(scenario, strategy, ALL);
    }

    protected final void assertExistsCountAgreesWithHits(Scenario scenario, IndexingStrategy strategy, Predicate<Configuration> slice)
        throws IOException {
        forEachConfiguration(scenario, strategy, slice, documentGroups(scenario), (configuration, mapperService, indexWriter, docs) -> {
            final Query exists = mapperService.fieldType(FIELD).existsQuery(createSearchExecutionContext(mapperService));
            for (final String stage : List.of("indexed", "deleted", "merged")) {
                if (stage.equals("deleted")) {
                    indexWriter.deleteDocuments(
                        new Term(IdFieldMapper.NAME, Uid.encodeId("d0")),
                        new Term(IdFieldMapper.NAME, Uid.encodeId("d" + (docs.size() - 1)))
                    );
                } else if (stage.equals("merged")) {
                    indexWriter.forceMerge(1);
                }
                try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
                    assertEquals(configuration + " " + stage, stage.equals("deleted"), reader.numDeletedDocs() > 0);
                    final IndexSearcher searcher = new IndexSearcher(reader);
                    assertEquals(
                        configuration + " " + stage + " documents=" + docs,
                        searcher.search(exists, docs.size()).scoreDocs.length,
                        searcher.count(exists)
                    );
                }
            }
        });
    }

    protected final void assertSourceDropsNullElements(Scenario scenario, IndexingStrategy strategy) throws IOException {
        forEachConfiguration(scenario, strategy, ALL, documentGroups(scenario), (configuration, mapperService, indexWriter, docs) -> {
            assertSources(scenario, mapperService, indexWriter, docs, AbstractColumnarNullContractTestCase::assertSourceDropsNulls);
        });
    }

    // NOTE: compares the two source modes as they behave today, nulls included. It does not depend on the null contract, so it passes
    // while the tests of that contract are muted, and it fails if `columnar_stored` returns a source that differs from the synthetic one.
    protected final void assertSourceModesAgree(Scenario scenario, IndexingStrategy strategy) throws IOException {
        final List<DocumentGroup> groups = documentGroups(scenario);
        forEachConfiguration(scenario, strategy, COLUMNAR_STORED_SOURCE, groups, (configuration, stored, storedWriter, docs) -> {
            final Configuration syntheticConfiguration = new Configuration(
                configuration.mode(),
                configuration.index(),
                SourceFieldMapper.Mode.SYNTHETIC,
                configuration.numericCodec()
            );
            final MapperService synthetic = createMapperService(scenario, syntheticConfiguration);
            try (
                Directory directory = new MockDirectoryWrapper(random(), new ByteBuffersDirectory());
                IndexWriter syntheticWriter = new IndexWriter(directory, indexWriterConfig(synthetic))
            ) {
                indexGroups(scenario, strategy, syntheticConfiguration, synthetic, syntheticWriter, groups);
                for (final boolean merged : new boolean[] { false, true }) {
                    if (merged) {
                        storedWriter.forceMerge(1);
                        syntheticWriter.forceMerge(1);
                    }
                    final List<String> storedSources = sourcesOfAllSegments(stored, storedWriter, merged);
                    final List<String> syntheticSources = sourcesOfAllSegments(synthetic, syntheticWriter, merged);
                    assertEquals("documents", syntheticSources.size(), storedSources.size());
                    for (int i = 0; i < docs.size(); i++) {
                        assertEquals(
                            configuration + " merged=" + merged + " document=" + docs.get(i),
                            XContentHelper.convertToMap(JsonXContent.jsonXContent, syntheticSources.get(i), false),
                            XContentHelper.convertToMap(JsonXContent.jsonXContent, storedSources.get(i), false)
                        );
                    }
                }
            }
        });
    }

    protected final void assertSourceHoldsTheValues(Scenario scenario, IndexingStrategy strategy) throws IOException {
        forEachConfiguration(
            scenario,
            strategy,
            ALL,
            documentGroups(scenario).stream().filter(group -> group.shape() == Shape.SCALARS || group.shape() == Shape.ARRAYS).toList(),
            (configuration, mapperService, indexWriter, docs) -> {
                assertSources(scenario, mapperService, indexWriter, docs, AbstractColumnarNullContractTestCase::assertSourceDropsNulls);
            }
        );
    }

    protected final void assertSourceKeepsTheOrderOfTheRemainingValues(Scenario scenario, IndexingStrategy strategy) throws IOException {
        forEachConfiguration(
            scenario,
            strategy,
            ALL,
            orderedDocumentGroups(scenario.values()),
            (configuration, mapperService, indexWriter, docs) -> {
                assertSources(scenario, mapperService, indexWriter, docs, AbstractColumnarNullContractTestCase::assertSourceKeepsSequence);
            }
        );
    }

    /**
     * Returns the reconstructed {@code _source} of every document in index order, reading each segment with its own document ids.
     * It does not merge: a caller that passes {@code merged} has force merged the index to one segment first.
     */
    protected final List<String> sourcesOfAllSegments(MapperService mapperService, IndexWriter indexWriter, boolean merged)
        throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
            if (merged) {
                assertEquals("segments after the merge", 1, reader.leaves().size());
            }
            final SourceProvider provider = SourceProvider.fromLookup(mapperService.mappingLookup(), null, SourceFieldMetrics.NOOP, null);
            final List<String> sources = new ArrayList<>();
            for (final LeafReaderContext leaf : reader.leaves()) {
                for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                    sources.add(provider.getSource(leaf, doc).internalSourceRef().utf8ToString());
                }
            }
            return sources;
        }
    }

    private void assertSources(
        Scenario scenario,
        MapperService mapperService,
        IndexWriter indexWriter,
        List<TestDocument> documents,
        SourceComparison comparison
    ) throws IOException {
        final List<String> failures = new ArrayList<>();
        for (final boolean merged : new boolean[] { false, true }) {
            if (merged) {
                indexWriter.forceMerge(1);
            }
            final List<String> sources = sourcesOfAllSegments(mapperService, indexWriter, merged);
            assertEquals("documents", documents.size(), sources.size());
            for (int i = 0; i < documents.size(); i++) {
                try {
                    comparison.accept(scenario, scenario.expected(documents.get(i)), sources.get(i));
                } catch (AssertionError e) {
                    failures.add("merged=" + merged + " document=" + documents.get(i) + " source=" + sources.get(i) + " " + e.getMessage());
                }
            }
        }
        if (failures.isEmpty() == false) {
            fail(failures.size() + " documents failed:\n" + String.join("\n", failures));
        }
    }

    private interface SourceComparison {
        void accept(Scenario scenario, List<Object> expected, String source) throws IOException;
    }

    // NOTE: compares distinct values on purpose, because the multiplicity of repeated values belongs to #166868.
    protected static void assertSourceDropsNulls(Scenario scenario, List<Object> expected, String source) throws IOException {
        final Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, source, false);
        if (expected.isEmpty()) {
            assertFalse("a field without a value is absent", parsed.containsKey(FIELD));
            return;
        }
        assertTrue("a field with a value is present", parsed.containsKey(FIELD));
        final Object actual = parsed.get(FIELD);
        final List<?> actualValues = actual instanceof List<?> list ? list : Collections.singletonList(actual);
        assertFalse("the source holds no null", actualValues.contains(null));
        final SortedSet<String> expectedDistinct = new TreeSet<>(canonicalValues(scenario, expected));
        if (expectedDistinct.size() == 1 && expected.size() == 1) {
            assertFalse("a single remaining value is a scalar", actual instanceof List);
        } else if (expectedDistinct.size() > 1) {
            assertTrue("several remaining values are an array", actual instanceof List);
        }
        assertEquals(expectedDistinct, new TreeSet<>(canonicalValues(scenario, actualValues)));
    }

    // NOTE: a null that the source still holds is left out. Whether nulls are dropped is the null contract and is checked by
    // assertSourceDropsNulls, so that each of the two fails for its own reason.
    protected static void assertSourceKeepsSequence(Scenario scenario, List<Object> expected, String source) throws IOException {
        final Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, source, false);
        if (expected.isEmpty()) {
            assertFalse("a field without a value is absent", parsed.containsKey(FIELD));
            return;
        }
        assertTrue("a field with a value is present", parsed.containsKey(FIELD));
        final Object actual = parsed.get(FIELD);
        final List<?> actualValues = actual instanceof List<?> list ? list : Collections.singletonList(actual);
        assertEquals(
            canonicalValues(scenario, expected),
            canonicalValues(scenario, actualValues.stream().filter(value -> value != null).toList())
        );
    }

    private static List<String> canonicalValues(Scenario scenario, List<?> values) {
        return values.stream().map(value -> scenario.canonical().apply(value)).toList();
    }
}
