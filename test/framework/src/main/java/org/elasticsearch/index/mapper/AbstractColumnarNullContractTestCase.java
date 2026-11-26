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
import org.apache.lucene.index.LogDocMergePolicy;
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
 * <p>Each helper indexes the same documents with an {@link IndexingStrategy}, in each strict mode. Batch indexing is the path
 * {@code InternalEngine} takes. Any exception from the mappers makes the chunk fall back to document indexing, as it does in
 * production, so a scenario states which column shapes it expects to execute as a batch today ({@link Scenario#batchedGroups}) and a
 * helper fails when one of them falls back. A shape that is not listed may execute either way, which keeps the tests valid when batch
 * indexing learns to map more shapes. A test of a scenario that lists no shape exercises the fallback, not batch indexing.
 *
 * <p>Order and multiplicity of the remaining values belong to the array-order tests. The source check compares the distinct values that
 * remain, and {@link #assertSourceKeepsTheOrderOfTheRemainingValues} only checks that dropping a null does not move or lose the values
 * that remain, so that each of the two fails for its own reason.
 */
public abstract class AbstractColumnarNullContractTestCase extends MapperServiceTestCase {

    protected static final String FIELD = "field";
    protected static final int SCALARS = 0;
    protected static final int ARRAYS = 1;
    protected static final int BARE_NULL = 2;
    protected static final int NULL_ELEMENTS = 3;

    private static final List<Integer> DOCUMENT_SHAPES = List.of(SCALARS, ARRAYS, BARE_NULL, NULL_ELEMENTS);
    private static final long TIMESTAMP = 1735689600000L;
    private static final List<IndexMode> MODES = List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR, IndexMode.VECTORDB_COLUMNAR);

    protected static final Predicate<Configuration> ALL = configuration -> true;

    private enum Kind {
        ABSENT,
        BARE_NULL,
        SCALAR,
        ARRAY
    }

    /**
     * @param indexed the value of the {@code index} parameter, which is always {@code true} for a type that has no such parameter
     */
    protected record Configuration(IndexMode mode, boolean indexed) {}

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
     * @param hasIndex      whether the type has an {@code index} parameter, which the tests run both ways
     * @param batchedGroups the configurations and column shapes that are known to execute as a batch today
     * @param modes         the strict modes that accept the type
     * @param singleValued  whether the type rejects a document with more than one value, which the documents then respect
     */
    protected record Scenario(
        String type,
        Map<String, Object> parameters,
        Supplier<Object> values,
        Object nullValue,
        Function<Object, String> canonical,
        boolean hasIndex,
        BiPredicate<Configuration, Integer> batchedGroups,
        List<IndexMode> modes,
        boolean singleValued
    ) {
        public Scenario withParameters(Map<String, Object> parameters) {
            return new Scenario(type, parameters, values, nullValue, canonical, hasIndex, batchedGroups, modes, singleValued);
        }

        public Scenario withNullValue(Object nullValue) {
            final Map<String, Object> withNullValue = new HashMap<>(parameters);
            withNullValue.put("null_value", nullValue);
            return new Scenario(type, withNullValue, values, nullValue, canonical, hasIndex, batchedGroups, modes, singleValued);
        }

        public Scenario withValues(Supplier<Object> values) {
            return new Scenario(type, parameters, values, nullValue, canonical, hasIndex, batchedGroups, modes, singleValued);
        }

        public Scenario withCanonical(Function<Object, String> canonical) {
            return new Scenario(type, parameters, values, nullValue, canonical, hasIndex, batchedGroups, modes, singleValued);
        }

        public Scenario withoutIndex() {
            return new Scenario(type, parameters, values, nullValue, canonical, false, batchedGroups, modes, singleValued);
        }

        public Scenario asSingleValued() {
            return new Scenario(type, parameters, values, nullValue, canonical, hasIndex, batchedGroups, modes, true);
        }

        public Scenario batchedGroups(BiPredicate<Configuration, Integer> batchedGroups) {
            return new Scenario(type, parameters, values, nullValue, canonical, hasIndex, batchedGroups, modes, singleValued);
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
        return new Scenario(type, Map.of(), values, null, String::valueOf, true, (configuration, group) -> false, MODES, false);
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
    private static List<List<TestDocument>> documentGroups(Scenario scenario) {
        final Supplier<Object> values = scenario.values();
        if (scenario.singleValued()) {
            return List.of(
                List.of(scalar(values.get()), absent(), scalar(values.get())),
                List.of(array(values.get()), array()),
                List.of(scalar(values.get()), bareNull()),
                List.of(array((Object) null), array(values.get(), null), array(null, values.get()))
            );
        }
        final Object repeated = values.get();
        return List.of(
            List.of(scalar(values.get()), absent(), scalar(values.get())),
            List.of(array(values.get()), array(), array(values.get(), values.get()), array(repeated, values.get(), repeated)),
            List.of(scalar(values.get()), bareNull()),
            List.of(
                array((Object) null),
                array(null, null),
                array(values.get(), null),
                array(null, values.get()),
                array(values.get(), null, values.get()),
                array(null, values.get(), null, values.get(), null)
            )
        );
    }

    private static List<List<TestDocument>> orderedDocumentGroups(Supplier<Object> values) {
        final Object first = values.get();
        Object second = values.get();
        for (int attempt = 0; attempt < 100 && first.equals(second); attempt++) {
            second = values.get();
        }
        assertNotEquals("the values of the scenario must offer two distinct values", first, second);
        return List.of(
            List.of(
                array(first, null, second),
                array(null, first, second),
                array(first, second, null),
                array(second, null, first, null, null),
                array(second, null, first, second)
            )
        );
    }

    private static List<Configuration> configurations(Scenario scenario) {
        final List<Configuration> configurations = new ArrayList<>();
        for (final IndexMode mode : scenario.modes()) {
            configurations.add(new Configuration(mode, true));
            if (scenario.hasIndex()) {
                configurations.add(new Configuration(mode, false));
            }
        }
        return configurations;
    }

    private MapperService createMapperService(Scenario scenario, Configuration configuration) throws IOException {
        return createMapperService(
            IndexVersion.current(),
            Settings.builder().put(IndexSettings.MODE.getKey(), configuration.mode().getName()).build(),
            mapping(b -> {
                b.startObject("@timestamp").field("type", "date").endObject();
                b.startObject(FIELD).field("type", scenario.type());
                if (scenario.hasIndex()) {
                    b.field("index", configuration.indexed());
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

    private void forEachConfiguration(
        Scenario scenario,
        IndexingStrategy strategy,
        Predicate<Configuration> slice,
        List<List<TestDocument>> groups,
        List<Integer> shapes,
        IndexedAssertion assertion
    ) throws IOException {
        final List<TestDocument> documents = groups.stream().flatMap(List::stream).toList();
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
                int firstSeqNo = 0;
                for (int g = 0; g < groups.size(); g++) {
                    final IndexingStrategy.Outcome outcome = strategy.index(mapperService, indexWriter, sources(groups.get(g)), firstSeqNo);
                    if (strategy == IndexingStrategy.BATCH && scenario.batchedGroups().test(configuration, shapes.get(g))) {
                        assertTrue(
                            scenario.type()
                                + " "
                                + configuration
                                + " column shape "
                                + shapes.get(g)
                                + " is expected to execute as a batch but fell back to document indexing: "
                                + outcome.fallbackReason(),
                            outcome.batched()
                        );
                    }
                    indexWriter.commit();
                    firstSeqNo += groups.get(g).size();
                }
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
        forEachConfiguration(
            scenario,
            strategy,
            slice,
            documentGroups(scenario),
            DOCUMENT_SHAPES,
            (configuration, mapperService, indexWriter, docs) -> {
                final List<Integer> expected = new ArrayList<>();
                for (int i = 0; i < docs.size(); i++) {
                    if (scenario.expected(docs.get(i)).isEmpty() == false) {
                        expected.add(i);
                    }
                }
                final Query exists = mapperService.fieldType(FIELD).existsQuery(createSearchExecutionContext(mapperService));
                for (final boolean merged : new boolean[] { false, true }) {
                    if (merged) {
                        indexWriter.forceMerge(1);
                    }
                    try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
                        final List<Integer> matched = new ArrayList<>();
                        for (final ScoreDoc hit : new IndexSearcher(reader).search(exists, docs.size()).scoreDocs) {
                            matched.add(hit.doc);
                        }
                        Collections.sort(matched);
                        assertEquals(configuration + " merged=" + merged + " documents=" + docs, expected, matched);
                    }
                }
            }
        );
    }

    protected final void assertExistsCountAgreesWithHits(Scenario scenario, IndexingStrategy strategy) throws IOException {
        assertExistsCountAgreesWithHits(scenario, strategy, ALL);
    }

    protected final void assertExistsCountAgreesWithHits(Scenario scenario, IndexingStrategy strategy, Predicate<Configuration> slice)
        throws IOException {
        forEachConfiguration(
            scenario,
            strategy,
            slice,
            documentGroups(scenario),
            DOCUMENT_SHAPES,
            (configuration, mapperService, indexWriter, docs) -> {
                final Query exists = mapperService.fieldType(FIELD).existsQuery(createSearchExecutionContext(mapperService));
                for (final boolean merged : new boolean[] { false, true }) {
                    if (merged) {
                        indexWriter.forceMerge(1);
                    }
                    try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
                        final IndexSearcher searcher = new IndexSearcher(reader);
                        assertEquals(
                            configuration + " merged=" + merged + " documents=" + docs,
                            searcher.search(exists, docs.size()).scoreDocs.length,
                            searcher.count(exists)
                        );
                    }
                }
            }
        );
    }

    protected final void assertSourceDropsNullElements(Scenario scenario, IndexingStrategy strategy) throws IOException {
        forEachConfiguration(
            scenario,
            strategy,
            ALL,
            documentGroups(scenario),
            DOCUMENT_SHAPES,
            (configuration, mapperService, indexWriter, docs) -> {
                assertSources(scenario, mapperService, indexWriter, docs, false);
            }
        );
    }

    protected final void assertSourceKeepsTheOrderOfTheRemainingValues(Scenario scenario, IndexingStrategy strategy) throws IOException {
        forEachConfiguration(
            scenario,
            strategy,
            ALL,
            orderedDocumentGroups(scenario.values()),
            List.of(NULL_ELEMENTS),
            (configuration, mapperService, indexWriter, docs) -> {
                assertSources(scenario, mapperService, indexWriter, docs, true);
            }
        );
    }

    /**
     * Force merges the index to one segment, which changes the segments of {@code indexWriter}, and returns the reconstructed
     * {@code _source} of the first {@code count} documents in index order.
     */
    protected final List<String> reconstructedSources(MapperService mapperService, IndexWriter indexWriter, int count) throws IOException {
        indexWriter.forceMerge(1);
        try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
            final List<String> sources = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                sources.add(syntheticSource(mapperService.documentMapper(), reader, i));
            }
            return sources;
        }
    }

    private void assertSources(
        Scenario scenario,
        MapperService mapperService,
        IndexWriter indexWriter,
        List<TestDocument> documents,
        boolean ordered
    ) throws IOException {
        final List<String> sources = reconstructedSources(mapperService, indexWriter, documents.size());
        final List<String> failures = new ArrayList<>();
        for (int i = 0; i < documents.size(); i++) {
            try {
                assertSourceHoldsOnly(scenario, scenario.expected(documents.get(i)), sources.get(i), ordered);
            } catch (AssertionError e) {
                failures.add("document=" + documents.get(i) + " source=" + sources.get(i) + " " + e.getMessage());
            }
        }
        if (failures.isEmpty() == false) {
            fail(failures.size() + " of " + documents.size() + " documents failed:\n" + String.join("\n", failures));
        }
    }

    // NOTE: with ordered set, a null the source still holds is left out of the comparison. Whether nulls are dropped is the null
    // contract and is checked by the unordered comparison, and the two must fail for their own reasons.
    private static void assertSourceHoldsOnly(Scenario scenario, List<Object> expected, String source, boolean ordered) throws IOException {
        final Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, source, false);
        if (expected.isEmpty()) {
            assertFalse("a field without a value is absent", parsed.containsKey(FIELD));
            return;
        }
        final Object actual = parsed.get(FIELD);
        final List<?> actualValues = actual instanceof List<?> list ? list : Collections.singletonList(actual);
        final List<String> actualCanonical = new ArrayList<>();
        for (final Object value : actualValues) {
            if (ordered && value == null) {
                continue;
            }
            actualCanonical.add(value == null ? "null" : scenario.canonical().apply(value));
        }
        final List<String> expectedCanonical = expected.stream().map(value -> scenario.canonical().apply(value)).toList();
        if (ordered) {
            assertEquals(expectedCanonical, actualCanonical);
            return;
        }
        final SortedSet<String> expectedDistinct = new TreeSet<>(expectedCanonical);
        if (expectedDistinct.size() == 1 && expected.size() == 1) {
            assertFalse("a single remaining value is a scalar", actual instanceof List);
        } else if (expectedDistinct.size() > 1) {
            assertTrue("several remaining values are an array", actual instanceof List);
        }
        assertEquals(expectedDistinct, new TreeSet<>(actualCanonical));
    }
}
