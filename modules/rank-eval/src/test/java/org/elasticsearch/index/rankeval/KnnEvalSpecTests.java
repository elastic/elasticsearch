/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.containsString;

public class KnnEvalSpecTests extends ESTestCase {

    /** The optional {@code filter} is a {@link QueryBuilder}, so parsing and wire round-trips need the query registries. */
    private static final SearchModule SEARCH_MODULE = new SearchModule(Settings.EMPTY, emptyList());

    static final NamedWriteableRegistry NAMED_WRITEABLE_REGISTRY = new NamedWriteableRegistry(SEARCH_MODULE.getNamedWriteables());

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(SEARCH_MODULE.getNamedXContents());
    }

    static KnnEvalKnobs createTestKnobs() {
        return new KnnEvalKnobs(
            randomBoolean() ? null : randomFloatBetween(0.0f, 100.0f, true),
            randomBoolean() ? null : randomIntBetween(50, 200),
            randomBoolean() ? null : randomFloatBetween(1.0f, 20.0f, true),
            false
        );
    }

    /** Covers both accepted {@code query_vector} forms: a plain float array and an encoded (base64 or hex) string. */
    static KnnEvalQuery createTestQuery(String id) {
        if (randomBoolean()) {
            float[] vector = new float[randomIntBetween(1, 8)];
            for (int i = 0; i < vector.length; i++) {
                vector[i] = randomFloatBetween(-10.0f, 10.0f, true);
            }
            return new KnnEvalQuery(id, VectorData.fromFloats(vector));
        }
        // hex rather than base64 so that the encoded form also survives a write to a transport version predating base64 support
        return new KnnEvalQuery(
            id,
            VectorData.fromStringVector(HexFormat.of().formatHex(randomByteArrayOfLength(randomIntBetween(4, 32))))
        );
    }

    static KnnEvalSpec createTestItem() {
        // num_candidates must be at least k, and createTestKnobs() draws it from [50, 200]. Leave room for mutateTestItem's k + 1.
        int k = randomIntBetween(1, 40);
        // a tolerance is only accepted alongside the flag that turns the value-based metrics on
        boolean includeFidelity = randomBoolean();
        boolean sampled = randomBoolean();
        List<KnnEvalQuery> queries = null;
        KnnEvalSample sample = null;
        if (sampled) {
            sample = new KnnEvalSample(randomIntBetween(1, KnnEvalSample.MAX_SAMPLE_SIZE), randomBoolean() ? null : randomInt());
        } else {
            queries = new ArrayList<>();
            int numQueries = randomIntBetween(1, 5);
            for (int i = 0; i < numQueries; i++) {
                queries.add(createTestQuery("query_" + i));
            }
        }
        List<KnnEvalKnobs> candidates = new ArrayList<>();
        int numCandidates = randomIntBetween(1, 4);
        for (int i = 0; i < numCandidates; i++) {
            candidates.add(createTestKnobs());
        }
        return new KnnEvalSpec(
            randomAlphaOfLengthBetween(1, 10),
            k,
            queries,
            sample,
            createTestKnobs(),
            candidates,
            randomBoolean(),
            randomBoolean() ? null : createTestFilter(),
            randomIntBetween(1, 200),
            randomIntBetween(1, 10),
            includeFidelity,
            includeFidelity && randomBoolean() ? randomDoubleBetween(0.0, 1.0, true) : null,
            randomBoolean()
        );
    }

    static QueryBuilder createTestFilter() {
        return randomBoolean()
            ? new TermQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))
            : new IdsQueryBuilder().addIds(randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(1, 5));
    }

    public void testXContentRoundtrip() throws IOException {
        KnnEvalSpec testItem = createTestItem();
        XContentBuilder shuffled = shuffleXContent(testItem.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(shuffled))) {
            KnnEvalSpec parsedItem = KnnEvalSpec.parse(parser);
            assertNotSame(testItem, parsedItem);
            assertEquals(testItem, parsedItem);
            assertEquals(testItem.hashCode(), parsedItem.hashCode());
        }
    }

    public void testSerialization() throws IOException {
        KnnEvalSpec original = createTestItem();
        KnnEvalSpec deserialized = copy(original);
        assertNotSame(original, deserialized);
        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(createTestItem(), KnnEvalSpecTests::copy, KnnEvalSpecTests::mutateTestItem);
    }

    private static KnnEvalSpec copy(KnnEvalSpec original) throws IOException {
        return copyWriteable(original, NAMED_WRITEABLE_REGISTRY, KnnEvalSpec::new);
    }

    static KnnEvalSpec mutateTestItem(KnnEvalSpec original) {
        String field = original.getField();
        int k = original.getK();
        List<KnnEvalQuery> queries = original.getQueries() == null ? null : new ArrayList<>(original.getQueries());
        KnnEvalSample sample = original.getSample();
        KnnEvalKnobs baseline = original.getBaseline();
        List<KnnEvalKnobs> candidates = new ArrayList<>(original.getKnnSettings());
        boolean includeDetails = original.isIncludeDetails();
        QueryBuilder filter = original.getFilter();
        int maxQueriesPerBatch = original.getMaxQueriesPerBatch();
        int maxConcurrentSearches = original.getMaxConcurrentSearches();
        boolean includeFidelity = original.isIncludeFidelity();
        Double valueTolerance = includeFidelity ? original.getValueTolerance() : null;
        boolean includeHistogram = original.isIncludeHistogram();

        switch (randomIntBetween(0, 9)) {
            case 0 -> field = field + "_mutated";
            case 1 -> k = k + 1;
            case 2 -> candidates.add(new KnnEvalKnobs(randomFloatBetween(0.0f, 100.0f, true), null, null, false));
            case 3 -> includeDetails = includeDetails == false;
            case 4 -> {
                if (queries == null) {
                    sample = new KnnEvalSample(sample.getSize() + 1, sample.getSeed());
                } else {
                    queries.add(createTestQuery("mutation"));
                }
            }
            case 5 -> filter = filter == null ? new TermQueryBuilder("mutation", "mutation") : null;
            case 6 -> maxQueriesPerBatch = maxQueriesPerBatch + 1;
            case 7 -> maxConcurrentSearches = maxConcurrentSearches + 1;
            case 8 -> {
                if (includeFidelity) {
                    valueTolerance = original.getValueTolerance() + 0.5;
                } else {
                    includeFidelity = true;
                }
            }
            case 9 -> includeHistogram = includeHistogram == false;
            default -> throw new AssertionError("unreachable");
        }
        return new KnnEvalSpec(
            field,
            k,
            queries,
            sample,
            baseline,
            candidates,
            includeDetails,
            filter,
            maxQueriesPerBatch,
            maxConcurrentSearches,
            includeFidelity,
            valueTolerance,
            includeHistogram
        );
    }

    public void testQueriesAndSampleAreMutuallyExclusive() {
        KnnEvalKnobs knobs = new KnnEvalKnobs(100.0f, null, null, false);
        List<KnnEvalKnobs> candidates = List.of(new KnnEvalKnobs(5.0f, null, null, false));
        List<KnnEvalQuery> queries = List.of(createTestQuery("q1"));
        KnnEvalSample sample = new KnnEvalSample(10, 42);

        Exception both = expectThrows(
            IllegalArgumentException.class,
            () -> new KnnEvalSpec("emb", 10, queries, sample, knobs, candidates, false, null, 50, 1, false, null, false)
        );
        assertThat(both.getMessage(), containsString("exactly one of [queries] and [sample] must be provided"));

        Exception neither = expectThrows(
            IllegalArgumentException.class,
            () -> new KnnEvalSpec("emb", 10, null, null, knobs, candidates, false, null, 50, 1, false, null, false)
        );
        assertThat(neither.getMessage(), containsString("exactly one of [queries] and [sample] must be provided"));
    }

    public void testInvalidValuesAreRejected() {
        KnnEvalKnobs knobs = new KnnEvalKnobs(100.0f, null, null, false);
        List<KnnEvalKnobs> candidates = List.of(new KnnEvalKnobs(5.0f, null, null, false));
        KnnEvalSample sample = new KnnEvalSample(10, null);

        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec("emb", 0, null, sample, knobs, candidates, false, null, 50, 1, false, null, false)
            ).getMessage(),
            containsString("[k] must be greater than 0")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec("emb", 10, null, sample, knobs, List.of(), false, null, 50, 1, false, null, false)
            ).getMessage(),
            containsString("[knn_settings] must not be empty")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec("", 10, null, sample, knobs, candidates, false, null, 50, 1, false, null, false)
            ).getMessage(),
            containsString("[field] must be a non-empty field name")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec("emb", 10, List.of(), null, knobs, candidates, false, null, 50, 1, false, null, false)
            ).getMessage(),
            containsString("[queries] must not be empty")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec(
                    "emb",
                    10,
                    List.of(createTestQuery("q1"), createTestQuery("q1")),
                    null,
                    knobs,
                    candidates,
                    false,
                    null,
                    50,
                    1,
                    false,
                    null,
                    false
                )
            ).getMessage(),
            containsString("duplicate query id [q1]")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec(
                    "emb",
                    10,
                    null,
                    sample,
                    knobs,
                    List.of(new KnnEvalKnobs(5.0f, 9, null, false)),
                    false,
                    null,
                    50,
                    1,
                    false,
                    null,
                    false
                )
            ).getMessage(),
            containsString("[num_candidates] cannot be less than [k]")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalKnobs(null, null, 0.5f, false)).getMessage(),
            containsString("[oversample] must be at least 1.0")
        );
        // 0 is what the mapping uses to turn rescoring off, but a reference run with quantized scores is not useful
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalKnobs(null, null, 0.0f, false)).getMessage(),
            containsString("[oversample] must be at least 1.0")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalKnobs(100.1f, null, null, false)).getMessage(),
            containsString("[visit_percentage] must be between 0.0 and 100.0")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalSample(0, null)).getMessage(),
            containsString("[size] must be between 1 and " + KnnEvalSample.MAX_SAMPLE_SIZE)
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalSample(KnnEvalSample.MAX_SAMPLE_SIZE + 1, null)).getMessage(),
            containsString("[size] must be between 1 and " + KnnEvalSample.MAX_SAMPLE_SIZE)
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalQuery("q1", VectorData.fromFloats(new float[0]))).getMessage(),
            containsString("[query_vector] must not be empty")
        );
    }

    public void testIncludeDetailsDefaultsToFalse() throws IOException {
        String json = """
            {
              "field": "emb",
              "k": 10,
              "sample": { "size": 5 },
              "baseline": { "visit_percentage": 100 },
              "knn_settings": [ { "visit_percentage": 5 } ]
            }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            KnnEvalSpec spec = KnnEvalSpec.parse(parser);
            assertFalse(spec.isIncludeDetails());
            assertNull(spec.getQueries());
            assertEquals(5, spec.getSample().getSize());
            assertNull(spec.getSample().getSeed());
            assertEquals(100.0f, spec.getBaseline().getVisitPercentage(), 0.0f);
            assertNull(spec.getBaseline().getNumCandidates());
            assertNull(spec.getFilter());
            assertEquals(50, spec.getMaxQueriesPerBatch());
            assertEquals(1, spec.getMaxConcurrentSearches());
            assertEquals(0.0, spec.getValueTolerance(), 0.0);
            assertFalse(spec.isIncludeFidelity());
            assertFalse(spec.isIncludeHistogram());
        }
    }

    public void testMaxQueriesPerBatchMustBePositive() {
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec(
                    "emb",
                    10,
                    null,
                    new KnnEvalSample(10, null),
                    new KnnEvalKnobs(100.0f, null, null, false),
                    List.of(new KnnEvalKnobs(5.0f, null, null, false)),
                    false,
                    null,
                    0,
                    1,
                    false,
                    null,
                    false
                )
            ).getMessage(),
            containsString("[max_queries_per_batch] must be greater than 0")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec(
                    "emb",
                    10,
                    null,
                    new KnnEvalSample(10, null),
                    new KnnEvalKnobs(100.0f, null, null, false),
                    List.of(new KnnEvalKnobs(5.0f, null, null, false)),
                    false,
                    null,
                    50,
                    0,
                    false,
                    null,
                    false
                )
            ).getMessage(),
            containsString("[max_concurrent_searches] must be greater than 0")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec(
                    "emb",
                    10,
                    null,
                    new KnnEvalSample(10, null),
                    new KnnEvalKnobs(100.0f, null, null, false),
                    List.of(new KnnEvalKnobs(5.0f, null, null, false)),
                    false,
                    null,
                    50,
                    1,
                    true,
                    -0.1,
                    false
                )
            ).getMessage(),
            containsString("[value_tolerance] must not be negative")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec(
                    "emb",
                    10,
                    null,
                    new KnnEvalSample(10, null),
                    new KnnEvalKnobs(100.0f, null, null, false),
                    List.of(new KnnEvalKnobs(5.0f, null, null, false)),
                    false,
                    null,
                    50,
                    1,
                    false,
                    0.05,
                    false
                )
            ).getMessage(),
            containsString("[value_tolerance] requires [include_fidelity] to be true")
        );
    }

    public void testQueryVectorAcceptsArraysAndEncodedStrings() throws IOException {
        String json = """
            {
              "field": "emb",
              "k": 5,
              "queries": [
                { "id": "array", "query_vector": [1.5, -2.5] },
                { "id": "encoded", "query_vector": "P8AAAMAgAAA=" }
              ],
              "baseline": { "visit_percentage": 100 },
              "knn_settings": [ { "visit_percentage": 5 } ]
            }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            KnnEvalSpec spec = KnnEvalSpec.parse(parser);
            assertEquals(VectorData.fromFloats(new float[] { 1.5f, -2.5f }), spec.getQueries().get(0).getQueryVector());
            // the encoded form is carried through verbatim; only the mapper can decode it, once dims and element type are known
            assertEquals(VectorData.fromStringVector("P8AAAMAgAAA="), spec.getQueries().get(1).getQueryVector());
            assertTrue(spec.getQueries().get(1).getQueryVector().isStringVector());
        }
    }

    public void testFilterIsParsedAsATopLevelQuery() throws IOException {
        String json = """
            {
              "field": "emb",
              "k": 5,
              "sample": { "size": 5 },
              "baseline": { "visit_percentage": 100 },
              "knn_settings": [ { "visit_percentage": 5 } ],
              "filter": { "ids": { "values": ["2", "3"] } }
            }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            KnnEvalSpec spec = KnnEvalSpec.parse(parser);
            assertEquals(new IdsQueryBuilder().addIds("2", "3"), spec.getFilter());
        }
    }
}
