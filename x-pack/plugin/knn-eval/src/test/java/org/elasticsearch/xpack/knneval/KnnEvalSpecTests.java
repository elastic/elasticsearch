/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.containsString;

public class KnnEvalSpecTests extends ESTestCase {

    static final NamedWriteableRegistry NAMED_WRITEABLE_REGISTRY = new NamedWriteableRegistry(List.of());

    static KnnEvalSettings createTestSettings() {
        return new KnnEvalSettings(
            randomBoolean() ? null : randomFloatBetween(0.0f, 100.0f, true),
            randomBoolean() ? null : randomIntBetween(50, 200),
            randomBoolean() ? null : randomFloatBetween(1.0f, 20.0f, true),
            false
        );
    }

    /** Covers both {@code query_vector} forms: a float array and an encoded (base64 or hex) string. */
    static KnnEvalQuery createTestQuery(String id) {
        if (randomBoolean()) {
            float[] vector = new float[randomIntBetween(1, 8)];
            for (int i = 0; i < vector.length; i++) {
                vector[i] = randomFloatBetween(-10.0f, 10.0f, true);
            }
            return new KnnEvalQuery(id, VectorData.fromFloats(vector));
        }
        // hex, not base64, so it survives a write to a transport version predating base64
        return new KnnEvalQuery(
            id,
            VectorData.fromStringVector(HexFormat.of().formatHex(randomByteArrayOfLength(randomIntBetween(4, 32))))
        );
    }

    static KnnEvalSpec createTestItem() {
        // num_candidates must be at least k; createTestSettings() draws [50, 200], so leave room for mutateTestItem's k + 1
        int k = randomIntBetween(1, 40);
        KnnEvalQuerySource querySource;
        if (randomBoolean()) {
            querySource = new KnnEvalQuerySource.DocsSource(
                new KnnEvalSample(randomIntBetween(1, KnnEvalSample.MAX_SAMPLE_SIZE), randomBoolean() ? null : randomInt())
            );
        } else {
            List<KnnEvalQuery> queries = new ArrayList<>();
            int numQueries = randomIntBetween(1, 5);
            for (int i = 0; i < numQueries; i++) {
                queries.add(createTestQuery("query_" + i));
            }
            querySource = new KnnEvalQuerySource.VectorsSource(queries);
        }
        List<KnnEvalSettings> candidates = new ArrayList<>();
        int numCandidates = randomIntBetween(1, 4);
        while (candidates.size() < numCandidates) {
            KnnEvalSettings candidate = createTestSettings();
            if (candidates.contains(candidate) == false) {
                candidates.add(candidate);
            }
        }
        KnnEvalSettings baseline = randomBoolean() ? new KnnEvalSettings(null, null, null, true) : createTestSettings();
        return new KnnEvalSpec(randomAlphaOfLengthBetween(1, 10), k, querySource, baseline, candidates);
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
        KnnEvalQuerySource querySource = original.getQuerySource();
        KnnEvalSettings baseline = original.getBaseline();
        List<KnnEvalSettings> candidates = new ArrayList<>(original.getKnnSettings());

        switch (randomIntBetween(0, 3)) {
            case 0 -> field = field + "_mutated";
            case 1 -> k = k + 1;
            case 2 -> {
                KnnEvalSettings candidate;
                do {
                    candidate = new KnnEvalSettings(randomFloatBetween(0.0f, 100.0f, true), null, null, false);
                } while (candidates.contains(candidate));
                candidates.add(candidate);
            }
            case 3 -> {
                if (querySource instanceof KnnEvalQuerySource.DocsSource ds) {
                    KnnEvalSample s = ds.sample();
                    int size = s.getSize() == KnnEvalSample.MAX_SAMPLE_SIZE ? s.getSize() - 1 : s.getSize() + 1;
                    querySource = new KnnEvalQuerySource.DocsSource(new KnnEvalSample(size, s.getSeed()));
                } else if (querySource instanceof KnnEvalQuerySource.VectorsSource vs) {
                    List<KnnEvalQuery> updated = new ArrayList<>(vs.vectors());
                    updated.add(createTestQuery("mutation"));
                    querySource = new KnnEvalQuerySource.VectorsSource(updated);
                }
            }
            default -> throw new AssertionError("unreachable");
        }
        return new KnnEvalSpec(field, k, querySource, baseline, candidates);
    }

    public void testQuerySourceIsRequired() {
        KnnEvalSettings knnSettings = new KnnEvalSettings(100.0f, null, null, false);
        List<KnnEvalSettings> candidates = List.of(new KnnEvalSettings(5.0f, null, null, false));

        Exception e = expectThrows(NullPointerException.class, () -> new KnnEvalSpec("emb", 10, null, knnSettings, candidates));
        assertThat(e.getMessage(), containsString("[query_source] must be provided"));
    }

    public void testVectorsAreCapped() {
        List<KnnEvalQuery> queries = new ArrayList<>(KnnEvalSpec.MAX_QUERIES + 1);
        for (int i = 0; i <= KnnEvalSpec.MAX_QUERIES; i++) {
            queries.add(new KnnEvalQuery("q" + i, VectorData.fromFloats(new float[] { i })));
        }
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new KnnEvalSpec(
                "emb",
                10,
                new KnnEvalQuerySource.VectorsSource(queries),
                new KnnEvalSettings(100.0f, null, null, false),
                List.of(new KnnEvalSettings(5.0f, null, null, false))
            )
        );
        assertThat(e.getMessage(), containsString("[vectors] must contain at most " + KnnEvalSpec.MAX_QUERIES + " entries"));
    }

    public void testInvalidValuesAreRejected() {
        KnnEvalSettings knnSettings = new KnnEvalSettings(100.0f, null, null, false);
        List<KnnEvalSettings> candidates = List.of(new KnnEvalSettings(5.0f, null, null, false));
        KnnEvalQuerySource docsSource = new KnnEvalQuerySource.DocsSource(new KnnEvalSample(10, null));

        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalSpec("emb", 0, docsSource, knnSettings, candidates)).getMessage(),
            containsString("[k] must be between 1 and 1000")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalSpec("emb", 10, docsSource, knnSettings, List.of())).getMessage(),
            containsString("[knn_settings] must contain between 1 and 32 entries")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalSpec("", 10, docsSource, knnSettings, candidates)).getMessage(),
            containsString("[field] must be a non-empty field name")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec("emb", 10, new KnnEvalQuerySource.VectorsSource(List.of()), knnSettings, candidates)
            ).getMessage(),
            containsString("[vectors] must not be empty")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec(
                    "emb",
                    10,
                    new KnnEvalQuerySource.VectorsSource(List.of(createTestQuery("q1"), createTestQuery("q1"))),
                    knnSettings,
                    candidates
                )
            ).getMessage(),
            containsString("duplicate query id [q1]")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec("emb", 10, docsSource, knnSettings, List.of(new KnnEvalSettings(5.0f, 9, null, false)))
            ).getMessage(),
            containsString("[num_candidates] cannot be less than [k]")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalSettings(null, null, 0.5f, false)).getMessage(),
            containsString("[rescore_vector.oversample] must be at least 1.0")
        );
        // 0 turns rescoring off in the mapping, but a quantized reference is useless
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalSettings(null, null, 0.0f, false)).getMessage(),
            containsString("[rescore_vector.oversample] must be at least 1.0")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalSettings(100.1f, null, null, false)).getMessage(),
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

    public void testResourceLimitsAndRedundantSettingsAreRejected() {
        KnnEvalSettings baseline = new KnnEvalSettings(100.0f, null, null, false);
        KnnEvalSettings candidate = new KnnEvalSettings(5.0f, null, null, false);
        KnnEvalQuerySource docsSource = new KnnEvalQuerySource.DocsSource(new KnnEvalSample(10, null));
        KnnEvalQuerySource vectorsSource = new KnnEvalQuerySource.VectorsSource(
            List.of(new KnnEvalQuery("q0", VectorData.fromFloats(new float[] { 0 })))
        );

        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalSpec("emb", 1_001, docsSource, baseline, List.of(candidate)))
                .getMessage(),
            containsString("[k] must be between 1 and 1000")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec("emb", 10, vectorsSource, baseline, List.of(new KnnEvalSettings(5.0f, 10_001, null, false)))
            ).getMessage(),
            containsString("[num_candidates] cannot exceed 10000 in")
        );
        KnnEvalSettings atLimit = new KnnEvalSettings(5.0f, 10_000, null, false);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalSpec("emb", 10, docsSource, baseline, List.of(atLimit)))
                .getMessage(),
            containsString("[num_candidates] cannot exceed 9999 with sampling")
        );
        assertEquals(
            10_000,
            (int) new KnnEvalSpec("emb", 10, vectorsSource, baseline, List.of(atLimit)).getKnnSettings().get(0).getNumCandidates()
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new KnnEvalSpec("emb", 10, docsSource, baseline, List.of(candidate, candidate))
            ).getMessage(),
            containsString("duplicate entry in [knn_settings]")
        );
        List<KnnEvalSettings> tooManyCandidates = new ArrayList<>();
        for (int i = 1; i <= KnnEvalSpec.MAX_KNN_SETTINGS + 1; i++) {
            tooManyCandidates.add(new KnnEvalSettings((float) i, null, null, false));
        }
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new KnnEvalSpec("emb", 10, docsSource, baseline, tooManyCandidates))
                .getMessage(),
            containsString("[knn_settings] must contain between 1 and 32 entries")
        );
    }

    public void testDefaults() throws IOException {
        String json = """
            {
              "field": "emb",
              "k": 10,
              "query_source": { "from": "docs", "size": 5 },
              "baseline": { "visit_percentage": 100 },
              "knn_settings": [ { "visit_percentage": 5 } ]
            }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            KnnEvalSpec spec = KnnEvalSpec.parse(parser);
            assertNull(spec.getQueries());
            assertEquals(5, spec.getSample().getSize());
            assertNull(spec.getSample().getSeed());
            assertEquals(100.0f, spec.getBaseline().getVisitPercentage(), 0.0f);
            assertNull(spec.getBaseline().getNumCandidates());
        }
    }

    public void testBaselineDefaultsToBoundedProxy() throws IOException {
        for (String baseline : List.of("", "\"baseline\": {},")) {
            String json = """
                {
                  "field": "emb",
                  "k": 10,
                  "query_source": { "from": "docs", "size": 5 },
                  %s
                  "knn_settings": [ { "visit_percentage": 5 } ]
                }""".replace("%s", baseline);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
                KnnEvalSpec spec = KnnEvalSpec.parse(parser);
                assertFalse(spec.getBaseline().isExact());
                assertEquals(20.0f, spec.getBaseline().getVisitPercentage(), 0.0f);
                assertEquals(100.0f, spec.getBaseline().getRescoreOversample(), 0.0f);
            }
        }
    }

    public void testQueryVectorAcceptsArraysAndEncodedStrings() throws IOException {
        String json = """
            {
              "field": "emb",
              "k": 5,
              "query_source": {
                "from": "vectors",
                "vectors": [
                  { "id": "array", "query_vector": [1.5, -2.5] },
                  { "id": "encoded", "query_vector": "P8AAAMAgAAA=" }
                ]
              },
              "baseline": { "visit_percentage": 100 },
              "knn_settings": [ { "visit_percentage": 5 } ]
            }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            KnnEvalSpec spec = KnnEvalSpec.parse(parser);
            assertEquals(VectorData.fromFloats(new float[] { 1.5f, -2.5f }), spec.getQueries().get(0).getQueryVector());
            // carried verbatim; only the mapper can decode it, knowing dims and element type
            assertEquals(VectorData.fromStringVector("P8AAAMAgAAA="), spec.getQueries().get(1).getQueryVector());
            assertTrue(spec.getQueries().get(1).getQueryVector().isStringVector());
        }
    }

}
