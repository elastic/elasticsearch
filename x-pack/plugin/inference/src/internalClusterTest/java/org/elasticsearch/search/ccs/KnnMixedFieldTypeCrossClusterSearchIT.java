/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.search.vectors.LookupQueryVectorBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper;
import org.elasticsearch.xpack.inference.queries.GenericQueryVectorBuilder;
import org.elasticsearch.xpack.inference.vectors.EmbeddingQueryVectorBuilder;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Parameterized CCS test covering all combinations of:
 * <ul>
 *   <li>local/remote field types: {@code dense_vector}, {@code semantic_text}, {@code semantic}</li>
 *   <li>query vector builders: {@code generic}, {@code text_embedding}, {@code embedding}, {@code lookup}</li>
 *   <li>{@code ccsMinimizeRoundtrips}: {@code true} / {@code false}</li>
 * </ul>
 *
 * <p>Both {@code text_embedding} and {@code embedding} QVBs (with null model/inference ID) are compatible with
 * any field that carries an inference endpoint ({@code semantic_text} or {@code semantic}), because the test mock
 * service returns vectors regardless of task type. They are incompatible only with {@code dense_vector} fields,
 * which have no inference endpoint at all. {@code generic} and {@code lookup} supply a pre-computed vector and
 * are always compatible.
 */
public class KnnMixedFieldTypeCrossClusterSearchIT extends AbstractSemanticCrossClusterSearchTestCase {

    private static final int DIMS = 256;
    private static final String TEXT_EMBEDDING_INFERENCE_ID = "mixed-text-embedding-inference-id";
    private static final String EMBEDDING_INFERENCE_ID = "mixed-embedding-inference-id";

    /** Dense-vector field used as the source for {@link LookupQueryVectorBuilder}. Always local. */
    private static final String LOOKUP_SOURCE_FIELD = "lookup-source";

    private static final Map<String, String> FIELDS_FOR_TYPES = Map.of("dense_vector", "dv", "semantic_text", "st", "semantic", "s");

    // QVB type constants
    private static final String QVB_GENERIC = "generic";
    private static final String QVB_TEXT_EMBEDDING = "text_embedding";
    private static final String QVB_EMBEDDING = "embedding";
    private static final String QVB_LOOKUP = "lookup";

    private final String localFieldType;
    private final String remoteFieldType;
    private final String qvbType;
    private final boolean ccsMinimizeRoundtrips;

    private boolean clustersConfigured = false;

    public KnnMixedFieldTypeCrossClusterSearchIT(
        @Name("localFieldType") String localFieldType,
        @Name("remoteFieldType") String remoteFieldType,
        @Name("qvbType") String qvbType,
        @Name("ccsMinimizeRoundtrips") boolean ccsMinimizeRoundtrips
    ) {
        this.localFieldType = localFieldType;
        this.remoteFieldType = remoteFieldType;
        this.qvbType = qvbType;
        this.ccsMinimizeRoundtrips = ccsMinimizeRoundtrips;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        boolean semanticEnabled = SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled();

        List<String> types = new ArrayList<>(List.of("dense_vector", "semantic_text"));
        if (semanticEnabled) {
            types.add("semantic");
        }
        List<String> qvbTypes = new ArrayList<>(List.of(QVB_GENERIC, QVB_TEXT_EMBEDDING, QVB_LOOKUP));
        if (semanticEnabled) {
            qvbTypes.add(QVB_EMBEDDING);
        }

        List<Object[]> params = new ArrayList<>();
        for (String local : types) {
            for (String remote : types) {
                for (String qvb : qvbTypes) {
                    for (boolean ccs : List.of(true, false)) {
                        params.add(new Object[] { local, remote, qvb, ccs });
                    }
                }
            }
        }
        return params;
    }

    @Override
    protected boolean reuseClusters() {
        return true;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (clustersConfigured == false) {
            configureClusters();
            clustersConfigured = true;
        }
    }

    public void testKnnQueryAcrossMixedFieldTypes() throws Exception {
        String field = fieldName(localFieldType, remoteFieldType);
        Consumer<SearchRequest> modifier = s -> s.setCcsMinimizeRoundtrips(ccsMinimizeRoundtrips);

        QueryVectorBuilder qvb = buildQvb();
        InferenceStringGroup embeddingInput = qvb instanceof EmbeddingQueryVectorBuilder eqvb ? eqvb.getInput() : null;
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder(field, qvb, 10, 100, 10f, null);

        boolean localCompatible = isCompatible(qvbType, localFieldType, embeddingInput);
        boolean remoteCompatible = isCompatible(qvbType, remoteFieldType, embeddingInput);

        if (localCompatible && remoteCompatible) {
            String expectedLocalAlias = getExpectedLocalClusterAlias(ccsMinimizeRoundtrips);
            assertSearchResponse(
                query,
                QUERY_INDICES,
                List.of(
                    new SearchResult(expectedLocalAlias, LOCAL_INDEX_NAME, docId(field)),
                    new SearchResult(REMOTE_CLUSTER, REMOTE_INDEX_NAME, docId(field))
                ),
                null,
                modifier
            );
        } else if (localCompatible && ccsMinimizeRoundtrips) {
            // Local succeeds; remote is skipped with the expected error
            assertSearchResponse(
                query,
                QUERY_INDICES,
                List.of(new SearchResult(LOCAL_CLUSTER, LOCAL_INDEX_NAME, docId(field))),
                new ClusterFailure(
                    SearchResponse.Cluster.Status.SKIPPED,
                    Set.of(new FailureCause(IllegalArgumentException.class, incompatibleError(qvbType, remoteFieldType, embeddingInput)))
                ),
                modifier
            );
        } else {
            // When local is incompatible, its failure propagates before remote is contacted.
            // When only remote is incompatible (with !ccsMinimizeRoundtrips), the remote error propagates.
            String failingFieldType = localCompatible ? remoteFieldType : localFieldType;
            assertSearchFailure(
                query,
                QUERY_INDICES,
                IllegalArgumentException.class,
                incompatibleError(qvbType, failingFieldType, embeddingInput),
                modifier
            );
        }
    }

    private QueryVectorBuilder buildQvb() {
        return switch (qvbType) {
            case QVB_GENERIC -> new GenericQueryVectorBuilder(
                generateDenseVectorFieldValue(DIMS, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f)
            );
            case QVB_TEXT_EMBEDDING -> new TextEmbeddingQueryVectorBuilder(null, "hello");
            case QVB_EMBEDDING -> new EmbeddingQueryVectorBuilder(null, randomEmbeddingInput(), null);
            case QVB_LOOKUP -> new LookupQueryVectorBuilder(docId(LOOKUP_SOURCE_FIELD), LOCAL_INDEX_NAME, LOOKUP_SOURCE_FIELD, null);
            default -> throw new IllegalArgumentException("unknown qvb type: " + qvbType);
        };
    }

    /**
     * Returns a randomly chosen {@link InferenceStringGroup} for use with {@link EmbeddingQueryVectorBuilder}:
     * <ul>
     *   <li>single text — compatible with both {@code text_embedding} and {@code embedding} endpoints</li>
     *   <li>single image (base64) — compatible only with {@code embedding} endpoints</li>
     *   <li>text + image (multi-input) — compatible only with {@code embedding} endpoints</li>
     * </ul>
     */
    private InferenceStringGroup randomEmbeddingInput() {
        String base64Image = "data:image/png;base64,"
            + Base64.getEncoder().encodeToString(randomAlphaOfLength(8).getBytes(StandardCharsets.UTF_8));
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new InferenceStringGroup(randomAlphaOfLength(10));
            case 1 -> new InferenceStringGroup(new InferenceString(DataType.IMAGE, base64Image));
            case 2 -> new InferenceStringGroup(
                List.of(new InferenceString(DataType.TEXT, randomAlphaOfLength(5)), new InferenceString(DataType.IMAGE, base64Image))
            );
            default -> throw new IllegalStateException("unexpected random value");
        };
    }

    /**
     * Returns true when the QVB can resolve an embedding for the given field type without an explicit model/inference ID.
     * <ul>
     *   <li>{@code generic} and {@code lookup} supply a pre-computed vector — always compatible.</li>
     *   <li>{@code text_embedding} requires an inference endpoint on the field ({@code semantic_text} or {@code semantic}).</li>
     *   <li>{@code embedding} also requires an inference endpoint, but additionally the input type must match what the endpoint
     *       supports: {@code embedding} (EMBEDDING) endpoints accept all input types; {@code text_embedding} (TEXT_EMBEDDING)
     *       endpoints only accept single plain-text inputs.</li>
     * </ul>
     */
    private static boolean isCompatible(String qvbType, String fieldType, @Nullable InferenceStringGroup embeddingInput) {
        return switch (qvbType) {
            case QVB_GENERIC, QVB_LOOKUP -> true;
            case QVB_TEXT_EMBEDDING -> fieldType.equals("semantic_text") || fieldType.equals("semantic");
            case QVB_EMBEDDING -> switch (fieldType) {
                // EMBEDDING endpoints accept all input types
                case "semantic" -> true;
                // TEXT_EMBEDDING endpoints only accept single plain-text inputs
                case "semantic_text" -> embeddingInput != null
                    && embeddingInput.containsNonTextEntry() == false
                    && embeddingInput.containsMultipleInferenceStrings() == false;
                // dense_vector has no inference endpoint
                default -> false;
            };
            default -> throw new IllegalArgumentException("unknown qvb type: " + qvbType);
        };
    }

    /**
     * Returns the expected error message substring when the QVB is incompatible with {@code failingFieldType}.
     */
    private static String incompatibleError(String qvbType, String failingFieldType, @Nullable InferenceStringGroup embeddingInput) {
        return switch (qvbType) {
            case QVB_TEXT_EMBEDDING -> "[model_id] must be specified";
            case QVB_EMBEDDING -> switch (failingFieldType) {
                case "dense_vector" -> "[inference_id] must be specified";
                case "semantic_text" -> embeddingInput != null && embeddingInput.containsNonTextEntry()
                    ? "Non-text input is not supported for [text_embedding] inference endpoints for inference_id ["
                        + TEXT_EMBEDDING_INFERENCE_ID
                        + "]"
                    : "Multiple text inputs are not supported for [text_embedding] inference endpoints for inference_id ["
                        + TEXT_EMBEDDING_INFERENCE_ID
                        + "]";
                default -> throw new IllegalArgumentException("unexpected failing field type: " + failingFieldType);
            };
            default -> throw new IllegalArgumentException("no error expected for qvb type: " + qvbType);
        };
    }

    private void configureClusters() throws Exception {
        List<String> types = new ArrayList<>(List.of("dense_vector", "semantic_text"));
        if (SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled()) {
            types.add("semantic");
        }

        Map<String, Object> localMappings = new HashMap<>();
        Map<String, Object> remoteMappings = new HashMap<>();
        Map<String, Map<String, Object>> localDocs = new HashMap<>();
        Map<String, Map<String, Object>> remoteDocs = new HashMap<>();

        for (String localType : types) {
            for (String remoteType : types) {
                String field = fieldName(localType, remoteType);
                localMappings.put(field, mapping(localType));
                remoteMappings.put(field, mapping(remoteType));
                localDocs.put(docId(field), Map.of(field, fieldValue(localType)));
                remoteDocs.put(docId(field), Map.of(field, fieldValue(remoteType)));
            }
        }

        // Dedicated lookup source: dense_vector on local only (LookupQueryVectorBuilder always uses the coordinator)
        localMappings.put(LOOKUP_SOURCE_FIELD, denseVectorMapping(DIMS));
        localDocs.put(
            docId(LOOKUP_SOURCE_FIELD),
            Map.of(LOOKUP_SOURCE_FIELD, generateDenseVectorFieldValue(DIMS, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f))
        );

        Map<String, MinimalServiceSettings> inferenceEndpoints = new HashMap<>(
            Map.of(
                TEXT_EMBEDDING_INFERENCE_ID,
                textEmbeddingServiceSettings(DIMS, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            )
        );
        if (SemanticFieldMapper.SEMANTIC_FIELD_FEATURE_FLAG.isEnabled()) {
            inferenceEndpoints.put(
                EMBEDDING_INFERENCE_ID,
                embeddingServiceSettings(DIMS, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.FLOAT)
            );
        }

        setupTwoClusters(
            new TestIndexInfo(LOCAL_INDEX_NAME, inferenceEndpoints, localMappings, localDocs),
            new TestIndexInfo(REMOTE_INDEX_NAME, inferenceEndpoints, remoteMappings, remoteDocs)
        );
    }

    private static String fieldName(String local, String remote) {
        return "param-" + FIELDS_FOR_TYPES.get(local) + "-" + FIELDS_FOR_TYPES.get(remote);
    }

    private static String docId(String field) {
        return field + "_doc";
    }

    private static Map<String, Object> mapping(String fieldType) {
        return switch (fieldType) {
            case "dense_vector" -> denseVectorMapping(DIMS);
            case "semantic_text" -> semanticTextMapping(TEXT_EMBEDDING_INFERENCE_ID);
            case "semantic" -> semanticFieldMapping(EMBEDDING_INFERENCE_ID);
            default -> throw new IllegalArgumentException("unknown field type: " + fieldType);
        };
    }

    private Object fieldValue(String fieldType) {
        return switch (fieldType) {
            case "dense_vector" -> generateDenseVectorFieldValue(DIMS, DenseVectorFieldMapper.ElementType.FLOAT, 1.0f);
            case "semantic_text", "semantic" -> "hello";
            default -> throw new IllegalArgumentException("unknown field type: " + fieldType);
        };
    }
}
