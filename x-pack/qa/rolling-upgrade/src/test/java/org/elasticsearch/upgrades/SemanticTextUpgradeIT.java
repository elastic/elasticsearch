/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapperTests.addSemanticTextInferenceResults;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticText;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

public class SemanticTextUpgradeIT extends AbstractUpgradeTestCase {
    private static final String INDEX_BASE_NAME = "semantic_text_test_index";
    private static final String SPARSE_FIELD = "sparse_field";
    private static final String DENSE_FIELD = "dense_field";

    private static final String DOC_1_ID = "doc_1";
    private static final String DOC_2_ID = "doc_2";
    private static final Map<String, List<String>> DOC_VALUES = Map.of(
        DOC_1_ID,
        List.of("a test value", "with multiple test values"),
        DOC_2_ID,
        List.of("another test value")
    );

    private static Model SPARSE_MODEL;
    private static Model DENSE_MODEL;

    private final boolean useLegacyFormat;

    @BeforeClass
    public static void beforeClass() {
        SPARSE_MODEL = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
        // Exclude dot product because we are not producing unit length vectors
        DENSE_MODEL = TestModel.createRandomInstance(TaskType.TEXT_EMBEDDING, List.of(SimilarityMeasure.DOT_PRODUCT));
    }

    public SemanticTextUpgradeIT(boolean useLegacyFormat) {
        this.useLegacyFormat = useLegacyFormat;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    public void testSemanticTextOperations() throws Exception {
        switch (CLUSTER_TYPE) {
            case OLD -> createAndPopulateIndex();
            case MIXED, UPGRADED -> performIndexQueryHighlightOps();
            default -> throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private void createAndPopulateIndex() throws IOException {
        final String indexName = getIndexName();
        final String mapping = Strings.format("""
            {
              "properties": {
                "%s": {
                  "type": "semantic_text",
                  "inference_id": "%s"
                },
                "%s": {
                  "type": "semantic_text",
                  "inference_id": "%s"
                }
              }
            }
            """, SPARSE_FIELD, SPARSE_MODEL.getInferenceEntityId(), DENSE_FIELD, DENSE_MODEL.getInferenceEntityId());

        CreateIndexResponse response = createIndex(
            indexName,
            Settings.builder().put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat).build(),
            mapping
        );
        assertThat(response.isAcknowledged(), equalTo(true));

        indexDoc(DOC_1_ID, DOC_VALUES.get(DOC_1_ID));
    }

    private void performIndexQueryHighlightOps() throws IOException {
        indexDoc(DOC_2_ID, DOC_VALUES.get(DOC_2_ID));

        ObjectPath sparseQueryObjectPath = semanticQuery(SPARSE_FIELD, SPARSE_MODEL, "test value", 3);
        assertQueryResponseWithHighlights(sparseQueryObjectPath, SPARSE_FIELD);

        ObjectPath denseQueryObjectPath = semanticQuery(DENSE_FIELD, DENSE_MODEL, "test value", 3);
        assertQueryResponseWithHighlights(denseQueryObjectPath, DENSE_FIELD);
    }

    private String getIndexName() {
        return INDEX_BASE_NAME + (useLegacyFormat ? "_legacy" : "_new");
    }

    private void indexDoc(String id, List<String> semanticTextFieldValue) throws IOException {
        final String indexName = getIndexName();
        final SemanticTextField sparseFieldValue = randomSemanticText(
            useLegacyFormat,
            SPARSE_FIELD,
            SPARSE_MODEL,
            null,
            semanticTextFieldValue,
            XContentType.JSON
        );
        final SemanticTextField denseFieldValue = randomSemanticText(
            useLegacyFormat,
            DENSE_FIELD,
            DENSE_MODEL,
            null,
            semanticTextFieldValue,
            XContentType.JSON
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        if (useLegacyFormat == false) {
            builder.field(sparseFieldValue.fieldName(), semanticTextFieldValue);
            builder.field(denseFieldValue.fieldName(), semanticTextFieldValue);
        }
        addSemanticTextInferenceResults(useLegacyFormat, builder, List.of(sparseFieldValue, denseFieldValue));
        builder.endObject();

        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder().addParameter("refresh", "true").build();
        Request request = new Request("POST", indexName + "/_doc/" + id);
        request.setJsonEntity(Strings.toString(builder));
        request.setOptions(requestOptions);

        Response response = client().performRequest(request);
        assertOK(response);
    }

    private ObjectPath semanticQuery(String field, Model fieldModel, String query, Integer numOfHighlightFragments) throws IOException {
        // We can't perform a real semantic query because that requires performing inference, so instead we perform an equivalent nested
        // query
        final String embeddingsFieldName = SemanticTextField.getEmbeddingsFieldName(field);
        final QueryBuilder innerQueryBuilder = switch (fieldModel.getTaskType()) {
            case SPARSE_EMBEDDING -> {
                List<WeightedToken> weightedTokens = Arrays.stream(query.split("\\s")).map(t -> new WeightedToken(t, 1.0f)).toList();
                yield new SparseVectorQueryBuilder(embeddingsFieldName, weightedTokens, null, null, null, null);
            }
            case TEXT_EMBEDDING -> {
                DenseVectorFieldMapper.ElementType elementType = fieldModel.getServiceSettings().elementType();
                int embeddingLength = DenseVectorFieldMapperTestUtils.getEmbeddingLength(
                    elementType,
                    fieldModel.getServiceSettings().dimensions()
                );

                // Create a query vector with a value of 1 for each dimension, which will effectively act as a pass-through for the document
                // vector
                float[] queryVector = new float[embeddingLength];
                if (elementType == DenseVectorFieldMapper.ElementType.BIT) {
                    Arrays.fill(queryVector, -128.0f);
                } else {
                    Arrays.fill(queryVector, 1.0f);
                }

                yield new KnnVectorQueryBuilder(embeddingsFieldName, queryVector, DOC_VALUES.size(), null, null, null);
            }
            default -> throw new UnsupportedOperationException("Unhandled task type [" + fieldModel.getTaskType() + "]");
        };

        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(
            SemanticTextField.getChunksFieldName(field),
            innerQueryBuilder,
            ScoreMode.Max
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("query", nestedQueryBuilder);
        if (numOfHighlightFragments != null) {
            HighlightBuilder.Field highlightField = new HighlightBuilder.Field(field);
            highlightField.numOfFragments(numOfHighlightFragments);

            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightBuilder.field(highlightField);

            builder.field("highlight", highlightBuilder);
        }
        builder.endObject();

        Request request = new Request("GET", getIndexName() + "/_search");
        request.setJsonEntity(Strings.toString(builder));

        Response response = client().performRequest(request);
        return assertOKAndCreateObjectPath(response);
    }

    private static void assertQueryResponseWithHighlights(ObjectPath queryObjectPath, String field) throws IOException {
        assertThat(queryObjectPath.evaluate("hits.total.value"), equalTo(2));
        assertThat(queryObjectPath.evaluateArraySize("hits.hits"), equalTo(2));

        Set<String> docIds = new HashSet<>();
        List<Map<String, Object>> hits = queryObjectPath.evaluate("hits.hits");
        for (Map<String, Object> hit : hits) {
            String id = ObjectPath.evaluate(hit, "_id");
            assertThat(id, notNullValue());
            docIds.add(id);

            List<String> expectedHighlight = DOC_VALUES.get(id);
            assertThat(expectedHighlight, notNullValue());
            assertThat(ObjectPath.evaluate(hit, "highlight." + field), equalTo(expectedHighlight));
        }

        assertThat(docIds, equalTo(Set.of(DOC_1_ID, DOC_2_ID)));
    }
}
