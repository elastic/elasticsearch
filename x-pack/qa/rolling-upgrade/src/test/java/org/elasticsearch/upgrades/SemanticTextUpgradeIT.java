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
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class SemanticTextUpgradeIT extends AbstractUpgradeTestCase {
    private static final String INDEX_BASE_NAME = "semantic_text_test_index";
    private static final String SPARSE_FIELD = "sparse_field";
    private static final String DENSE_FIELD = "dense_field";

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

        indexDoc("doc_1", List.of("a test value", "with multiple test values"));
    }

    private void performIndexQueryHighlightOps() throws IOException {
        indexDoc("doc_2", List.of("another test value"));
        ObjectPath queryObjectPath = semanticQuery(SPARSE_FIELD, "test value", 3);
        assertQueryResponse(queryObjectPath, SPARSE_FIELD);
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

    private ObjectPath semanticQuery(String field, String query, Integer numOfHighlightFragments) throws IOException {
        // We can't perform a real semantic query because that requires performing inference, so instead we perform an equivalent nested
        // query
        List<WeightedToken> weightedTokens = Arrays.stream(query.split("\\s")).map(t -> new WeightedToken(t, 1.0f)).toList();
        SparseVectorQueryBuilder sparseVectorQueryBuilder = new SparseVectorQueryBuilder(
            SemanticTextField.getEmbeddingsFieldName(field),
            weightedTokens,
            null,
            null,
            null,
            null
        );
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(
            SemanticTextField.getChunksFieldName(field),
            sparseVectorQueryBuilder,
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

    @SuppressWarnings("unchecked")
    private static void assertQueryResponse(ObjectPath queryObjectPath, String field) throws IOException {
        final Map<String, List<String>> expectedHighlights = Map.of(
            "doc_1",
            List.of("a test value", "with multiple test values"),
            "doc_2",
            List.of("another test value")
        );

        assertThat(queryObjectPath.evaluate("hits.total.value"), equalTo(2));
        assertThat(queryObjectPath.evaluateArraySize("hits.hits"), equalTo(2));

        Set<String> docIds = new HashSet<>();
        List<Object> hits = queryObjectPath.evaluate("hits.hits");
        for (Object hit : hits) {
            assertThat(hit, instanceOf(Map.class));
            Map<String, Object> hitMap = (Map<String, Object>) hit;

            String id = (String) hitMap.get("_id");
            assertThat(id, notNullValue());
            docIds.add(id);

            List<String> expectedHighlight = expectedHighlights.get(id);
            assertThat(expectedHighlight, notNullValue());
            assertThat(((Map<String, Object>) hitMap.get("highlight")).get(field), equalTo(expectedHighlight));
        }

        assertThat(docIds, equalTo(Set.of("doc_1", "doc_2")));
    }
}
