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
import org.elasticsearch.inference.TaskType;
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
import java.util.List;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapperTests.addSemanticTextInferenceResults;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticText;
import static org.hamcrest.CoreMatchers.equalTo;

public class SemanticTextUpgradeIT extends AbstractUpgradeTestCase {
    private static final String INDEX_BASE_NAME = "semantic_text_test_index";
    private static final String SEMANTIC_TEXT_FIELD = "semantic_field";

    private static Model SPARSE_MODEL;

    private final boolean useLegacyFormat;

    @BeforeClass
    public static void beforeClass() {
        SPARSE_MODEL = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
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
        final String mapping = """
            {
              "properties": {
                "%s": {
                  "type": "semantic_text",
                  "inference_id": "%s"
                }
              }
            }
            """.formatted(SEMANTIC_TEXT_FIELD, SPARSE_MODEL.getInferenceEntityId());

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
        ObjectPath queryObjectPath = semanticQuery("test value");
        assertThat(queryObjectPath.evaluate("hits.total.value"), equalTo(2));
    }

    private String getIndexName() {
        return INDEX_BASE_NAME + (useLegacyFormat ? "_legacy" : "_new");
    }

    private void indexDoc(String id, List<String> semanticTextFieldValue) throws IOException {
        final String indexName = getIndexName();
        final SemanticTextField semanticTextField = randomSemanticText(
            useLegacyFormat,
            SEMANTIC_TEXT_FIELD,
            SPARSE_MODEL,
            null,
            semanticTextFieldValue,
            XContentType.JSON
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        if (useLegacyFormat == false) {
            builder.field(semanticTextField.fieldName(), semanticTextFieldValue);
        }
        addSemanticTextInferenceResults(useLegacyFormat, builder, List.of(semanticTextField));
        builder.endObject();

        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder().addParameter("refresh", "true").build();
        Request request = new Request("POST", indexName + "/_doc/" + id);
        request.setJsonEntity(Strings.toString(builder));
        request.setOptions(requestOptions);

        Response response = client().performRequest(request);
        assertOK(response);
    }

    private ObjectPath semanticQuery(String query) throws IOException {
        // We can't perform a real semantic query because that requires performing inference, so instead we perform an equivalent nested
        // query
        List<WeightedToken> weightedTokens = Arrays.stream(query.split("\\s")).map(t -> new WeightedToken(t, 1.0f)).toList();
        SparseVectorQueryBuilder sparseVectorQueryBuilder = new SparseVectorQueryBuilder(
            SemanticTextField.getEmbeddingsFieldName(SEMANTIC_TEXT_FIELD),
            weightedTokens,
            null,
            null,
            null,
            null
        );
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(
            SemanticTextField.getChunksFieldName(SEMANTIC_TEXT_FIELD),
            sparseVectorQueryBuilder,
            ScoreMode.Max
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("query", nestedQueryBuilder);
        builder.endObject();

        Request request = new Request("GET", getIndexName() + "/_search");
        request.setJsonEntity(Strings.toString(builder));

        Response response = client().performRequest(request);
        return assertOKAndCreateObjectPath(response);
    }
}
