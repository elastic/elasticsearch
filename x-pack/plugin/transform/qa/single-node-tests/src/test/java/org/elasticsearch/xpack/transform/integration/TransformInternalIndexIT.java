/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.transform.GetTransformRequest;
import org.elasticsearch.client.transform.GetTransformResponse;
import org.elasticsearch.client.transform.UpdateTransformRequest;
import org.elasticsearch.client.transform.UpdateTransformResponse;
import org.elasticsearch.client.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

import static org.elasticsearch.xpack.transform.persistence.TransformInternalIndex.addTransformsConfigMappings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;


public class TransformInternalIndexIT extends ESRestTestCase {


    private static final String CURRENT_INDEX = TransformInternalIndexConstants.LATEST_INDEX_NAME;
    private static final String OLD_INDEX = TransformInternalIndexConstants.INDEX_PATTERN + "001";


    public void testUpdateDeletesOldTransformConfig() throws Exception {
        TestRestHighLevelClient client = new TestRestHighLevelClient();
        // The mapping does not need to actually be the "OLD" mapping, we are testing that the old doc gets deleted, and the new one
        // created.
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.startObject("properties");
            builder.startObject(TransformField.INDEX_DOC_TYPE.getPreferredName()).field("type", "keyword").endObject();
            addTransformsConfigMappings(builder);
            builder.endObject();
            builder.endObject();
            client.indices().create(new CreateIndexRequest(OLD_INDEX).mapping(builder), RequestOptions.DEFAULT);
        }
        String transformIndex = "transform-index-deletes-old";
        createSourceIndex(transformIndex);
        String transformId = "transform-update-deletes-old-transform-config";
        String config = "{\"dest\": {\"index\":\"bar\"},"
            + " \"source\": {\"index\":\"" + transformIndex + "\", \"query\": {\"match_all\":{}}},"
            + " \"id\": \""+transformId+"\","
            + " \"doc_type\": \"data_frame_transform_config\","
            + " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } } } },"
            + "\"frequency\":\"1s\""
            + "}";
        client.index(new IndexRequest(OLD_INDEX)
                .id(TransformConfig.documentId(transformId))
                .source(config, XContentType.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
            RequestOptions.DEFAULT);
        GetResponse getResponse = client.get(new GetRequest(OLD_INDEX, TransformConfig.documentId(transformId)),
            RequestOptions.DEFAULT);
        assertThat(getResponse.isExists(), is(true));

        GetTransformResponse response = client.transform()
            .getTransform(new GetTransformRequest(transformId), RequestOptions.DEFAULT);
        assertThat(response.getTransformConfigurations().get(0).getId(), equalTo(transformId));

        UpdateTransformResponse updated = client.transform().updateTransform(
            new UpdateTransformRequest(TransformConfigUpdate.builder().setDescription("updated").build(), transformId),
            RequestOptions.DEFAULT);

        assertThat(updated.getTransformConfiguration().getId(), equalTo(transformId));
        assertThat(updated.getTransformConfiguration().getDescription(), equalTo("updated"));

        // Old should now be gone
        getResponse = client.get(new GetRequest(OLD_INDEX, TransformConfig.documentId(transformId)), RequestOptions.DEFAULT);
        assertThat(getResponse.isExists(), is(false));

        // New should be here
        getResponse = client.get(new GetRequest(CURRENT_INDEX, TransformConfig.documentId(transformId)),
            RequestOptions.DEFAULT);
        assertThat(getResponse.isExists(), is(true));
    }


    @Override
    protected Settings restClientSettings() {
        final String token = "Basic " +
            Base64.getEncoder().encodeToString(("x_pack_rest_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    private void createSourceIndex(String index) throws IOException {
        TestRestHighLevelClient client = new TestRestHighLevelClient();
        client.indices().create(new CreateIndexRequest(index), RequestOptions.DEFAULT);
    }

    private class TestRestHighLevelClient extends RestHighLevelClient {
        TestRestHighLevelClient() {
            super(client(), restClient -> {},
                new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        }
    }
}
