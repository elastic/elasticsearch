/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformInternalIndexIT extends TransformSingleNodeTestCase {

    private static final String CURRENT_INDEX = TransformInternalIndexConstants.LATEST_INDEX_NAME;
    private static final String OLD_INDEX = TransformInternalIndexConstants.INDEX_PATTERN + "001";

    @Override
    protected Settings nodeSettings() {
        // TODO Change this to run with security enabled
        // https://github.com/elastic/elasticsearch/issues/75940
        return Settings.builder().put(super.nodeSettings()).put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    public void testUpdateDeletesOldTransformConfig() throws Exception {

        // The mapping does not need to actually be the "OLD" mapping, we are testing that the old doc gets deleted, and the new one
        // created.
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field(TransformInternalIndex.DYNAMIC, "false");
            builder.startObject("properties");
            builder.startObject(TransformField.INDEX_DOC_TYPE.getPreferredName()).field("type", "keyword").endObject();
            TransformInternalIndex.addTransformsConfigMappings(builder);
            builder.endObject();
            builder.endObject();
            indicesAdmin().create(new CreateIndexRequest(OLD_INDEX).mapping(builder).origin(ClientHelper.TRANSFORM_ORIGIN)).actionGet();
        }
        String transformIndex = "transform-index-deletes-old";
        createSourceIndex(transformIndex);
        String transformId = "transform-update-deletes-old-transform-config";
        String config = Strings.format("""
            {
              "dest": {
                "index": "bar"
              },
              "source": {
                "index": "%s",
                "query": {
                  "match_all": {}
                }
              },
              "id": "%s",
              "doc_type": "data_frame_transform_config",
              "pivot": {
                "group_by": {
                  "reviewer": {
                    "terms": {
                      "field": "user_id"
                    }
                  }
                },
                "aggregations": {
                  "avg_rating": {
                    "avg": {
                      "field": "stars"
                    }
                  }
                }
              },
              "frequency": "1s"
            }""", transformIndex, transformId);
        IndexRequest indexRequest = new IndexRequest(OLD_INDEX).id(TransformConfig.documentId(transformId))
            .source(config, XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.getResult(), is(DocWriteResponse.Result.CREATED));

        GetTransformAction.Request getTransformRequest = new GetTransformAction.Request(transformId);
        GetTransformAction.Response getTransformResponse = client().execute(GetTransformAction.INSTANCE, getTransformRequest).actionGet();
        assertThat(getTransformResponse.getTransformConfigurations().get(0).getId(), equalTo(transformId));

        UpdateTransformAction.Request updateTransformActionRequest = new UpdateTransformAction.Request(
            new TransformConfigUpdate(null, null, null, null, "updated", null, null, null),
            transformId,
            false,
            AcknowledgedRequest.DEFAULT_ACK_TIMEOUT
        );
        UpdateTransformAction.Response updateTransformActionResponse = client().execute(
            UpdateTransformAction.INSTANCE,
            updateTransformActionRequest
        ).actionGet();
        assertThat(updateTransformActionResponse.getConfig().getId(), equalTo(transformId));
        assertThat(updateTransformActionResponse.getConfig().getDescription(), equalTo("updated"));

        // Old should now be gone
        {
            GetRequest getRequest = new GetRequest(OLD_INDEX, TransformConfig.documentId(transformId));
            GetResponse getResponse = client().get(getRequest).actionGet();
            assertThat(getResponse.isExists(), is(false));
        }

        // New should be here
        {
            GetRequest getRequest = new GetRequest(CURRENT_INDEX, TransformConfig.documentId(transformId));
            GetResponse getResponse = client().get(getRequest).actionGet();
            assertThat(getResponse.isExists(), is(true));
        }
    }

    private void createSourceIndex(String index) {
        indicesAdmin().create(new CreateIndexRequest(index)).actionGet();
    }
}
