/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.TransformDeprecations;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformOldTransformsIT extends TransformSingleNodeTestCase {

    private static final String OLD_INDEX = TransformInternalIndexConstants.INDEX_PATTERN + "001";

    @Override
    protected Settings nodeSettings() {
        // TODO Change this to run with security enabled
        // https://github.com/elastic/elasticsearch/issues/75940
        return Settings.builder().put(super.nodeSettings()).put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
    }

    /**
     * Create an old transform and check that it can not be started, but updated and than started
     */
    public void testStopThrowsForDeprecatedTransformConfig() throws Exception {

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
        String transformIndex = "transform-index";
        createSourceIndex(transformIndex);
        String transformId = "transform-throws-for-old-config";
        Version transformVersion = VersionUtils.randomVersionBetween(
            random(),
            Version.V_7_2_0,
            VersionUtils.getPreviousVersion(TransformDeprecations.MIN_TRANSFORM_VERSION)
        );
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
              "frequency": "1s",
              "version": "%s"
            }""", transformIndex, transformId, transformVersion);
        IndexRequest indexRequest = new IndexRequest(OLD_INDEX).id(TransformConfig.documentId(transformId))
            .source(config, XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        IndexResponse indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.getResult(), is(DocWriteResponse.Result.CREATED));

        GetTransformAction.Request getTransformRequest = new GetTransformAction.Request(transformId);
        GetTransformAction.Response getTransformResponse = client().execute(GetTransformAction.INSTANCE, getTransformRequest).actionGet();
        assertThat(getTransformResponse.getTransformConfigurations().get(0).getId(), equalTo(transformId));
        assertThat(getTransformResponse.getTransformConfigurations().get(0).getVersion(), equalTo(transformVersion));

        StartTransformAction.Request startTransformRequest = new StartTransformAction.Request(
            transformId,
            null,
            AcknowledgedRequest.DEFAULT_ACK_TIMEOUT
        );

        ValidationException validationException = expectThrows(
            ValidationException.class,
            () -> client().execute(StartTransformAction.INSTANCE, startTransformRequest).actionGet()
        );

        assertThat(validationException.getMessage(), containsString("Transform configuration is too old"));

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

        StartTransformAction.Response startTransformActionResponse = client().execute(StartTransformAction.INSTANCE, startTransformRequest)
            .actionGet();

        assertTrue(startTransformActionResponse.isAcknowledged());

        StopTransformAction.Response stopTransformActionResponse = client().execute(
            StopTransformAction.INSTANCE,
            new StopTransformAction.Request(transformId, true, false, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT, false, false)
        ).actionGet();
        assertTrue(stopTransformActionResponse.isAcknowledged());
    }

    private void createSourceIndex(String index) {
        indicesAdmin().create(new CreateIndexRequest(index)).actionGet();
    }
}
