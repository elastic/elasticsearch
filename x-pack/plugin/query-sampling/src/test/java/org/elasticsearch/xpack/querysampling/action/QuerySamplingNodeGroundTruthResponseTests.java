/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class QuerySamplingNodeGroundTruthResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        QuerySamplingNodeGroundTruthResponse response = new QuerySamplingNodeGroundTruthResponse(
            DiscoveryNodeUtils.create(randomAlphaOfLength(5)),
            randomIntBetween(0, 1000),
            randomIntBetween(0, 1000)
        );

        QuerySamplingNodeGroundTruthResponse copy = copyWriteable(response, writableRegistry(), QuerySamplingNodeGroundTruthResponse::new);

        assertThat(copy.getNode(), equalTo(response.getNode()));
        assertThat(copy.computed(), equalTo(response.computed()));
        assertThat(copy.failed(), equalTo(response.failed()));
    }

    public void testRendersWhatTheNodeDid() throws IOException {
        QuerySamplingNodeGroundTruthResponse response = new QuerySamplingNodeGroundTruthResponse(
            DiscoveryNodeUtils.builder("node-id").name("node-name").build(),
            3,
            1
        );
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        assertThat(Strings.toString(builder), equalTo("{\"node-id\":{\"name\":\"node-name\",\"computed\":3,\"failed\":1}}"));
    }

    public void testRequestNeedsAtLeastOneQuery() {
        assertNull(new QuerySamplingGroundTruthRequest(1).validate());
        assertNotNull(new QuerySamplingGroundTruthRequest(0).validate());
    }
}
