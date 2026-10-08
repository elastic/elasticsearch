/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class QuerySamplingGroundTruthActionTests extends ESTestCase {

    public void testRequestNeedsAtLeastOneQuery() {
        assertNull(new QuerySamplingGroundTruthRequest(1).validate());
        assertNotNull(new QuerySamplingGroundTruthRequest(0).validate());
        assertNotNull(new QuerySamplingGroundTruthRequest(-5).validate());
    }

    public void testResponseRendersWhatWasDone() throws IOException {
        XContentBuilder builder = new QuerySamplingGroundTruthResponse(3, 1).toXContent(
            JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS
        );

        assertThat(Strings.toString(builder), equalTo("{\"computed\":3,\"failed\":1}"));
    }
}
