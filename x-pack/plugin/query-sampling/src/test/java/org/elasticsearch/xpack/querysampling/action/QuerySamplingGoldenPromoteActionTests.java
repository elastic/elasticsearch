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

public class QuerySamplingGoldenPromoteActionTests extends ESTestCase {

    public void testRequestIsBoundedBothWays() {
        assertNull(new QuerySamplingGoldenPromoteRequest(1).validate());
        assertNull(new QuerySamplingGoldenPromoteRequest(QuerySamplingGoldenPromoteRequest.MAX_QUERIES).validate());
        assertNotNull(new QuerySamplingGoldenPromoteRequest(0).validate());
        assertNotNull(new QuerySamplingGoldenPromoteRequest(-5).validate());
        assertNotNull(new QuerySamplingGoldenPromoteRequest(QuerySamplingGoldenPromoteRequest.MAX_QUERIES + 1).validate());
    }

    public void testResponseRendersWhatWasDone() throws IOException {
        XContentBuilder builder = new QuerySamplingGoldenPromoteResponse(4L, 3, 1).toXContent(
            JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS
        );

        assertThat(Strings.toString(builder), equalTo("{\"version\":4,\"promoted\":3,\"failed\":1}"));
    }

    public void testResponseHasNoVersionWhenThereWasNothingToPromote() throws IOException {
        XContentBuilder builder = new QuerySamplingGoldenPromoteResponse(null, 0, 0).toXContent(
            JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS
        );

        assertThat(Strings.toString(builder), equalTo("{\"version\":null,\"promoted\":0,\"failed\":0}"));
    }
}
