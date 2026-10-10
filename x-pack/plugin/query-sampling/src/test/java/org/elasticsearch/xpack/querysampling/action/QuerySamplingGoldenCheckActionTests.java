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
import org.elasticsearch.xpack.querysampling.storage.GoldenStaleness;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class QuerySamplingGoldenCheckActionTests extends ESTestCase {

    public void testRequestIsBoundedAndTheVersionIsNotNegative() {
        assertNull(new QuerySamplingGoldenCheckRequest(0, 1).validate());
        assertNull(new QuerySamplingGoldenCheckRequest(7, QuerySamplingGoldenCheckRequest.MAX_QUERIES).validate());
        assertNotNull(new QuerySamplingGoldenCheckRequest(0, 0).validate());
        assertNotNull(new QuerySamplingGoldenCheckRequest(0, QuerySamplingGoldenCheckRequest.MAX_QUERIES + 1).validate());
        assertNotNull(new QuerySamplingGoldenCheckRequest(-1, 10).validate());
        assertThat("both are told", new QuerySamplingGoldenCheckRequest(-1, 0).validate().validationErrors().size(), equalTo(2));
    }

    public void testResponseRendersWhatWasFound() throws IOException {
        XContentBuilder builder = new QuerySamplingGoldenCheckResponse(new GoldenStaleness.Result(4, 10, 6, 2, 1, 1)).toXContent(
            JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS
        );

        assertThat(Strings.toString(builder), equalTo("{\"version\":4,\"checked\":10,\"fresh\":6,\"stale\":2,\"unknown\":1,\"failed\":1}"));
    }

    public void testResponseHasNoVersionWhenThereIsNoneToCheck() throws IOException {
        XContentBuilder builder = new QuerySamplingGoldenCheckResponse(new GoldenStaleness.Result(0, 0, 0, 0, 0, 0)).toXContent(
            JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS
        );

        assertThat(
            Strings.toString(builder),
            equalTo("{\"version\":null,\"checked\":0,\"fresh\":0,\"stale\":0,\"unknown\":0,\"failed\":0}")
        );
    }
}
