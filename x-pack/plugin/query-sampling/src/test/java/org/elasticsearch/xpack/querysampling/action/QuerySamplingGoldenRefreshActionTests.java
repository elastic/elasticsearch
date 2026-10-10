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
import org.elasticsearch.xpack.querysampling.storage.GoldenRefresher;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class QuerySamplingGoldenRefreshActionTests extends ESTestCase {

    public void testRequestIsBoundedAndTheVersionIsNotNegative() {
        assertNull(new QuerySamplingGoldenRefreshRequest(0, 1).validate());
        assertNull(new QuerySamplingGoldenRefreshRequest(7, QuerySamplingGoldenRefreshRequest.MAX_QUERIES).validate());
        assertNotNull(new QuerySamplingGoldenRefreshRequest(0, 0).validate());
        assertNotNull(new QuerySamplingGoldenRefreshRequest(0, QuerySamplingGoldenRefreshRequest.MAX_QUERIES + 1).validate());
        assertNotNull(new QuerySamplingGoldenRefreshRequest(-1, 10).validate());
    }

    public void testResponseRendersWhatWasDone() throws IOException {
        XContentBuilder builder = new QuerySamplingGoldenRefreshResponse(new GoldenRefresher.Result(4, 5, 6, 3, 1)).toXContent(
            JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS
        );

        assertThat(Strings.toString(builder), equalTo("{\"source_version\":4,\"version\":5,\"copied\":6,\"recomputed\":3,\"failed\":1}"));
    }

    public void testResponseHasNoVersionsWhenThereWasNothingToRefreshOrNothingWasMade() throws IOException {
        XContentBuilder builder = new QuerySamplingGoldenRefreshResponse(new GoldenRefresher.Result(4, 0, 6, 0, 0)).toXContent(
            JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS
        );
        XContentBuilder none = new QuerySamplingGoldenRefreshResponse(new GoldenRefresher.Result(0, 0, 0, 0, 0)).toXContent(
            JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS
        );

        assertThat(
            Strings.toString(builder),
            equalTo("{\"source_version\":4,\"version\":null,\"copied\":6,\"recomputed\":0,\"failed\":0}")
        );
        assertThat(
            Strings.toString(none),
            equalTo("{\"source_version\":null,\"version\":null,\"copied\":0,\"recomputed\":0,\"failed\":0}")
        );
    }
}
