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
import org.elasticsearch.xpack.querysampling.storage.GoldenUpdater;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class QuerySamplingGoldenUpdateActionTests extends ESTestCase {

    public void testRequestIsBoundedBothWays() {
        assertNull(new QuerySamplingGoldenUpdateRequest(1).validate());
        assertNull(new QuerySamplingGoldenUpdateRequest(QuerySamplingGoldenUpdateRequest.MAX_QUERIES).validate());
        assertNotNull(new QuerySamplingGoldenUpdateRequest(0).validate());
        assertNotNull(new QuerySamplingGoldenUpdateRequest(-5).validate());
        assertNotNull(new QuerySamplingGoldenUpdateRequest(QuerySamplingGoldenUpdateRequest.MAX_QUERIES + 1).validate());
    }

    public void testResponseRendersWhatWasDone() throws IOException {
        XContentBuilder builder = new QuerySamplingGoldenUpdateResponse(new GoldenUpdater.Result(4, 5, 6, 3, 2, 1, 1)).toXContent(
            JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS
        );

        assertThat(
            Strings.toString(builder),
            equalTo("{\"source_version\":4,\"version\":5,\"copied\":6,\"recomputed\":3,\"added\":2,\"dropped\":1,\"failed\":1}")
        );
    }

    public void testResponseHasNoVersionsWhenThereWasNothingToUpdateOrNothingWasMade() throws IOException {
        XContentBuilder nothingMade = new QuerySamplingGoldenUpdateResponse(new GoldenUpdater.Result(4, 0, 6, 0, 0, 0, 0)).toXContent(
            JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS
        );
        XContentBuilder none = new QuerySamplingGoldenUpdateResponse(new GoldenUpdater.Result(0, 0, 0, 0, 0, 0, 0)).toXContent(
            JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS
        );

        assertThat(
            Strings.toString(nothingMade),
            equalTo("{\"source_version\":4,\"version\":null,\"copied\":6,\"recomputed\":0,\"added\":0,\"dropped\":0,\"failed\":0}")
        );
        assertThat(
            Strings.toString(none),
            equalTo("{\"source_version\":null,\"version\":null,\"copied\":0,\"recomputed\":0,\"added\":0,\"dropped\":0,\"failed\":0}")
        );
    }
}
