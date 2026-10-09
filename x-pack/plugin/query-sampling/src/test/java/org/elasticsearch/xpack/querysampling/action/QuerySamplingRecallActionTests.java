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
import org.elasticsearch.xpack.querysampling.estimate.RecallEstimate;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class QuerySamplingRecallActionTests extends ESTestCase {

    public void testRequestMaxIsBounded() {
        assertNull(new QuerySamplingRecallRequest(1, false).validate());
        assertNull(new QuerySamplingRecallRequest(QuerySamplingRecallRequest.MAX_SAMPLES, true).validate());
        assertNotNull(new QuerySamplingRecallRequest(0, false).validate());
        assertNotNull(new QuerySamplingRecallRequest(QuerySamplingRecallRequest.MAX_SAMPLES + 1, false).validate());
    }

    public void testResponseRendersTheEstimate() throws IOException {
        QuerySamplingRecallResponse response = new QuerySamplingRecallResponse(
            new RecallEstimate(5, 4, 0.9, 3.5, 0.8, 4.0, List.of(), List.of()),
            null
        );

        assertThat(
            render(response),
            equalTo(
                "{\"records\":5,\"records_with_ground_truth\":4,\"traffic_weighted_recall\":0.9,\"traffic_effective_size\":3.5,"
                    + "\"unique_query_recall\":0.8,\"unique_query_effective_size\":4.0,\"by_hardness\":[],\"by_cluster\":[]}"
            )
        );
    }

    public void testResponseRendersWhatIsUnknownAsNull() throws IOException {
        QuerySamplingRecallResponse response = new QuerySamplingRecallResponse(
            new RecallEstimate(0, 0, null, 0, null, 0, List.of(), List.of()),
            List.of()
        );

        assertThat(
            render(response),
            equalTo(
                "{\"records\":0,\"records_with_ground_truth\":0,\"traffic_weighted_recall\":null,\"traffic_effective_size\":0.0,"
                    + "\"unique_query_recall\":null,\"unique_query_effective_size\":0.0,"
                    + "\"by_hardness\":[],\"by_cluster\":[],\"samples\":[]}"
            )
        );
    }

    public void testResponseRendersTheEstimatesOfTheStrata() throws IOException {
        RecallEstimate estimate = new RecallEstimate(
            2,
            2,
            0.9,
            2.0,
            0.8,
            2.0,
            List.of(new RecallEstimate.GroupEstimate("hard", 1, 0.5, 1.0, null, 0.0)),
            List.of(new RecallEstimate.GroupEstimate("vec/2#3", 1, 1.0, 1.0, 1.0, 1.0))
        );

        assertThat(
            render(new QuerySamplingRecallResponse(estimate, null)),
            equalTo(
                "{\"records\":2,\"records_with_ground_truth\":2,\"traffic_weighted_recall\":0.9,\"traffic_effective_size\":2.0,"
                    + "\"unique_query_recall\":0.8,\"unique_query_effective_size\":2.0,"
                    + "\"by_hardness\":[{\"hardness\":\"hard\",\"records_with_ground_truth\":1,\"traffic_weighted_recall\":0.5,"
                    + "\"traffic_effective_size\":1.0,\"unique_query_recall\":null,\"unique_query_effective_size\":0.0}],"
                    + "\"by_cluster\":[{\"cluster\":\"vec/2#3\",\"records_with_ground_truth\":1,\"traffic_weighted_recall\":1.0,"
                    + "\"traffic_effective_size\":1.0,\"unique_query_recall\":1.0,\"unique_query_effective_size\":1.0}]}"
            )
        );
    }

    public void testResponseRendersWhatEachQueryContributed() throws IOException {
        QuerySamplingRecallResponse response = new QuerySamplingRecallResponse(
            new RecallEstimate(1, 1, 1.0, 1, 1.0, 1, List.of(), List.of()),
            List.of(new QuerySamplingRecallResponse.Sample("q7", 3, 30.0, 0.5, 0.25, 0.75))
        );

        assertThat(
            render(response),
            equalTo(
                "{\"records\":1,\"records_with_ground_truth\":1,\"traffic_weighted_recall\":1.0,\"traffic_effective_size\":1.0,"
                    + "\"unique_query_recall\":1.0,\"unique_query_effective_size\":1.0,"
                    + "\"by_hardness\":[],\"by_cluster\":[],\"samples\":[{\"label\":\"q7\",\"multiplicity\":3,"
                    + "\"weighted_multiplicity\":30.0,\"inclusion_probability\":0.5,\"seen_probability\":0.25,\"recall\":0.75}]}"
            )
        );
    }

    private static String render(QuerySamplingRecallResponse response) throws IOException {
        XContentBuilder builder = response.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
        return Strings.toString(builder);
    }
}
