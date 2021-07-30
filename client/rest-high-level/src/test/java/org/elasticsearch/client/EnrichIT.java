/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.enrich.DeletePolicyRequest;
import org.elasticsearch.client.enrich.ExecutePolicyRequest;
import org.elasticsearch.client.enrich.ExecutePolicyResponse;
import org.elasticsearch.client.enrich.GetPolicyRequest;
import org.elasticsearch.client.enrich.GetPolicyResponse;
import org.elasticsearch.client.enrich.PutPolicyRequest;
import org.elasticsearch.client.enrich.StatsRequest;
import org.elasticsearch.client.enrich.StatsResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class EnrichIT extends ESRestHighLevelClientTestCase {

    public void testCRUD() throws Exception {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my-index")
            .mapping(Map.of("properties", Map.of("enrich_key", Map.of("type", "keyword"))));
        highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);

        final EnrichClient enrichClient = highLevelClient().enrich();
        PutPolicyRequest putPolicyRequest =
            new PutPolicyRequest("my-policy", "match", List.of("my-index"), "enrich_key", List.of("enrich_value"));
        AcknowledgedResponse putPolicyResponse = execute(putPolicyRequest, enrichClient::putPolicy, enrichClient::putPolicyAsync);
        assertThat(putPolicyResponse.isAcknowledged(), is(true));

        GetPolicyRequest getPolicyRequest = randomBoolean() ? new GetPolicyRequest("my-policy") : new GetPolicyRequest();
        GetPolicyResponse getPolicyResponse = execute(getPolicyRequest, enrichClient::getPolicy, enrichClient::getPolicyAsync);
        assertThat(getPolicyResponse.getPolicies().size(), equalTo(1));
        assertThat(getPolicyResponse.getPolicies().get(0).getType(), equalTo(putPolicyRequest.getType()));
        assertThat(getPolicyResponse.getPolicies().get(0).getIndices(), equalTo(putPolicyRequest.getIndices()));
        assertThat(getPolicyResponse.getPolicies().get(0).getMatchField(), equalTo(putPolicyRequest.getMatchField()));
        assertThat(getPolicyResponse.getPolicies().get(0).getEnrichFields(), equalTo(putPolicyRequest.getEnrichFields()));

        StatsRequest statsRequest = new StatsRequest();
        StatsResponse statsResponse = execute(statsRequest, enrichClient::stats, enrichClient::statsAsync);
        assertThat(statsResponse.getExecutingPolicies().size(), equalTo(0));
        assertThat(statsResponse.getCoordinatorStats().size(), equalTo(1));
        assertThat(statsResponse.getCoordinatorStats().get(0).getNodeId(), notNullValue());
        assertThat(statsResponse.getCoordinatorStats().get(0).getQueueSize(), greaterThanOrEqualTo(0));
        assertThat(statsResponse.getCoordinatorStats().get(0).getRemoteRequestsCurrent(), greaterThanOrEqualTo(0));
        assertThat(statsResponse.getCoordinatorStats().get(0).getRemoteRequestsTotal(), greaterThanOrEqualTo(0L));
        assertThat(statsResponse.getCoordinatorStats().get(0).getExecutedSearchesTotal(), greaterThanOrEqualTo(0L));

        ExecutePolicyRequest executePolicyRequest = new ExecutePolicyRequest("my-policy");
        ExecutePolicyResponse executePolicyResponse =
            execute(executePolicyRequest, enrichClient::executePolicy, enrichClient::executePolicyAsync);
        assertThat(executePolicyResponse.getExecutionStatus().getPhase(), equalTo("COMPLETE"));

        DeletePolicyRequest deletePolicyRequest = new DeletePolicyRequest("my-policy");
        AcknowledgedResponse deletePolicyResponse =
            execute(deletePolicyRequest, enrichClient::deletePolicy, enrichClient::deletePolicyAsync);
        assertThat(deletePolicyResponse.isAcknowledged(), is(true));

        getPolicyRequest = new GetPolicyRequest();
        getPolicyResponse = execute(getPolicyRequest, enrichClient::getPolicy, enrichClient::getPolicyAsync);
        assertThat(getPolicyResponse.getPolicies().size(), equalTo(0));
    }

}
