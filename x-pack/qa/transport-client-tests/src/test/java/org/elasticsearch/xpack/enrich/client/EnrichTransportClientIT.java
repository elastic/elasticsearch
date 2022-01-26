/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.client;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.ESXPackSmokeClientTestCase;
import org.elasticsearch.xpack.core.XPackClient;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.client.EnrichClient;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class EnrichTransportClientIT extends ESXPackSmokeClientTestCase {

    private static void assertEqualPolicies(EnrichPolicy expectedInstance, EnrichPolicy newInstance) {
        assertThat(newInstance.getType(), equalTo(expectedInstance.getType()));
        if (newInstance.getQuery() != null) {
            // testFromXContent, always shuffles the xcontent and then byte wise the query is different, so we check the parsed version:
            assertThat(newInstance.getQuery().getQueryAsMap(), equalTo(expectedInstance.getQuery().getQueryAsMap()));
        } else {
            assertThat(expectedInstance.getQuery(), nullValue());
        }
        assertThat(newInstance.getIndices(), equalTo(expectedInstance.getIndices()));
        assertThat(newInstance.getMatchField(), equalTo(expectedInstance.getMatchField()));
        assertThat(newInstance.getEnrichFields(), equalTo(expectedInstance.getEnrichFields()));
    }

    public void testEnrichCrud() throws IOException {
        Client client = getClient();
        XPackClient xPackClient = new XPackClient(client);
        EnrichClient enrichClient = xPackClient.enrichClient();

        EnrichPolicy policy = new EnrichPolicy("match", null, Collections.emptyList(), "test", Collections.emptyList());
        String policyName = "my-policy";

        AcknowledgedResponse acknowledgedResponse = enrichClient.putEnrichPolicy(new PutEnrichPolicyAction.Request(policyName, policy))
            .actionGet();

        assertTrue(acknowledgedResponse.isAcknowledged());

        GetEnrichPolicyAction.Response getResponse = enrichClient.getEnrichPolicy(
            new GetEnrichPolicyAction.Request(new String[] { policyName })
        ).actionGet();

        assertThat(getResponse.getPolicies().size(), equalTo(1));
        assertThat(policyName, equalTo(getResponse.getPolicies().get(0).getName()));
        assertEqualPolicies(policy, getResponse.getPolicies().get(0).getPolicy());

        acknowledgedResponse = enrichClient.deleteEnrichPolicy(new DeleteEnrichPolicyAction.Request(policyName)).actionGet();
        assertTrue(acknowledgedResponse.isAcknowledged());
    }
}
