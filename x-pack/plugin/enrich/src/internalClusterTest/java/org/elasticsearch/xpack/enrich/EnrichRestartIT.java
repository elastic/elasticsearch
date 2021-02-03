/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.enrich.AbstractEnrichTestCase.createSourceIndices;
import static org.elasticsearch.xpack.enrich.EnrichMultiNodeIT.DECORATE_FIELDS;
import static org.elasticsearch.xpack.enrich.EnrichMultiNodeIT.MATCH_FIELD;
import static org.elasticsearch.xpack.enrich.EnrichMultiNodeIT.POLICY_NAME;
import static org.elasticsearch.xpack.enrich.EnrichMultiNodeIT.SOURCE_INDEX_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class EnrichRestartIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateEnrich.class, ReindexPlugin.class);
    }

    public void testRestart() throws Exception {
        final int numPolicies = randomIntBetween(2, 4);
        internalCluster().startNode();

        EnrichPolicy enrichPolicy = new EnrichPolicy(
            EnrichPolicy.MATCH_TYPE,
            null,
            List.of(SOURCE_INDEX_NAME),
            MATCH_FIELD,
            List.of(DECORATE_FIELDS)
        );
        createSourceIndices(client(), enrichPolicy);
        for (int i = 0; i < numPolicies; i++) {
            String policyName = POLICY_NAME + i;
            PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(policyName, enrichPolicy);
            client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
        }

        verifyPolicies(numPolicies, enrichPolicy);
        // After full restart the policies should still exist:
        internalCluster().fullRestart();
        verifyPolicies(numPolicies, enrichPolicy);
    }

    private static void verifyPolicies(int numPolicies, EnrichPolicy enrichPolicy) {
        GetEnrichPolicyAction.Response response = client().execute(GetEnrichPolicyAction.INSTANCE, new GetEnrichPolicyAction.Request())
            .actionGet();
        assertThat(response.getPolicies().size(), equalTo(numPolicies));
        for (int i = 0; i < numPolicies; i++) {
            String policyName = POLICY_NAME + i;
            Optional<EnrichPolicy.NamedPolicy> result = response.getPolicies()
                .stream()
                .filter(namedPolicy -> namedPolicy.getName().equals(policyName))
                .findFirst();
            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getPolicy(), equalTo(enrichPolicy));
        }
    }

}
