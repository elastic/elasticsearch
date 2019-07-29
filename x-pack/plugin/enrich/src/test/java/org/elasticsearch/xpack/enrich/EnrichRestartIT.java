/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.Version;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ListEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.enrich.EnrichMultiNodeIT.DECORATE_FIELDS;
import static org.elasticsearch.xpack.enrich.EnrichMultiNodeIT.KEY_FIELD;
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

        EnrichPolicy enrichPolicy = newPolicy();
        for (int i = 0; i < numPolicies; i++) {
            String policyName = POLICY_NAME + i;
            PutEnrichPolicyAction.Request request = new PutEnrichPolicyAction.Request(policyName, newPolicy());
            if (request.validate() != null) {
                throw request.validate();
            }
            client().execute(PutEnrichPolicyAction.INSTANCE, request).actionGet();
        }

        verifyPolicies(numPolicies, enrichPolicy);
        // After full restart the policies should still exist:
        internalCluster().fullRestart();
        verifyPolicies(numPolicies, enrichPolicy);
    }

    private EnrichPolicy newPolicy() {
        return new EnrichPolicy(EnrichPolicy.EXACT_MATCH_TYPE, null, List.of(SOURCE_INDEX_NAME), KEY_FIELD, List.of(DECORATE_FIELDS));
    }

    private static void verifyPolicies(int numPolicies, EnrichPolicy enrichPolicy) {
        ListEnrichPolicyAction.Response response =
            client().execute(ListEnrichPolicyAction.INSTANCE, new ListEnrichPolicyAction.Request()).actionGet();
        assertThat(response.getPolicies().size(), equalTo(numPolicies));
        for (int i = 0; i < numPolicies; i++) {
            String policyName = POLICY_NAME + i;
            Optional<EnrichPolicy.NamedPolicy> result = response.getPolicies().stream()
                .filter(namedPolicy -> namedPolicy.getName().equals(policyName))
                .findFirst();
            assertThat(result.isPresent(), is(true));
            assertThat(result.get().getPolicy().getType(), equalTo(enrichPolicy.getType()));
            assertThat(result.get().getPolicy().getQuery(), equalTo(enrichPolicy.getQuery()));
            assertThat(result.get().getPolicy().getIndices(), equalTo(enrichPolicy.getIndices()));
            assertThat(result.get().getPolicy().getEnrichKey(), equalTo(enrichPolicy.getEnrichKey()));
            assertThat(result.get().getPolicy().getEnrichValues(), equalTo(enrichPolicy.getEnrichValues()));
            assertThat(result.get().getPolicy().getVersionCreated(), equalTo(Version.CURRENT));
        }
    }

}
