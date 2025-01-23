/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.entitlement.qa.test.RestEntitlementsCheckAction;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.elasticsearch.entitlement.qa.EntitlementsUtil.ALLOWED_ENTITLEMENTS;
import static org.hamcrest.Matchers.equalTo;

public class EntitlementsAllowedNonModularIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("test-plugin", spec -> EntitlementsUtil.setupEntitlements(spec, false, ALLOWED_ENTITLEMENTS))
        .systemProperty("es.entitlements.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .build();

    private final String actionName;

    public EntitlementsAllowedNonModularIT(@Name("actionName") String actionName) {
        this.actionName = actionName;
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return RestEntitlementsCheckAction.getCheckActionsAllowedInPlugins().stream().map(action -> new Object[] { action }).toList();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testCheckActionWithPolicyPass() throws IOException {
        logger.info("Executing Entitlement test for [{}]", actionName);
        var request = new Request("GET", "/_entitlement_check");
        request.addParameter("action", actionName);
        Response result = client().performRequest(request);
        assertThat(result.getStatusLine().getStatusCode(), equalTo(200));
    }
}
