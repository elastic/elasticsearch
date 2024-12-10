/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.entitlements;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class EntitlementsAllowedIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .plugin("entitlement-allowed")
        .systemProperty("es.entitlements.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .build();

    // private final String actionName;
    //
    // public EntitlementsAllowedIT(@Name("actionName") String actionName) {
    // this.actionName = actionName;
    // }
    //
    // @ParametersFactory
    // public static Iterable<Object[]> data() {
    // return RestEntitlementsCheckAction.getServerAndPluginsCheckActions().stream()
    // .map(s -> new Object[] { s })
    // .toList();
    // }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testCheckCreateURLClassLoaderWithPolicyPass() throws IOException {
        var request = new Request("GET", "/_entitlement/allowed/_check");
        request.addParameter("action", "create_classloader");
        Response result = client().performRequest(request);
        assertThat(result.getStatusLine().getStatusCode(), equalTo(200));
    }
}
