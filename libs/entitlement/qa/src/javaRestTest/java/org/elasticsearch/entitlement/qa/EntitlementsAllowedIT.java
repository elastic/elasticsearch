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
import org.elasticsearch.entitlement.qa.common.RestEntitlementsCheckAction;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class EntitlementsAllowedIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .plugin("entitlement-allowed")
        .plugin("entitlement-allowed-nonmodular")
        .systemProperty("es.entitlements.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .build();

    private final String pathPrefix;
    private final String actionName;

    public EntitlementsAllowedIT(@Name("pathPrefix") String pathPrefix, @Name("actionName") String actionName) {
        this.pathPrefix = pathPrefix;
        this.actionName = actionName;
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return Stream.of("allowed", "allowed_nonmodular")
            .flatMap(
                path -> RestEntitlementsCheckAction.getServerAndPluginsCheckActions().stream().map(action -> new Object[] { path, action })
            )
            .toList();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testCheckActionWithPolicyPass() throws IOException {
        logger.info("Executing Entitlement test [{}] for [{}]", pathPrefix, actionName);
        var request = new Request("GET", "/_entitlement/" + pathPrefix + "/_check");
        request.addParameter("action", actionName);
        Response result = client().performRequest(request);
        assertThat(result.getStatusLine().getStatusCode(), equalTo(200));
    }
}
