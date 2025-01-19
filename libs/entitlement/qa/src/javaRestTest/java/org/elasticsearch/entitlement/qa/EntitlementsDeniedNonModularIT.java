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
import org.elasticsearch.entitlement.qa.test.RestEntitlementsCheckAction;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class EntitlementsDeniedNonModularIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("test-plugin", spec -> EntitlementsUtil.setupEntitlements(spec, false, null))
        .systemProperty("es.entitlements.enabled", "true")
        .setting("xpack.security.enabled", "false")
        // Logs in libs/entitlement/qa/build/test-results/javaRestTest/TEST-org.elasticsearch.entitlement.qa.EntitlementsDeniedIT.xml
        // .setting("logger.org.elasticsearch.entitlement", "DEBUG")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private final String actionName;

    public EntitlementsDeniedNonModularIT(@Name("actionName") String actionName) {
        this.actionName = actionName;
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return RestEntitlementsCheckAction.getAllCheckActions().stream().map(action -> new Object[] { action }).toList();
    }

    public void testCheckThrows() {
        logger.info("Executing Entitlement test for [{}]", actionName);
        var exception = expectThrows(IOException.class, () -> {
            var request = new Request("GET", "/_entitlement_check");
            request.addParameter("action", actionName);
            client().performRequest(request);
        });
        assertThat(exception.getMessage(), containsString("not_entitled_exception"));
    }
}
