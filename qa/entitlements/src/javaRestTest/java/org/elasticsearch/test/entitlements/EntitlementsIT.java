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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

@ESTestCase.WithoutSecurityManager
public class EntitlementsIT extends ESRestTestCase {

    private static final String ENTITLEMENT_BRIDGE_JAR_NAME = System.getProperty("tests.entitlement-bridge.jar-name");

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .plugin("entitlement-qa")
        .systemProperty("es.entitlements.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .jvmArg("-Djdk.attach.allowAttachSelf=true")
        .jvmArg("-XX:+EnableDynamicAgentLoading")
        .jvmArg("--patch-module=java.base=lib/entitlement-bridge/" + ENTITLEMENT_BRIDGE_JAR_NAME)
        .jvmArg("--add-exports=java.base/org.elasticsearch.entitlement.bridge=org.elasticsearch.entitlement")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testCheckSystemExit() {
        var exception = expectThrows(
            IOException.class,
            () -> { client().performRequest(new Request("GET", "/_entitlement/_check_system_exit")); }
        );
        assertThat(exception.getMessage(), containsString("not_entitled_exception"));
    }
}
