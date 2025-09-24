/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl.extension;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;

public class SslProfileExtensionIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .plugin("test-ssl-extension")
        .configFile("test.ssl.ca.crt", Resource.fromClasspath("ca.crt"))
        .setting("test.ssl.certificate_authorities", "test.ssl.ca.crt")
        .setting("xpack.security.enabled", "true")
        .user("admin", "pass/word")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String basicAuth = basicAuthHeaderValue("admin", new SecureString("pass/word".toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", basicAuth).build();
    }

    public void testCertificateIsLoaded() throws Exception {
        final Response certResponse = client().performRequest(new Request("GET", "/_ssl/certificates"));
        final List<Object> certs = entityAsList(certResponse);

        assertThat(certs, everyItem(Matchers.instanceOf(Map.class)));
        assertThat(certs, hasItem(hasEntry("path", "test.ssl.ca.crt")));
    }

}
