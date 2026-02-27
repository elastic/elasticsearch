/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

import static io.opentelemetry.api.common.AttributeKey.stringKey;

public abstract class AbstractOTLPIndexingRestIT extends ESRestTestCase {

    protected static final String USER = "test_admin";
    protected static final String PASS = "x-pack-test-password";
    protected static final Resource TEST_RESOURCE = Resource.create(Attributes.of(stringKey("service.name"), "elasticsearch"));
    protected static final InstrumentationScopeInfo TEST_SCOPE = InstrumentationScopeInfo.create("io.opentelemetry.example.metrics");

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .user(USER, PASS, "superuser", false)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        assertBusy(() -> assertOK(client().performRequest(new Request("GET", "_index_template/metrics-otel@template"))));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected static String createApiKey(String indexPattern) throws IOException {
        Request createApiKeyRequest = new Request("POST", "/_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
              "name": "otel-test-key",
              "role_descriptors": {
                "writer": {
                  "index": [
                    {
                      "names": ["$INDEX_PATTERN"],
                      "privileges": ["create_doc", "auto_configure"]
                    }
                  ]
                }
              }
            }
            """.replace("$INDEX_PATTERN", indexPattern));
        ObjectPath createApiKeyResponse = ObjectPath.createFromResponse(client().performRequest(createApiKeyRequest));
        return createApiKeyResponse.evaluate("encoded");
    }
}
