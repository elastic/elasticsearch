/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class InferenceServiceIT extends ESRestTestCase {
    // protected static final String BASIC_AUTH_VALUE_SUPER_USER = UsernamePasswordToken.basicAuthHeaderValue(
    // "x_pack_rest_user",
    // SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    // );
    //
    // @Override
    // protected Settings restClientSettings() {
    // return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE_SUPER_USER).build();
    // }

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "true")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .user("super-user", "x-pack-super-password")
        // .setting("xpack.security.enabled", "false")
        .build();

    @Override
    protected Settings restClientSettings() {
        return getClientSettings("super-user", "x-pack-super-password");
    }

    @Override
    protected Settings restAdminSettings() {
        return getClientSettings("super-user", "x-pack-super-password");
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private Settings getClientSettings(final String username, final String password) {
        final String token = basicAuthHeaderValue(username, new SecureString(password.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testGetModel_DoesNotIncludeSecrets() throws IOException {
        var modelId = "test-elser";
        var inferenceRoute = "_inference/sparse_embedding/" + modelId;
        Request putElserServiceReq = new Request("PUT", inferenceRoute);
        String body = Strings.format("""
            {
              "service": "elser_mlnode",
              "service_settings": {
                  "num_allocations": 1,
                  "num_threads": 1
              },
              "task_settings": {
              }
            }
            """);

        putElserServiceReq.setJsonEntity(body);
        Response putServiceResponse = client().performRequest(putElserServiceReq);
        assertThat(putServiceResponse.getStatusLine().getStatusCode(), is(RestStatus.OK));

        Request getElserServiceReq = new Request("GET", inferenceRoute);
        Response getResponse = client().performRequest(getElserServiceReq);
        assertThat(EntityUtils.toString(getResponse.getEntity()), is(""));
    }
}
