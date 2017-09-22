/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.XPackPlugin;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;

@ESIntegTestCase.ClusterScope(scope = SUITE)
public class UpgradeToTrialTests extends AbstractLicensesIntegrationTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("node.data", true)
                .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "basic")
                .put(NetworkModule.HTTP_ENABLED.getKey(), true).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(XPackPlugin.class, Netty4Plugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void testUpgradeToTrial() throws Exception {
        LicensingClient licensingClient = new LicensingClient(client());
        GetLicenseResponse getLicenseResponse = licensingClient.prepareGetLicense().get();

        assertEquals("basic", getLicenseResponse.license().type());

        RestClient restClient = getRestClient();
        Response response = restClient.performRequest("GET", "/_xpack/license/trial_status");
        String body = Streams.copyToString(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals("{\"eligible_to_start_trial\":true}", body);

        Response response2 = restClient.performRequest("POST", "/_xpack/license/start_trial");
        String body2 = Streams.copyToString(new InputStreamReader(response2.getEntity().getContent(), StandardCharsets.UTF_8));
        assertEquals(200, response2.getStatusLine().getStatusCode());
        assertEquals("{\"trial_was_started\":true}", body2);

        Response response3 = restClient.performRequest("GET", "/_xpack/license/trial_status");
        String body3 = Streams.copyToString(new InputStreamReader(response3.getEntity().getContent(), StandardCharsets.UTF_8));
        assertEquals(200, response3.getStatusLine().getStatusCode());
        assertEquals("{\"eligible_to_start_trial\":false}", body3);

        ResponseException ex = expectThrows(ResponseException.class,
                () -> restClient.performRequest("POST", "/_xpack/license/start_trial"));
        Response response4 = ex.getResponse();
        String body4 = Streams.copyToString(new InputStreamReader(response4.getEntity().getContent(), StandardCharsets.UTF_8));
        assertEquals(403, response4.getStatusLine().getStatusCode());
        assertTrue(body4.contains("\"trial_was_started\":false"));
        assertTrue(body4.contains("\"error_message\":\"Operation failed: Trial was already activated.\""));
    }
}
