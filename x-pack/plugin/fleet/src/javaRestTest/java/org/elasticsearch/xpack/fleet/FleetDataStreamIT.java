/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class FleetDataStreamIT extends ESRestTestCase {

    static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @Override
    protected Settings restClientSettings() {
        // Note that we are superuser here but DO NOT provide a product origin
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    @Override
    protected Settings restAdminSettings() {
        // Note that we are both superuser here and provide a product origin
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)
            .put(ThreadContext.PREFIX + ".X-elastic-product-origin", "fleet")
            .build();
    }

    public void testAliasWithSystemDataStream() throws Exception {
        // Create a system data stream
        Request initialDocResponse = new Request("POST", ".fleet-actions-results/_doc");
        initialDocResponse.setJsonEntity("{\"@timestamp\": 0}");
        assertOK(adminClient().performRequest(initialDocResponse));

        // Create a system index - this one has an alias
        Request sysIdxRequest = new Request("PUT", ".fleet-artifacts");
        assertOK(adminClient().performRequest(sysIdxRequest));

        // Create a regular index
        String regularIndex = "regular-idx";
        String regularAlias = "regular-alias";
        Request regularIdxRequest = new Request("PUT", regularIndex);
        regularIdxRequest.setJsonEntity("{\"aliases\": {\"" + regularAlias + "\":  {}}}");
        assertOK(client().performRequest(regularIdxRequest));

        assertGetAliasAPIBehavesAsExpected(regularIndex, regularAlias);
    }

    public void testAliasWithSystemIndices() throws Exception {
        // Create a system index - this one has an alias
        Request sysIdxRequest = new Request("PUT", ".fleet-artifacts");
        assertOK(adminClient().performRequest(sysIdxRequest));

        // Create a regular index
        String regularIndex = "regular-idx";
        String regularAlias = "regular-alias";
        Request regularIdxRequest = new Request("PUT", regularIndex);
        regularIdxRequest.setJsonEntity("{\"aliases\": {\"" + regularAlias + "\":  {}}}");
        assertOK(client().performRequest(regularIdxRequest));

        assertGetAliasAPIBehavesAsExpected(regularIndex, regularAlias);
    }

    private void assertGetAliasAPIBehavesAsExpected(String regularIndex, String regularAlias) throws Exception {
        // Get a non-system alias, should not warn or error
        {
            Request request = new Request("GET", "_alias/" + regularAlias);
            Response response = client().performRequest(request);
            assertOK(response);
            assertThat(
                EntityUtils.toString(response.getEntity()),
                allOf(containsString(regularAlias), containsString(regularIndex), not(containsString(".fleet-artifacts")))
            );
        }

        // Fully specify a regular index and alias, should not warn or error
        {
            Request request = new Request("GET", regularIndex + "/_alias/" + regularAlias);
            Response response = client().performRequest(request);
            assertOK(response);
            assertThat(
                EntityUtils.toString(response.getEntity()),
                allOf(containsString(regularAlias), containsString(regularIndex), not(containsString(".fleet-artifacts")))
            );
        }

        // The rest of these produce a warning
        RequestOptions consumeWarningsOptions = RequestOptions.DEFAULT.toBuilder()
            .setWarningsHandler(
                warnings -> org.elasticsearch.core.List.of(
                    "this request accesses system indices: [.fleet-artifacts-7], but "
                        + "in a future major version, direct access to system indices will be prevented by default"
                ).equals(warnings) == false
            )
            .build();

        // The base _alias route warns because there is a system index in the response
        {
            Request request = new Request("GET", "_alias");
            request.setOptions(consumeWarningsOptions); // The result includes system indices, so we warn
            Response response = client().performRequest(request);
            assertOK(response);
            assertThat(
                EntityUtils.toString(response.getEntity()),
                allOf(containsString(regularAlias), containsString(regularIndex), not(containsString(".fleet-actions-results")))
            );
        }

        // Specify a system alias, should warn
        {
            Request request = new Request("GET", "_alias/.fleet-artifacts");
            request.setOptions(consumeWarningsOptions);
            Response response = client().performRequest(request);
            assertOK(response);
            assertThat(
                EntityUtils.toString(response.getEntity()),
                allOf(
                    containsString(".fleet-artifacts"),
                    containsString(".fleet-artifacts-7"),
                    not(containsString(regularAlias)),
                    not(containsString(regularIndex))
                )
            );
        }

        // Fully specify a system index and alias, should warn
        {
            Request request = new Request("GET", ".fleet-artifacts-7/_alias/.fleet-artifacts");
            request.setOptions(consumeWarningsOptions);
            Response response = client().performRequest(request);
            assertOK(response);
            assertThat(
                EntityUtils.toString(response.getEntity()),
                allOf(
                    containsString(".fleet-artifacts"),
                    containsString(".fleet-artifacts-7"),
                    not(containsString(regularAlias)),
                    not(containsString(regularIndex))
                )
            );
        }

        // Check an alias that doesn't exist
        {
            Request getAliasRequest = new Request("GET", "_alias/auditbeat-7.13.0");
            try {
                client().performRequest(getAliasRequest);
                fail("this request should not succeed, as it is looking for an alias that does not exist");
            } catch (ResponseException e) {
                assertThat(e.getResponse().getStatusLine().getStatusCode(), is(404));
                assertThat(
                    EntityUtils.toString(e.getResponse().getEntity()),
                    not(containsString("use and access is reserved for system operations"))
                );
            }
        }

        // Specify a system data stream as an alias - should 404
        {
            Request getAliasRequest = new Request("GET", "_alias/.fleet-actions-results");
            try {
                client().performRequest(getAliasRequest);
                fail("this request should not succeed, as it is looking for an alias that does not exist");
            } catch (ResponseException e) {
                assertThat(e.getResponse().getStatusLine().getStatusCode(), is(404));
                assertThat(
                    EntityUtils.toString(e.getResponse().getEntity()),
                    not(containsString("use and access is reserved for system operations"))
                );
            }
        }
    }

    public void testCountWithSystemDataStream() throws Exception {
        assertThatAPIWildcardResolutionWorks();

        // Create a system data stream
        Request initialDocResponse = new Request("POST", ".fleet-actions-results/_doc");
        initialDocResponse.setJsonEntity("{\"@timestamp\": 0}");
        assertOK(adminClient().performRequest(initialDocResponse));
        assertThatAPIWildcardResolutionWorks();

        // Create a system index - this one has an alias
        Request sysIdxRequest = new Request("PUT", ".fleet-artifacts");
        assertOK(adminClient().performRequest(sysIdxRequest));
        assertThatAPIWildcardResolutionWorks(
            singletonList(
                "this request accesses system indices: [.fleet-artifacts-7], but in a future major version, direct access to system"
                    + " indices will be prevented by default"
            )
        );
        assertThatAPIWildcardResolutionWorks(
            singletonList(
                "this request accesses system indices: [.fleet-artifacts-7], but in a future major version, direct access to system"
                    + " indices will be prevented by default"
            ),
            ".f*"
        );

        // Create a regular index
        String regularIndex = "regular-idx";
        String regularAlias = "regular-alias";
        Request regularIdxRequest = new Request("PUT", regularIndex);
        regularIdxRequest.setJsonEntity("{\"aliases\": {\"" + regularAlias + "\":  {}}}");
        assertOK(client().performRequest(regularIdxRequest));
        assertThatAPIWildcardResolutionWorks(
            singletonList(
                "this request accesses system indices: [.fleet-artifacts-7], but in a future major version, direct access to system"
                    + " indices will be prevented by default"
            )
        );
        assertThatAPIWildcardResolutionWorks(emptyList(), "r*");
    }

    private void assertThatAPIWildcardResolutionWorks() throws Exception {
        assertThatAPIWildcardResolutionWorks(emptyList(), null);
    }

    private void assertThatAPIWildcardResolutionWorks(List<String> warningsExpected) throws Exception {
        assertThatAPIWildcardResolutionWorks(warningsExpected, null);
    }

    private void assertThatAPIWildcardResolutionWorks(List<String> warningsExpected, String indexPattern) throws Exception {
        String path = indexPattern == null || indexPattern.isEmpty() ? "/_count" : "/" + indexPattern + "/_count";
        Request countRequest = new Request("GET", path);
        if (warningsExpected.isEmpty() == false) {
            countRequest.setOptions(
                countRequest.getOptions().toBuilder().setWarningsHandler(warnings -> warningsExpected.equals(warnings) == false)
            );
        }
        assertOK(client().performRequest(countRequest));
    }
}
