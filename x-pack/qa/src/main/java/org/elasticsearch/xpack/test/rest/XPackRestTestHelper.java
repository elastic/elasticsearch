/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.test.rest;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.elasticsearch.test.rest.ESRestTestCase.allowTypesRemovalWarnings;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;

public final class XPackRestTestHelper {

    private XPackRestTestHelper() {
    }

    /**
     * For each template name wait for the template to be created and
     * for the template version to be equal to the master node version.
     *
     * @param client            The rest client
     * @param templateNames     Names of the templates to wait for
     * @throws InterruptedException If the wait is interrupted
     */
    public static void waitForTemplates(RestClient client, List<String> templateNames) throws Exception {
        AtomicReference<Version> masterNodeVersion = new AtomicReference<>();

        assertBusy(() -> {
            Request request = new Request("GET", "/_cat/nodes");
            request.addParameter("h", "master,version");
            String response = EntityUtils.toString(client.performRequest(request).getEntity());

            for (String line : response.split("\n")) {
                if (line.startsWith("*")) {
                    masterNodeVersion.set(Version.fromString(line.substring(2).trim()));
                    return;
                }
            }
            fail("No master elected");
        });

        for (String template : templateNames) {
            assertBusy(() -> {
                Map<?, ?> response;
                try {
                    final Request getRequest = new Request("GET", "_template/" + template);
                    getRequest.setOptions(allowTypesRemovalWarnings());
                    String string = EntityUtils.toString(client.performRequest(getRequest).getEntity());
                    response = XContentHelper.convertToMap(JsonXContent.jsonXContent, string, false);
                } catch (ResponseException e) {
                    if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                        fail("Template [" + template + "] not found");
                    }
                    throw e;
                }
                Map<?, ?> templateDefinition = (Map<?, ?>) response.get(template);
                assertThat(Version.fromId((Integer) templateDefinition.get("version")), equalTo(masterNodeVersion.get()));
            });
        }
    }

    public static String resultsWriteAlias(String jobId) {
        // ".write" rather than simply "write" to avoid the danger of clashing
        // with the read alias of a job whose name begins with "write-"
        return XPackRestTestConstants.RESULTS_INDEX_PREFIX + ".write-" + jobId;
    }
}
