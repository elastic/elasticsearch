/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.test.rest;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.assertBusy;
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
     * @param client             The rest client
     * @param expectedTemplates  Names of the templates to wait for
     * @throws InterruptedException If the wait is interrupted
     */
    public static void waitForTemplates(RestClient client, List<String> expectedTemplates) throws Exception {
        AtomicReference<Version> masterNodeVersion = new AtomicReference<>();

        assertBusy(() -> {
            Request request = new Request("GET", "/_cat/nodes");
            request.addParameter("h", "master,version");
            request.addParameter("error_trace", "true");
            String response = EntityUtils.toString(client.performRequest(request).getEntity());

            for (String line : response.split("\n")) {
                if (line.startsWith("*")) {
                    masterNodeVersion.set(Version.fromString(line.substring(2).trim()));
                    return;
                }
            }
            fail("No master elected");
        });

        assertBusy(() -> {
            final Request request = new Request("GET", "_template");
            request.addParameter("error_trace", "true");

            String string = EntityUtils.toString(client.performRequest(request).getEntity());
            Map<String, Object> response = XContentHelper.convertToMap(JsonXContent.jsonXContent, string, false);

            final Set<String> templates = new TreeSet<>(response.keySet());

            final List<String> missingTemplates = expectedTemplates.stream()
                .filter(each -> templates.contains(each) == false)
                .collect(Collectors.toList());

            // While it's possible to use a Hamcrest matcher for this, the failure is much less legible.
            if (missingTemplates.isEmpty() == false) {
                fail("Some expected templates are missing: " + missingTemplates + ". The templates that exist are: " + templates + "");
            }

            expectedTemplates.forEach(template -> {
                Map<?, ?> templateDefinition = (Map<?, ?>) response.get(template);
                assertThat(
                    "Template [" + template + "] has unexpected version",
                    Version.fromId((Integer) templateDefinition.get("version")),
                    equalTo(masterNodeVersion.get()));
            });
        });
    }

    public static String resultsWriteAlias(String jobId) {
        // ".write" rather than simply "write" to avoid the danger of clashing
        // with the read alias of a job whose name begins with "write-"
        return XPackRestTestConstants.RESULTS_INDEX_PREFIX + ".write-" + jobId;
    }
}
