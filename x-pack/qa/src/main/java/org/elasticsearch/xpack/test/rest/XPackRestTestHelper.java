/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.test.rest;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.junit.Assert.fail;

public final class XPackRestTestHelper {

    private XPackRestTestHelper() {}

    /**
     * For each template name wait for the template to be created.
     *
     * @param client             The rest client
     * @param expectedTemplates  Names of the templates to wait for
     * @throws InterruptedException If the wait is interrupted
     */
    @SuppressWarnings("unchecked")
    public static void waitForTemplates(RestClient client, List<String> expectedTemplates) throws Exception {
        // TODO: legacy support can be removed once all X-Pack plugins use only composable
        // templates in the oldest version we test upgrades from
        assertBusy(() -> {
            Map<String, Object> response;
            final Request request = new Request("GET", "_index_template");
            request.addParameter("error_trace", "true");

            String string = EntityUtils.toString(client.performRequest(request).getEntity());
            List<Map<String, Object>> templateList = (List<Map<String, Object>>) XContentHelper.convertToMap(
                JsonXContent.jsonXContent,
                string,
                false
            ).get("index_templates");
            response = templateList.stream().collect(Collectors.toMap(m -> (String) m.get("name"), m -> m.get("index_template")));
            final Set<String> templates = new TreeSet<>(response.keySet());

            final Request legacyRequest = new Request("GET", "_template");
            legacyRequest.addParameter("error_trace", "true");

            string = EntityUtils.toString(client.performRequest(legacyRequest).getEntity());
            Map<String, Object> legacyResponse = XContentHelper.convertToMap(JsonXContent.jsonXContent, string, false);

            final Set<String> legacyTemplates = new TreeSet<>(legacyResponse.keySet());

            final List<String> missingTemplates = expectedTemplates.stream()
                .filter(each -> templates.contains(each) == false)
                .filter(each -> legacyTemplates.contains(each) == false)
                .toList();

            // While it's possible to use a Hamcrest matcher for this, the failure is much less legible.
            if (missingTemplates.isEmpty() == false) {
                fail(
                    "Some expected templates are missing: "
                        + missingTemplates
                        + ". The composable templates that exist are: "
                        + templates
                        + ". The legacy templates that exist are: "
                        + legacyTemplates
                );
            }
        });
    }

    public static String resultsWriteAlias(String jobId) {
        // ".write" rather than simply "write" to avoid the danger of clashing
        // with the read alias of a job whose name begins with "write-"
        return XPackRestTestConstants.RESULTS_INDEX_PREFIX + ".write-" + jobId;
    }
}
