/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class WatcherRestartIT extends AbstractUpgradeTestCase {
    private static final Version UPGRADE_FROM_VERSION = Version.fromString(System.getProperty("tests.upgrade_from_version"));

    private static final String templatePrefix = ".watch-history-";

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/79895")
    public void testWatcherRestart() throws Exception {
        client().performRequest(new Request("POST", "/_watcher/_stop"));
        ensureWatcherStopped();

        client().performRequest(new Request("POST", "/_watcher/_start"));
        ensureWatcherStarted();

        if (CLUSTER_TYPE.equals(ClusterType.UPGRADED)) {
            validateHistoryTemplate();
        }
    }

    private void validateHistoryTemplate() throws Exception {
        // v7.7.0 contains a Watch history template (version 11) that can't be used unless all nodes in the cluster are >=7.7.0, so
        // in a mixed cluster with some nodes <7.7.0 it will install template version 10, but if all nodes are <=7.7.0 template v11
        // is used.
        final String expectedFinalTemplate = templatePrefix + "13";
        // In 7.10 watcher templates were converted to composable index templates, so we only
        // check legacy templates if we upgraded from a version that had legacy templates.
        // In 7.11 watcher templates were updated to add `allow_auto_create`
        if (UPGRADE_FROM_VERSION.before(Version.V_7_11_0)) {
            String expectedMixedClusterTemplate = templatePrefix;
            if (UPGRADE_FROM_VERSION.before(Version.V_7_7_0)) {
                expectedMixedClusterTemplate += "10";
            } else if (UPGRADE_FROM_VERSION.before(Version.V_7_10_0)) {
                expectedMixedClusterTemplate += "11";
            } else {
                expectedMixedClusterTemplate += "12";
            }

            if (UPGRADE_FROM_VERSION.before(Version.V_7_10_0) && ClusterType.MIXED == CLUSTER_TYPE) {
                assertTemplateExists(expectedMixedClusterTemplate);
            } else if (ClusterType.UPGRADED == CLUSTER_TYPE) {
                assertIndexTemplateExists(expectedFinalTemplate);
            }
        } else {
            assertIndexTemplateExists(expectedFinalTemplate);
        }
    }

    /**
     * Checks that the specified template exists.
     * <p>
     * The implementation fetches all templates so that if the template <strong>doesn't</strong> exist,
     * then the error messages can state which templates <strong>do</strong> exist, for debugging
     * purposes.
     */
    private void assertTemplateExists(String expectedTemplate) throws IOException {
        final Request request = new Request("GET", "/_template");
        request.addParameter("include_type_name", "false");
        RequestOptions.Builder builder = request.getOptions().toBuilder();
        builder.setWarningsHandler(WarningsHandler.PERMISSIVE);
        request.setOptions(builder);
        Response response = client().performRequest(request);
        assertOK(response);

        final Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            response.getEntity().getContent(),
            true
        );

        final Set<String> templateNames = responseMap.keySet();

        assertThat(
            "For version " + UPGRADE_FROM_VERSION + ", template " + expectedTemplate + " not found in: " + templateNames,
            templateNames,
            hasItem(expectedTemplate)
        );
    }

    /**
     * Checks that the specified index template exists.
     * <p>
     * The implementation fetches all templates so that if the template <strong>doesn't</strong> exist,
     * then the error messages can state which templates <strong>do</strong> exist, for debugging
     * purposes.
     */
    @SuppressWarnings("unchecked")
    private void assertIndexTemplateExists(String expectedTemplate) throws IOException {
        final Response response = client().performRequest(new Request("GET", "/_index_template"));
        assertOK(response);

        final Map<String, Object> responseMap = XContentHelper.convertToMap(
            JsonXContent.jsonXContent,
            response.getEntity().getContent(),
            true
        );

        List<Map<String, Object>> templates = (List<Map<String, Object>>) responseMap.get("index_templates");

        final List<String> templateNames = templates.stream().map(each -> (String) each.get("name")).collect(Collectors.toList());

        assertThat(
            "For version " + UPGRADE_FROM_VERSION + ", template " + expectedTemplate + " not found in: " + templateNames,
            templateNames,
            hasItem(expectedTemplate)
        );
    }

    private void ensureWatcherStopped() throws Exception {
        assertBusy(() -> {
            Response stats = client().performRequest(new Request("GET", "_watcher/stats"));
            String responseBody = EntityUtils.toString(stats.getEntity(), StandardCharsets.UTF_8);
            assertThat(responseBody, containsString("\"watcher_state\":\"stopped\""));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"starting\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"started\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"stopping\"")));
        });
    }

    private void ensureWatcherStarted() throws Exception {
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "_watcher/stats"));
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            assertThat(responseBody, containsString("\"watcher_state\":\"started\""));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"starting\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"stopping\"")));
            assertThat(responseBody, not(containsString("\"watcher_state\":\"stopped\"")));
        });
    }
}
