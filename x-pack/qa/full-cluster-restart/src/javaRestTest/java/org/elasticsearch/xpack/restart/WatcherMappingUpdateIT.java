/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.restart;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class WatcherMappingUpdateIT extends AbstractXpackFullClusterRestartTestCase {

    public WatcherMappingUpdateIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testMappingsAreUpdated() throws Exception {
        if (isRunningAgainstOldCluster()) {
            // post a watch
            Request putWatchRequest = new Request("PUT", "_watcher/watch/log_error_watch");
            putWatchRequest.setJsonEntity("""
                {
                  "trigger" : {
                    "schedule" : { "interval" : "10s" }
                  },
                  "input" : {
                    "search" : {
                      "request" : {
                        "indices" : [ "logs" ],
                        "body" : {
                          "query" : {
                            "match" : { "message": "error" }
                          }
                        }
                      }
                    }
                  }
                }
                """);
            client().performRequest(putWatchRequest);

            if (getOldClusterVersion().onOrAfter(Version.V_7_13_0)) {
                assertMappingVersion(".watches", getOldClusterVersion());
            } else {
                // watches indices from before 7.10 do not have mapping versions in _meta
                assertNoMappingVersion(".watches");
            }
        } else {
            assertMappingVersion(".watches", Version.CURRENT);
        }
    }

    private void assertMappingVersion(String index, Version clusterVersion) throws Exception {
        assertBusy(() -> {
            Request mappingRequest = new Request("GET", index + "/_mappings");
            mappingRequest.setOptions(getWarningHandlerOptions(index));
            Response response = client().performRequest(mappingRequest);
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            assertThat(responseBody, containsString("\"version\":\"" + clusterVersion + "\""));
        }, 60L, TimeUnit.SECONDS);
    }

    private void assertNoMappingVersion(String index) throws Exception {
        assertBusy(() -> {
            Request mappingRequest = new Request("GET", index + "/_mappings");
            if (isRunningAgainstOldCluster() == false || getOldClusterVersion().onOrAfter(Version.V_7_10_0)) {
                mappingRequest.setOptions(getWarningHandlerOptions(index));
            }
            Response response = client().performRequest(mappingRequest);
            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            assertThat(responseBody, not(containsString("\"version\":\"")));
        }, 60L, TimeUnit.SECONDS);
    }

    private RequestOptions.Builder getWarningHandlerOptions(String index) {
        return RequestOptions.DEFAULT.toBuilder()
            .setWarningsHandler(w -> w.size() > 0 && w.contains(getWatcherSystemIndexWarning(index)) == false);
    }

    private String getWatcherSystemIndexWarning(String index) {
        return "this request accesses system indices: ["
            + index
            + "], but in a future major version, "
            + "direct access to system indices will be prevented by default";
    }
}
