/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.junit.After;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.is;

public class MonitoringWithWatcherRestIT extends ESRestTestCase {

    /**
     * An unsorted list of Watch IDs representing resource files for Monitoring Cluster Alerts.
     */
    public static final String[] WATCH_IDS = {
        "elasticsearch_cluster_status",
        "elasticsearch_version_mismatch",
        "kibana_version_mismatch",
        "logstash_version_mismatch",
        "xpack_license_expiration",
        "elasticsearch_nodes",
    };

    @After
    public void cleanExporters() throws Exception {
        Request cleanupSettingsRequest = new Request("PUT", "/_cluster/settings");
        cleanupSettingsRequest.setJsonEntity(Strings.toString(jsonBuilder().startObject()
                .startObject("transient")
                    .nullField("xpack.monitoring.exporters.*")
                .endObject().endObject()));
        adminClient().performRequest(cleanupSettingsRequest);
        final Request deleteRequest = new Request("DELETE", "/.watch*");
        RequestOptions allowSystemIndexAccessWarningOptions = RequestOptions.DEFAULT.toBuilder()
            .setWarningsHandler(warnings -> {
                if (warnings.size() != 1) {
                    return true;
                }
                // We don't know exactly which indices we're cleaning up in advance, so just accept all system index access warnings.
                final String warning = warnings.get(0);
                final boolean isSystemIndexWarning = warning.contains("this request accesses system indices")
                    && warning.contains("but in a future major version, direct access to system indices will be prevented by default");
                return isSystemIndexWarning == false;
            }).build();
        deleteRequest.setOptions(allowSystemIndexAccessWarningOptions);
        adminClient().performRequest(deleteRequest);
    }

    @AwaitsFix( bugUrl = "https://github.com/elastic/elasticsearch/issues/59132" )
    public void testThatLocalExporterAddsWatches() throws Exception {
        String watchId = createMonitoringWatch();

        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(Strings.toString(jsonBuilder().startObject()
                .startObject("transient")
                    .field("xpack.monitoring.exporters.my_local_exporter.type", "local")
                    .field("xpack.monitoring.exporters.my_local_exporter.cluster_alerts.management.enabled", true)
                .endObject().endObject()));
        adminClient().performRequest(request);

        assertTotalWatchCount(WATCH_IDS.length);

        assertMonitoringWatchHasBeenOverWritten(watchId);
    }

    public void testThatHttpExporterAddsWatches() throws Exception {
        String watchId = createMonitoringWatch();
        String httpHost = getHttpHost();

        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(Strings.toString(jsonBuilder().startObject()
                .startObject("transient")
                    .field("xpack.monitoring.exporters.my_http_exporter.type", "http")
                    .field("xpack.monitoring.exporters.my_http_exporter.host", httpHost)
                    .field("xpack.monitoring.exporters.my_http_exporter.cluster_alerts.management.enabled", true)
                .endObject().endObject()));
        adminClient().performRequest(request);

        assertTotalWatchCount(WATCH_IDS.length);

        assertMonitoringWatchHasBeenOverWritten(watchId);
    }

    private void assertMonitoringWatchHasBeenOverWritten(String watchId) throws Exception {
        assertBusy(() -> {
            ObjectPath path = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_watcher/watch/" + watchId)));
            String interval = path.evaluate("watch.trigger.schedule.interval");
            assertThat(interval, is("1m"));
        });
    }

    private void assertTotalWatchCount(int expectedWatches) throws Exception {
        assertBusy(() -> {
            refreshAllIndices();
            final Request countRequest = new Request("POST", "/_watcher/_query/watches");
            ObjectPath path = ObjectPath.createFromResponse(client().performRequest(countRequest));
            int count = path.evaluate("count");
            assertThat(count, is(expectedWatches));
        });
    }

    private String createMonitoringWatch() throws Exception {
        String clusterUUID = getClusterUUID();
        String watchId = clusterUUID + "_kibana_version_mismatch";
        Request request = new Request("PUT", "/_watcher/watch/" + watchId);
        String watch = "{\"trigger\":{\"schedule\":{\"interval\":\"1000m\"}},\"input\":{\"simple\":{}}," +
            "\"condition\":{\"always\":{}}," +
            "\"actions\":{\"logme\":{\"logging\":{\"level\":\"info\",\"text\":\"foo\"}}}}";
        request.setJsonEntity(watch);
        client().performRequest(request);
        return watchId;
    }

    private String getClusterUUID() throws Exception {
        Response response = client().performRequest(new Request("GET", "/_cluster/state/metadata"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        String clusterUUID = objectPath.evaluate("metadata.cluster_uuid");
        return clusterUUID;
    }

    public String getHttpHost() throws IOException {
        ObjectPath path = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_cluster/state")));
        String masterNodeId = path.evaluate("master_node");

        ObjectPath nodesPath = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        String httpHost = nodesPath.evaluate("nodes." + masterNodeId + ".http.publish_address");
        return httpHost;
    }
}
