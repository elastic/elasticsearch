/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.elasticsearch.xpack.watcher.actions.ActionBuilders;
import org.elasticsearch.xpack.watcher.client.WatchSourceBuilders;
import org.elasticsearch.xpack.watcher.trigger.TriggerBuilders;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.junit.After;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule.Interval.Unit.MINUTES;
import static org.hamcrest.Matchers.is;

@TestLogging("org.elasticsearch.client:TRACE,tracer:TRACE")
@AwaitsFix(bugUrl = "flaky tests")
public class MonitoringWithWatcherRestIT extends ESRestTestCase {

    @After
    public void cleanExporters() throws Exception {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(Strings.toString(jsonBuilder().startObject()
                .startObject("transient")
                    .nullField("xpack.monitoring.exporters.*")
                .endObject().endObject()));
        adminClient().performRequest(request);
        adminClient().performRequest(new Request("DELETE", "/.watch*"));
    }

    public void testThatLocalExporterAddsWatches() throws Exception {
        String watchId = createMonitoringWatch();

        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(Strings.toString(jsonBuilder().startObject()
                .startObject("transient")
                    .field("xpack.monitoring.exporters.my_local_exporter.type", "local")
                    .field("xpack.monitoring.exporters.my_local_exporter.cluster_alerts.management.enabled", true)
                .endObject().endObject()));
        adminClient().performRequest(request);

        assertTotalWatchCount(ClusterAlertsUtil.WATCH_IDS.length);

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

        assertTotalWatchCount(ClusterAlertsUtil.WATCH_IDS.length);

        assertMonitoringWatchHasBeenOverWritten(watchId);
    }

    private void assertMonitoringWatchHasBeenOverWritten(String watchId) throws Exception {
        ObjectPath path = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_xpack/watcher/watch/" + watchId)));
        String interval = path.evaluate("watch.trigger.schedule.interval");
        assertThat(interval, is("1m"));
    }

    private void assertTotalWatchCount(int expectedWatches) throws Exception {
        assertBusy(() -> {
            assertOK(client().performRequest(new Request("POST", "/.watches/_refresh")));
            ObjectPath path = ObjectPath.createFromResponse(client().performRequest(new Request("POST", "/.watches/_count")));
            int count = path.evaluate("count");
            assertThat(count, is(expectedWatches));
        });
    }

    private String createMonitoringWatch() throws Exception {
        String clusterUUID = getClusterUUID();
        String watchId = clusterUUID + "_kibana_version_mismatch";
        Request request = new Request("PUT", "/_xpack/watcher/watch/" + watchId);
        request.setJsonEntity(WatchSourceBuilders.watchBuilder()
                .trigger(TriggerBuilders.schedule(new IntervalSchedule(new IntervalSchedule.Interval(1000, MINUTES))))
                .input(simpleInput())
                .addAction("logme", ActionBuilders.loggingAction("foo"))
                .buildAsBytes(XContentType.JSON).utf8ToString());
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
