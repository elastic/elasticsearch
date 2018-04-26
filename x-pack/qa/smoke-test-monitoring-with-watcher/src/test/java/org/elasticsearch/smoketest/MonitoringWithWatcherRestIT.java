/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
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
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule.Interval.Unit.MINUTES;
import static org.hamcrest.Matchers.is;

@TestLogging("org.elasticsearch.client:TRACE,tracer:TRACE")
@AwaitsFix(bugUrl = "https://github.com/elastic/x-pack-elasticsearch/issues/2920")
public class MonitoringWithWatcherRestIT extends ESRestTestCase {

    @After
    public void cleanExporters() throws Exception {
        String body = Strings.toString(jsonBuilder().startObject().startObject("transient")
                .nullField("xpack.monitoring.exporters.*")
                .endObject().endObject());
        assertOK(adminClient().performRequest("PUT", "_cluster/settings", Collections.emptyMap(),
                new StringEntity(body, ContentType.APPLICATION_JSON)));

        assertOK(adminClient().performRequest("DELETE", ".watch*", Collections.emptyMap()));
    }

    public void testThatLocalExporterAddsWatches() throws Exception {
        String watchId = createMonitoringWatch();

        String body = BytesReference.bytes(jsonBuilder().startObject().startObject("transient")
                .field("xpack.monitoring.exporters.my_local_exporter.type", "local")
                .field("xpack.monitoring.exporters.my_local_exporter.cluster_alerts.management.enabled", true)
                .endObject().endObject()).utf8ToString();

        adminClient().performRequest("PUT", "_cluster/settings", Collections.emptyMap(),
                new StringEntity(body, ContentType.APPLICATION_JSON));

        assertTotalWatchCount(ClusterAlertsUtil.WATCH_IDS.length);

        assertMonitoringWatchHasBeenOverWritten(watchId);
    }

    public void testThatHttpExporterAddsWatches() throws Exception {
        String watchId = createMonitoringWatch();
        String httpHost = getHttpHost();

        String body = BytesReference.bytes(jsonBuilder().startObject().startObject("transient")
                .field("xpack.monitoring.exporters.my_http_exporter.type", "http")
                .field("xpack.monitoring.exporters.my_http_exporter.host", httpHost)
                .field("xpack.monitoring.exporters.my_http_exporter.cluster_alerts.management.enabled", true)
                .endObject().endObject()).utf8ToString();

        adminClient().performRequest("PUT", "_cluster/settings", Collections.emptyMap(),
                new StringEntity(body, ContentType.APPLICATION_JSON));

        assertTotalWatchCount(ClusterAlertsUtil.WATCH_IDS.length);

        assertMonitoringWatchHasBeenOverWritten(watchId);
    }

    private void assertMonitoringWatchHasBeenOverWritten(String watchId) throws Exception {
        ObjectPath path = ObjectPath.createFromResponse(client().performRequest("GET", "_xpack/watcher/watch/" + watchId));
        String interval = path.evaluate("watch.trigger.schedule.interval");
        assertThat(interval, is("1m"));
    }

    private void assertTotalWatchCount(int expectedWatches) throws Exception {
        assertBusy(() -> {
            assertOK(client().performRequest("POST", ".watches/_refresh"));
            ObjectPath path = ObjectPath.createFromResponse(client().performRequest("POST", ".watches/_count"));
            int count = path.evaluate("count");
            assertThat(count, is(expectedWatches));
        });
    }

    private String createMonitoringWatch() throws Exception {
        String clusterUUID = getClusterUUID();
        String watchId = clusterUUID + "_kibana_version_mismatch";
        String sampleWatch = WatchSourceBuilders.watchBuilder()
                .trigger(TriggerBuilders.schedule(new IntervalSchedule(new IntervalSchedule.Interval(1000, MINUTES))))
                .input(simpleInput())
                .addAction("logme", ActionBuilders.loggingAction("foo"))
                .buildAsBytes(XContentType.JSON).utf8ToString();
        client().performRequest("PUT", "_xpack/watcher/watch/" + watchId, Collections.emptyMap(),
                new StringEntity(sampleWatch, ContentType.APPLICATION_JSON));
        return watchId;
    }

    private String getClusterUUID() throws Exception {
        Response response = client().performRequest("GET", "_cluster/state/metadata", Collections.emptyMap());
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        String clusterUUID = objectPath.evaluate("metadata.cluster_uuid");
        return clusterUUID;
    }

    public String getHttpHost() throws IOException {
        ObjectPath path = ObjectPath.createFromResponse(client().performRequest("GET", "_cluster/state", Collections.emptyMap()));
        String masterNodeId = path.evaluate("master_node");

        ObjectPath nodesPath = ObjectPath.createFromResponse(client().performRequest("GET", "_nodes", Collections.emptyMap()));
        String httpHost = nodesPath.evaluate("nodes." + masterNodeId + ".http.publish_address");
        return httpHost;
    }
}
