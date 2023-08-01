/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;

import java.io.IOException;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.WatcherRestTestCase.deleteAllWatcherData;

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
        "elasticsearch_nodes", };

    @After
    public void cleanExporters() throws Exception {
        Request cleanupSettingsRequest = new Request("PUT", "/_cluster/settings");
        cleanupSettingsRequest.setJsonEntity(
            Strings.toString(
                jsonBuilder().startObject().startObject("persistent").nullField("xpack.monitoring.exporters.*").endObject().endObject()
            )
        );
        adminClient().performRequest(cleanupSettingsRequest);
        deleteAllWatcherData();
    }

    public String getHttpHost() throws IOException {
        ObjectPath path = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_cluster/state")));
        String masterNodeId = path.evaluate("master_node");

        ObjectPath nodesPath = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        String httpHost = nodesPath.evaluate("nodes." + masterNodeId + ".http.publish_address");
        return httpHost;
    }
}
