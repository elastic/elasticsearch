/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.security;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.elasticsearch.xpack.security.InternalClient;

import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class MonitoringInternalClientTests extends MonitoringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .build();
    }

    public void testAllowedAccess() {
        InternalClient internalClient = internalCluster().getInstance(InternalClient.class);

        assertAccessIsAllowed(internalClient.admin().cluster().prepareHealth());
        assertAccessIsAllowed(internalClient.admin().cluster().prepareClusterStats());
        assertAccessIsAllowed(internalClient.admin().cluster().prepareState());
        assertAccessIsAllowed(internalClient.admin().cluster().prepareNodesInfo());
        assertAccessIsAllowed(internalClient.admin().cluster().prepareNodesStats());
        assertAccessIsAllowed(internalClient.admin().cluster().prepareNodesHotThreads());

        assertAccessIsAllowed(internalClient.admin().indices().prepareGetSettings());
        assertAccessIsAllowed(internalClient.admin().indices().prepareSegments());
        assertAccessIsAllowed(internalClient.admin().indices().prepareRecoveries());
        assertAccessIsAllowed(internalClient.admin().indices().prepareStats());

        assertAccessIsAllowed(internalClient.admin().indices().prepareDelete(MONITORING_INDICES_PREFIX + "*"));
        assertAccessIsAllowed(internalClient.admin().indices().prepareCreate(MONITORING_INDICES_PREFIX + "test"));

        assertAccessIsAllowed(internalClient.admin().indices().preparePutTemplate("foo")
                .setSource(new BytesArray(randomTemplateSource()), XContentType.JSON));
        assertAccessIsAllowed(internalClient.admin().indices().prepareGetTemplates("foo"));
    }

    public void testAllowAllAccess() {
        InternalClient internalClient = internalCluster().getInstance(InternalClient.class);
        assertAcked(internalClient.admin().indices().preparePutTemplate("foo")
                .setSource(new BytesArray(randomTemplateSource()), XContentType.JSON).get());

        assertAccessIsAllowed(internalClient.admin().indices().prepareDeleteTemplate("foo"));
        assertAccessIsAllowed(internalClient.admin().cluster().prepareGetRepositories());
    }

    private static void assertAccessIsAllowed(ActionRequestBuilder request) {
        request.get();
    }

    /**
     * @return the source of a random monitoring template
     */
    private String randomTemplateSource() {
        return randomFrom(monitoringTemplates().stream().map(Tuple::v2).collect(Collectors.toList()));
    }
}

