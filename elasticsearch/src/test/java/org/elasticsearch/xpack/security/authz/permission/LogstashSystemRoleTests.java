/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import java.util.Arrays;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.security.authc.Authentication;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

/**
 * Tests for the logstash_system role
 */
public class LogstashSystemRoleTests extends ESTestCase {

    public void testCluster() {
        final TransportRequest request = new TransportRequest.Empty();
        final Authentication authentication = mock(Authentication.class);
        assertThat(LogstashSystemRole.INSTANCE.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(LogstashSystemRole.INSTANCE.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(LogstashSystemRole.INSTANCE.cluster().check(ClusterStatsAction.NAME, request, authentication), is(true));
        assertThat(LogstashSystemRole.INSTANCE.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(LogstashSystemRole.INSTANCE.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(LogstashSystemRole.INSTANCE.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(LogstashSystemRole.INSTANCE.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(true));
    }

    public void testRunAs() {
        assertThat(LogstashSystemRole.INSTANCE.runAs().isEmpty(), is(true));
    }

    public void testUnauthorizedIndices() {
        assertThat(LogstashSystemRole.INSTANCE.indices().allowedIndicesMatcher(IndexAction.NAME).test("foo"), is(false));
        assertThat(LogstashSystemRole.INSTANCE.indices().allowedIndicesMatcher(IndexAction.NAME).test(".reporting"), is(false));
        assertThat(LogstashSystemRole.INSTANCE.indices().allowedIndicesMatcher("indices:foo").test(randomAsciiOfLengthBetween(8, 24)),
                is(false));
    }
}
