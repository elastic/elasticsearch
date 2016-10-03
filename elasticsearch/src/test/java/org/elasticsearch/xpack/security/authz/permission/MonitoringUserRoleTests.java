/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

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
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.security.authc.Authentication;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

/**
 * Tests for the builtin monitoring user
 */
public class MonitoringUserRoleTests extends ESTestCase {

    public void testCluster() {
        final TransportRequest request = new TransportRequest.Empty();
        final Authentication authentication = mock(Authentication.class);
        assertThat(MonitoringUserRole.INSTANCE.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(MonitoringUserRole.INSTANCE.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(MonitoringUserRole.INSTANCE.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(MonitoringUserRole.INSTANCE.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(MonitoringUserRole.INSTANCE.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(MonitoringUserRole.INSTANCE.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(MonitoringUserRole.INSTANCE.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
    }

    public void testRunAs() {
        assertThat(MonitoringUserRole.INSTANCE.runAs().isEmpty(), is(true));
    }

    public void testUnauthorizedIndices() {
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher(SearchAction.NAME).test("foo"), is(false));
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher(SearchAction.NAME).test(".reporting"), is(false));
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher(SearchAction.NAME).test(".kibana"), is(false));
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher("indices:foo").test(randomAsciiOfLengthBetween(8, 24)),
                is(false));
    }

    public void testMonitoringIndices() {
        testReadAccess(".monitoring-" + randomAsciiOfLength(randomIntBetween(0, 13)));
        testReadAccess(".marvel-es-" + randomAsciiOfLength(randomIntBetween(0, 13)));
    }

    private void testReadAccess(String index) {
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher("indices:foo").test(index), is(false));
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher("indices:bar").test(index), is(false));
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(index), is(false));
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(index), is(false));
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher(IndexAction.NAME).test(index), is(false));
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher(DeleteAction.NAME).test(index), is(false));
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(index), is(false));
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher(SearchAction.NAME).test(index), is(true));
        assertThat(MonitoringUserRole.INSTANCE.indices().allowedIndicesMatcher(GetAction.NAME).test(index), is(true));
    }
}
