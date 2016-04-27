/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.permission;

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
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.marvel.action.MonitoringBulkAction;
import org.elasticsearch.shield.authc.Authentication;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class KibanaUserRoleTests extends ESTestCase {

    public void testCluster() {
        final Authentication authentication = mock(Authentication.class);
        final TransportRequest request = new TransportRequest.Empty();
        assertThat(KibanaUserRole.INSTANCE.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(KibanaUserRole.INSTANCE.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(KibanaUserRole.INSTANCE.cluster().check(ClusterStatsAction.NAME, request, authentication), is(true));
        assertThat(KibanaUserRole.INSTANCE.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(KibanaUserRole.INSTANCE.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(KibanaUserRole.INSTANCE.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(KibanaUserRole.INSTANCE.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
    }

    public void testRunAs() {
        assertThat(KibanaUserRole.INSTANCE.runAs().isEmpty(), is(true));
    }

    public void testUnauthorizedIndices() {
        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher(IndexAction.NAME).test("foo"), is(false));
        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher(IndexAction.NAME).test(".reporting"), is(false));
        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher("indices:foo")
                .test(randomAsciiOfLengthBetween(8, 24)), is(false));
    }

    public void testKibanaIndices() {
        Arrays.asList(".kibana", ".kibana-devnull").forEach(this::testIndexAccess);
    }

    private void testIndexAccess(String index) {
        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher("indices:foo").test(index), is(false));
        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher("indices:bar").test(index), is(false));

        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher(DeleteAction.NAME).test(index), is(true));
        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(index), is(true));
        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(index), is(true));
        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher(IndexAction.NAME).test(index), is(true));
        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher(SearchAction.NAME).test(index), is(true));
        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(index), is(true));
        assertThat(KibanaUserRole.INSTANCE.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(index), is(true));
    }
}
