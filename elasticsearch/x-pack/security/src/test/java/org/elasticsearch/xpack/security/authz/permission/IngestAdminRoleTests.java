/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.permission;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.ingest.DeletePipelineAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.security.authc.Authentication;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class IngestAdminRoleTests extends ESTestCase {

    public void testClusterPermissions() {
        final TransportRequest request = new TransportRequest.Empty();
        final Authentication authentication = mock(Authentication.class);
        assertThat(IngestAdminRole.INSTANCE.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(true));
        assertThat(IngestAdminRole.INSTANCE.cluster().check(GetIndexTemplatesAction.NAME, request, authentication), is(true));
        assertThat(IngestAdminRole.INSTANCE.cluster().check(DeleteIndexTemplateAction.NAME, request, authentication), is(true));
        assertThat(IngestAdminRole.INSTANCE.cluster().check(PutPipelineAction.NAME, request, authentication), is(true));
        assertThat(IngestAdminRole.INSTANCE.cluster().check(GetPipelineAction.NAME, request, authentication), is(true));
        assertThat(IngestAdminRole.INSTANCE.cluster().check(DeletePipelineAction.NAME, request, authentication), is(true));


        assertThat(IngestAdminRole.INSTANCE.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(IngestAdminRole.INSTANCE.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(IngestAdminRole.INSTANCE.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
    }

    public void testNoIndicesPermissions() {
        assertThat(IngestAdminRole.INSTANCE.indices().allowedIndicesMatcher(IndexAction.NAME).test("foo"), is(false));
        assertThat(IngestAdminRole.INSTANCE.indices().allowedIndicesMatcher("indices:foo").test(randomAsciiOfLengthBetween(8, 24)),
                is(false));
        assertThat(IngestAdminRole.INSTANCE.indices().allowedIndicesMatcher(GetAction.NAME).test(randomAsciiOfLengthBetween(8, 24)),
                is(false));
    }
}
