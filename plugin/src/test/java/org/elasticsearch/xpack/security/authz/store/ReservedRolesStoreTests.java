/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.ingest.DeletePipelineAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.security.action.user.PutUserAction;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;
import org.elasticsearch.xpack.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for the {@link ReservedRolesStore}
 */
public class ReservedRolesStoreTests extends ESTestCase {

    public void testIsReserved() {
        assertThat(ReservedRolesStore.isReserved("kibana_system"), is(true));
        assertThat(ReservedRolesStore.isReserved("superuser"), is(true));
        assertThat(ReservedRolesStore.isReserved("foobar"), is(false));
        assertThat(ReservedRolesStore.isReserved(SystemUser.ROLE_NAME), is(true));
        assertThat(ReservedRolesStore.isReserved("transport_client"), is(true));
        assertThat(ReservedRolesStore.isReserved("kibana_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("ingest_admin"), is(true));
        assertThat(ReservedRolesStore.isReserved("remote_monitoring_agent"), is(true));
        assertThat(ReservedRolesStore.isReserved("monitoring_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("reporting_user"), is(true));
    }

    public void testIngestAdminRole() {
        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("ingest_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role ingestAdminRole = Role.builder(roleDescriptor, null).build();
        assertThat(ingestAdminRole.cluster().check(PutIndexTemplateAction.NAME), is(true));
        assertThat(ingestAdminRole.cluster().check(GetIndexTemplatesAction.NAME), is(true));
        assertThat(ingestAdminRole.cluster().check(DeleteIndexTemplateAction.NAME), is(true));
        assertThat(ingestAdminRole.cluster().check(PutPipelineAction.NAME), is(true));
        assertThat(ingestAdminRole.cluster().check(GetPipelineAction.NAME), is(true));
        assertThat(ingestAdminRole.cluster().check(DeletePipelineAction.NAME), is(true));

        assertThat(ingestAdminRole.cluster().check(ClusterRerouteAction.NAME), is(false));
        assertThat(ingestAdminRole.cluster().check(ClusterUpdateSettingsAction.NAME), is(false));
        assertThat(ingestAdminRole.cluster().check(MonitoringBulkAction.NAME), is(false));

        assertThat(ingestAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test("foo"), is(false));
        assertThat(ingestAdminRole.indices().allowedIndicesMatcher("indices:foo").test(randomAsciiOfLengthBetween(8, 24)),
                is(false));
        assertThat(ingestAdminRole.indices().allowedIndicesMatcher(GetAction.NAME).test(randomAsciiOfLengthBetween(8, 24)),
                is(false));
    }

    public void testKibanaRole() {
        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("kibana_system");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role kibanaRole = Role.builder(roleDescriptor, null).build();
        assertThat(kibanaRole.cluster().check(ClusterHealthAction.NAME), is(true));
        assertThat(kibanaRole.cluster().check(ClusterStateAction.NAME), is(true));
        assertThat(kibanaRole.cluster().check(ClusterStatsAction.NAME), is(true));
        assertThat(kibanaRole.cluster().check(PutIndexTemplateAction.NAME), is(false));
        assertThat(kibanaRole.cluster().check(ClusterRerouteAction.NAME), is(false));
        assertThat(kibanaRole.cluster().check(ClusterUpdateSettingsAction.NAME), is(false));
        assertThat(kibanaRole.cluster().check(MonitoringBulkAction.NAME), is(true));

        assertThat(kibanaRole.runAs().check(randomAsciiOfLengthBetween(1, 12)), is(false));

        assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test("foo"), is(false));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(".reporting"), is(false));
        assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(randomAsciiOfLengthBetween(8, 24)), is(false));

        Arrays.asList(".kibana", ".kibana-devnull", ".reporting-" + randomAsciiOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            logger.info("index name [{}]", index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(index), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(index), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(index), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(index), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(index), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(index), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(index), is(true));
        });
    }

    public void testKibanaUserRole() {
        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("kibana_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role kibanaUserRole = Role.builder(roleDescriptor, null).build();
        assertThat(kibanaUserRole.cluster().check(ClusterHealthAction.NAME), is(true));
        assertThat(kibanaUserRole.cluster().check(ClusterStateAction.NAME), is(true));
        assertThat(kibanaUserRole.cluster().check(ClusterStatsAction.NAME), is(true));
        assertThat(kibanaUserRole.cluster().check(PutIndexTemplateAction.NAME), is(false));
        assertThat(kibanaUserRole.cluster().check(ClusterRerouteAction.NAME), is(false));
        assertThat(kibanaUserRole.cluster().check(ClusterUpdateSettingsAction.NAME), is(false));
        assertThat(kibanaUserRole.cluster().check(MonitoringBulkAction.NAME), is(false));

        assertThat(kibanaUserRole.runAs().check(randomAsciiOfLengthBetween(1, 12)), is(false));

        assertThat(kibanaUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test("foo"), is(false));
        assertThat(kibanaUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(".reporting"), is(false));
        assertThat(kibanaUserRole.indices().allowedIndicesMatcher("indices:foo")
                .test(randomAsciiOfLengthBetween(8, 24)), is(false));

        Arrays.asList(".kibana", ".kibana-devnull").forEach((index) -> {
            logger.info("index name [{}]", index);
            assertThat(kibanaUserRole.indices().allowedIndicesMatcher("indices:foo").test(index), is(false));
            assertThat(kibanaUserRole.indices().allowedIndicesMatcher("indices:bar").test(index), is(false));

            assertThat(kibanaUserRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(index), is(true));
            assertThat(kibanaUserRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(index), is(true));
            assertThat(kibanaUserRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(index), is(true));
            assertThat(kibanaUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(index), is(true));
            assertThat(kibanaUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(index), is(true));
            assertThat(kibanaUserRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(index), is(true));
            assertThat(kibanaUserRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(index), is(true));
        });
    }

    public void testMonitoringUserRole() {
        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("monitoring_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role monitoringUserRole = Role.builder(roleDescriptor, null).build();
        assertThat(monitoringUserRole.cluster().check(ClusterHealthAction.NAME), is(false));
        assertThat(monitoringUserRole.cluster().check(ClusterStateAction.NAME), is(false));
        assertThat(monitoringUserRole.cluster().check(ClusterStatsAction.NAME), is(false));
        assertThat(monitoringUserRole.cluster().check(PutIndexTemplateAction.NAME), is(false));
        assertThat(monitoringUserRole.cluster().check(ClusterRerouteAction.NAME), is(false));
        assertThat(monitoringUserRole.cluster().check(ClusterUpdateSettingsAction.NAME), is(false));
        assertThat(monitoringUserRole.cluster().check(MonitoringBulkAction.NAME), is(false));

        assertThat(monitoringUserRole.runAs().check(randomAsciiOfLengthBetween(1, 12)), is(false));

        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test("foo"), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(".reporting"), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(".kibana"), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher("indices:foo").test(randomAsciiOfLengthBetween(8, 24)),
                is(false));

        Arrays.asList(".monitoring-" + randomAsciiOfLength(randomIntBetween(0, 13)),
                ".marvel-es-" + randomAsciiOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            assertThat(monitoringUserRole.indices().allowedIndicesMatcher("indices:foo").test(index), is(false));
            assertThat(monitoringUserRole.indices().allowedIndicesMatcher("indices:bar").test(index), is(false));
            assertThat(monitoringUserRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(index), is(false));
            assertThat(monitoringUserRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(index), is(false));
            assertThat(monitoringUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(index), is(false));
            assertThat(monitoringUserRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(index), is(false));
            assertThat(monitoringUserRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(index), is(false));
            assertThat(monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(index), is(true));
            assertThat(monitoringUserRole.indices().allowedIndicesMatcher(GetAction.NAME).test(index), is(true));
        });
    }

    public void testRemoteMonitoringAgentRole() {
        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("remote_monitoring_agent");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role remoteMonitoringAgentRole = Role.builder(roleDescriptor, null).build();
        assertThat(remoteMonitoringAgentRole.cluster().check(ClusterHealthAction.NAME), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(ClusterStateAction.NAME), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(ClusterStatsAction.NAME), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(PutIndexTemplateAction.NAME), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(ClusterRerouteAction.NAME), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(ClusterUpdateSettingsAction.NAME),
                is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(MonitoringBulkAction.NAME), is(false));

        assertThat(remoteMonitoringAgentRole.runAs().check(randomAsciiOfLengthBetween(1, 12)), is(false));

        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test("foo"), is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(".reporting"), is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(".kibana"), is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:foo")
                .test(randomAsciiOfLengthBetween(8, 24)), is(false));

        Arrays.asList(".monitoring-" + randomAsciiOfLength(randomIntBetween(0, 13)),
                ".marvel-es-" + randomAsciiOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:foo").test(index), is(true));
            assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:bar").test(index), is(true));
            assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(index), is(true));
            assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(index), is(true));
            assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(index), is(true));
            assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(index), is(true));
            assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(index), is(true));
            assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(index), is(true));
            assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetAction.NAME).test(index), is(true));
            assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(index), is(true));
        });
    }

    public void testReportingUserRole() {
        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("reporting_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role reportingUserRole = Role.builder(roleDescriptor, null).build();
        assertThat(reportingUserRole.cluster().check(ClusterHealthAction.NAME), is(false));
        assertThat(reportingUserRole.cluster().check(ClusterStateAction.NAME), is(false));
        assertThat(reportingUserRole.cluster().check(ClusterStatsAction.NAME), is(false));
        assertThat(reportingUserRole.cluster().check(PutIndexTemplateAction.NAME), is(false));
        assertThat(reportingUserRole.cluster().check(ClusterRerouteAction.NAME), is(false));
        assertThat(reportingUserRole.cluster().check(ClusterUpdateSettingsAction.NAME), is(false));
        assertThat(reportingUserRole.cluster().check(MonitoringBulkAction.NAME), is(false));

        assertThat(reportingUserRole.runAs().check(randomAsciiOfLengthBetween(1, 12)), is(false));

        assertThat(reportingUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test("foo"), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(".reporting"), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(".kibana"), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher("indices:foo").test(randomAsciiOfLengthBetween(8, 24)),
                is(false));

        final String index = ".reporting-" + randomAsciiOfLength(randomIntBetween(0, 13));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher("indices:foo").test(index), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher("indices:bar").test(index), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(index), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(index), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(index), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(index), is(true));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(GetAction.NAME).test(index), is(true));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(index), is(true));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(UpdateAction.NAME).test(index), is(true));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(index), is(true));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(index), is(true));
    }

    public void testSuperuserRole() {
        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("superuser");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role superuserRole = Role.builder(roleDescriptor, null).build();
        assertThat(superuserRole.cluster().check(ClusterHealthAction.NAME), is(true));
        assertThat(superuserRole.cluster().check(ClusterUpdateSettingsAction.NAME), is(true));
        assertThat(superuserRole.cluster().check(PutUserAction.NAME), is(true));
        assertThat(superuserRole.cluster().check(PutRoleAction.NAME), is(true));
        assertThat(superuserRole.cluster().check(PutIndexTemplateAction.NAME), is(true));
        assertThat(superuserRole.cluster().check("internal:admin/foo"), is(false));

        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final MetaData metaData = new MetaData.Builder()
                .put(new IndexMetaData.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("b")
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetaData.Builder("ab").build())
                        .putAlias(new AliasMetaData.Builder("ba").build())
                        .build(), true)
                .build();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        Map<String, IndexAccessControl> authzMap =
                superuserRole.indices().authorize(SearchAction.NAME, Sets.newHashSet("a1", "ba"), metaData, fieldPermissionsCache);
        assertThat(authzMap.get("a1").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
        authzMap = superuserRole.indices().authorize(DeleteIndexAction.NAME, Sets.newHashSet("a1", "ba"), metaData, fieldPermissionsCache);
        assertThat(authzMap.get("a1").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
        authzMap = superuserRole.indices().authorize(IndexAction.NAME, Sets.newHashSet("a2", "ba"), metaData, fieldPermissionsCache);
        assertThat(authzMap.get("a2").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
        authzMap = superuserRole.indices()
                .authorize(UpdateSettingsAction.NAME, Sets.newHashSet("aaaaaa", "ba"), metaData, fieldPermissionsCache);
        assertThat(authzMap.get("aaaaaa").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
        assertTrue(superuserRole.indices().check(SearchAction.NAME));
        assertFalse(superuserRole.indices().check("unknown"));

        assertThat(superuserRole.runAs().check(randomAsciiOfLengthBetween(1, 30)), is(true));
    }

    public void testLogstashSystemRole() {
        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("logstash_system");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role logstashSystemRole = Role.builder(roleDescriptor, null).build();
        assertThat(logstashSystemRole.cluster().check(ClusterHealthAction.NAME), is(true));
        assertThat(logstashSystemRole.cluster().check(ClusterStateAction.NAME), is(true));
        assertThat(logstashSystemRole.cluster().check(ClusterStatsAction.NAME), is(true));
        assertThat(logstashSystemRole.cluster().check(PutIndexTemplateAction.NAME), is(false));
        assertThat(logstashSystemRole.cluster().check(ClusterRerouteAction.NAME), is(false));
        assertThat(logstashSystemRole.cluster().check(ClusterUpdateSettingsAction.NAME), is(false));
        assertThat(logstashSystemRole.cluster().check(MonitoringBulkAction.NAME), is(true));

        assertThat(logstashSystemRole.runAs().check(randomAsciiOfLengthBetween(1, 30)), is(false));

        assertThat(logstashSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test("foo"), is(false));
        assertThat(logstashSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(".reporting"), is(false));
        assertThat(logstashSystemRole.indices().allowedIndicesMatcher("indices:foo").test(randomAsciiOfLengthBetween(8, 24)),
                is(false));
    }

    public void testBeatsSystemRole() {
        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("beats_system");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role beatsSystemRole = Role.builder(roleDescriptor, null).build();
        assertThat(beatsSystemRole.cluster().check(ClusterHealthAction.NAME), is(true));
        assertThat(beatsSystemRole.cluster().check(ClusterStateAction.NAME), is(true));
        assertThat(beatsSystemRole.cluster().check(ClusterStatsAction.NAME), is(true));
        assertThat(beatsSystemRole.cluster().check(PutIndexTemplateAction.NAME), is(false));
        assertThat(beatsSystemRole.cluster().check(ClusterRerouteAction.NAME), is(false));
        assertThat(beatsSystemRole.cluster().check(ClusterUpdateSettingsAction.NAME), is(false));
        assertThat(beatsSystemRole.cluster().check(MonitoringBulkAction.NAME), is(true));

        assertThat(beatsSystemRole.runAs().check(randomAsciiOfLengthBetween(1, 30)), is(false));

        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test("foo"), is(false));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(".reporting"), is(false));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher("indices:foo").test(randomAsciiOfLengthBetween(8, 24)),
                is(false));
    }
}
