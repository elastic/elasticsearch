/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
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
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.action.XPackInfoAction;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.core.ilm.action.StartILMAction;
import org.elasticsearch.xpack.core.ilm.action.StopILMAction;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarEventAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.EstimateModelMemoryAction;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureAction;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.action.InternalInferModelAction;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.core.ml.action.ValidateDetectorAction;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.user.APMSystemUser;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.BeatsSystemUser;
import org.elasticsearch.xpack.core.security.user.LogstashSystemUser;
import org.elasticsearch.xpack.core.security.user.RemoteMonitoringUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.delete.DeleteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsAction;
import org.elasticsearch.xpack.core.watcher.watch.Watch;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link ReservedRolesStore}
 */
public class ReservedRolesStoreTests extends ESTestCase {

    private static final String READ_CROSS_CLUSTER_NAME = "internal:transport/proxy/indices:data/read/query";

    public void testIsReserved() {
        assertThat(ReservedRolesStore.isReserved("kibana_system"), is(true));
        assertThat(ReservedRolesStore.isReserved("superuser"), is(true));
        assertThat(ReservedRolesStore.isReserved("foobar"), is(false));
        assertThat(ReservedRolesStore.isReserved(SystemUser.ROLE_NAME), is(true));
        assertThat(ReservedRolesStore.isReserved("transport_client"), is(true));
        assertThat(ReservedRolesStore.isReserved("kibana_admin"), is(true));
        assertThat(ReservedRolesStore.isReserved("kibana_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("ingest_admin"), is(true));
        assertThat(ReservedRolesStore.isReserved("monitoring_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("reporting_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("machine_learning_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("machine_learning_admin"), is(true));
        assertThat(ReservedRolesStore.isReserved("data_frame_transforms_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("data_frame_transforms_admin"), is(true));
        assertThat(ReservedRolesStore.isReserved("transform_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("transform_admin"), is(true));
        assertThat(ReservedRolesStore.isReserved("watcher_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("watcher_admin"), is(true));
        assertThat(ReservedRolesStore.isReserved("beats_admin"), is(true));
        assertThat(ReservedRolesStore.isReserved(XPackUser.ROLE_NAME), is(true));
        assertThat(ReservedRolesStore.isReserved(AsyncSearchUser.ROLE_NAME), is(true));
        assertThat(ReservedRolesStore.isReserved(LogstashSystemUser.ROLE_NAME), is(true));
        assertThat(ReservedRolesStore.isReserved(BeatsSystemUser.ROLE_NAME), is(true));
        assertThat(ReservedRolesStore.isReserved(APMSystemUser.ROLE_NAME), is(true));
        assertThat(ReservedRolesStore.isReserved(RemoteMonitoringUser.COLLECTION_ROLE_NAME), is(true));
        assertThat(ReservedRolesStore.isReserved(RemoteMonitoringUser.INDEXING_ROLE_NAME), is(true));
        assertThat(ReservedRolesStore.isReserved("snapshot_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("code_admin"), is(false));
        assertThat(ReservedRolesStore.isReserved("code_user"), is(false));
        assertThat(ReservedRolesStore.isReserved("viewer"), is(true));
        assertThat(ReservedRolesStore.isReserved("editor"), is(true));
    }

    public void testSnapshotUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("snapshot_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role snapshotUserRole = Role.builder(roleDescriptor, null).build();
        assertThat(snapshotUserRole.cluster().check(GetRepositoriesAction.NAME, request, authentication), is(true));
        assertThat(snapshotUserRole.cluster().check(CreateSnapshotAction.NAME, request, authentication), is(true));
        assertThat(snapshotUserRole.cluster().check(SnapshotsStatusAction.NAME, request, authentication), is(true));
        assertThat(snapshotUserRole.cluster().check(GetSnapshotsAction.NAME, request, authentication), is(true));

        assertThat(snapshotUserRole.cluster().check(PutRepositoryAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(GetIndexTemplatesAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(DeleteIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(PutPipelineAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(GetPipelineAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(DeletePipelineAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(GetWatchAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(PutWatchAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(DeleteWatchAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(ExecuteWatchAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(AckWatchAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(ActivateWatchAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(WatcherServiceAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(snapshotUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(
                mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));
        assertThat(snapshotUserRole.indices().allowedIndicesMatcher("indices:foo").test(
                mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));
        assertThat(snapshotUserRole.indices().allowedIndicesMatcher(GetAction.NAME).test(
                mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));
        assertThat(snapshotUserRole.indices().allowedIndicesMatcher(GetAction.NAME).test(
                mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));

        assertThat(snapshotUserRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(
                mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(true));

        for (String index : RestrictedIndicesNames.RESTRICTED_NAMES) {
            // This test might cease to be true if we ever have non-security restricted names
            // but that depends on how users are supposed to perform snapshots of those new indices.
            assertThat(snapshotUserRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(
                    mockIndexAbstraction(index)), is(true));
        }
        assertThat(snapshotUserRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(
                mockIndexAbstraction(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2))), is(true));

        assertNoAccessAllowed(snapshotUserRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(snapshotUserRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testIngestAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("ingest_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role ingestAdminRole = Role.builder(roleDescriptor, null).build();
        assertThat(ingestAdminRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(GetIndexTemplatesAction.NAME, request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(DeleteIndexTemplateAction.NAME, request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(PutPipelineAction.NAME, request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(GetPipelineAction.NAME, request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(DeletePipelineAction.NAME, request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(ingestAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(ingestAdminRole.indices().allowedIndicesMatcher("indices:foo").test(
                mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));
        assertThat(ingestAdminRole.indices().allowedIndicesMatcher(GetAction.NAME).test(
                mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));

        assertNoAccessAllowed(ingestAdminRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(ingestAdminRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testKibanaSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("kibana_system");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role kibanaRole = Role.builder(roleDescriptor, null).build();
        assertThat(kibanaRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(GetIndexTemplatesAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(kibanaRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(kibanaRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(true));

        // ILM
        assertThat(kibanaRole.cluster().check(GetLifecycleAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(PutLifecycleAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(DeleteLifecycleAction.NAME, request, authentication), is(false));
        assertThat(kibanaRole.cluster().check(StartILMAction.NAME, request, authentication), is(false));
        assertThat(kibanaRole.cluster().check(StopILMAction.NAME, request, authentication), is(false));

        // SAML and token
        assertThat(kibanaRole.cluster().check(SamlPrepareAuthenticationAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(SamlAuthenticateAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(InvalidateTokenAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(CreateTokenAction.NAME, request, authentication), is(true));

        // API keys
        assertThat(kibanaRole.cluster().check(InvalidateApiKeyAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(GrantApiKeyAction.NAME, request, authentication), is(true));

        // ML
        assertRoleHasManageMl(kibanaRole);

        // Text Structure
        assertThat(kibanaRole.cluster().check(FindStructureAction.NAME, request, authentication), is(true));

        // Application Privileges
        DeletePrivilegesRequest deleteKibanaPrivileges = new DeletePrivilegesRequest("kibana-.kibana", new String[]{ "all", "read" });
        DeletePrivilegesRequest deleteLogstashPrivileges = new DeletePrivilegesRequest("logstash", new String[]{ "all", "read" });
        assertThat(kibanaRole.cluster().check(DeletePrivilegesAction.NAME, deleteKibanaPrivileges, authentication), is(true));
        assertThat(kibanaRole.cluster().check(DeletePrivilegesAction.NAME, deleteLogstashPrivileges, authentication), is(false));

        GetPrivilegesRequest getKibanaPrivileges = new GetPrivilegesRequest();
        getKibanaPrivileges.application("kibana-.kibana-sales");
        GetPrivilegesRequest getApmPrivileges = new GetPrivilegesRequest();
        getApmPrivileges.application("apm");
        assertThat(kibanaRole.cluster().check(GetPrivilegesAction.NAME, getKibanaPrivileges, authentication), is(true));
        assertThat(kibanaRole.cluster().check(GetPrivilegesAction.NAME, getApmPrivileges, authentication), is(false));

        PutPrivilegesRequest putKibanaPrivileges = new PutPrivilegesRequest();
        putKibanaPrivileges.setPrivileges(Collections.singletonList(new ApplicationPrivilegeDescriptor(
            "kibana-.kibana-" + randomAlphaOfLengthBetween(2,6), "all", Collections.emptySet(), Collections.emptyMap())));
        PutPrivilegesRequest putSwiftypePrivileges = new PutPrivilegesRequest();
        putSwiftypePrivileges.setPrivileges(Collections.singletonList(new ApplicationPrivilegeDescriptor(
            "swiftype-kibana" , "all", Collections.emptySet(), Collections.emptyMap())));
        assertThat(kibanaRole.cluster().check(PutPrivilegesAction.NAME, putKibanaPrivileges, authentication), is(true));
        assertThat(kibanaRole.cluster().check(PutPrivilegesAction.NAME, putSwiftypePrivileges, authentication), is(false));

        assertThat(kibanaRole.cluster().check(GetBuiltinPrivilegesAction.NAME, request, authentication), is(true));

        // Everything else
        assertThat(kibanaRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));
        assertThat(kibanaRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(true));

        assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
                is(false));

        Arrays.asList(
            ".kibana",
            ".kibana-devnull",
            ".reporting-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            ".apm-agent-configuration",
            ".apm-custom-link",
            ReservedRolesStore.LEGACY_ALERTS_INDEX + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.ALERTS_INDEX + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((index) -> {
            logger.info("index name [{}]", index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
            // inherits from 'all'
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(true));
        });

        // read-only index access, including cross cluster
        Arrays.asList(".monitoring-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            logger.info("index name [{}]", index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(true));
        });

        // read-only index access, excluding cross cluster
        Arrays.asList(
            ".ml-anomalies-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            ".ml-stats-" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((index) -> {
            logger.trace("index name [{}]", index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(false));
        });

        // read/write index access, excluding cross cluster
        Arrays.asList(
            ".ml-annotations-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            ".ml-notifications-" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((index) -> {
            logger.trace("index name [{}]", index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(false));
        });

        // read-only indices for APM telemetry
        Arrays.asList("apm-*").forEach((index) -> {
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(true));
        });

        // read-only indices for Endpoint diagnostic information
        Arrays.asList(".logs-endpoint.diagnostic.collection-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(false));
        });

        Arrays.asList(
            ".fleet",
            ".fleet-agents",
            ".fleet-actions",
            ".fleet-enrollment-api-keys",
            ".fleet-policies",
            ".fleet-actions-results",
            ".fleet-servers"
        ).forEach((index) -> {
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(true));
        });


        // Data telemetry reads mappings, metadata and stats of indices
        Arrays.asList(randomAlphaOfLengthBetween(8, 24), "packetbeat-*", "logs-*").forEach((index) -> {
            logger.info("index name [{}]", index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetMappingsAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndicesStatsAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(false));
        });

        // read-only datastream for Endpoint policy responses
        Arrays.asList("metrics-endpoint.policy-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(false));
        });

        // read-only datastream for Endpoint metrics
        Arrays.asList("metrics-endpoint.metrics-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(false));
        });

        // Beats management index
        final String index = ".management-beats";
        assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
        assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(false));

        assertNoAccessAllowed(kibanaRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(kibanaRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testKibanaAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("kibana_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
        assertThat(roleDescriptor.getMetadata(), not(hasEntry("_deprecated", true)));

        Role kibanaAdminRole = Role.builder(roleDescriptor, null).build();
        assertThat(kibanaAdminRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication),
                is(false));
        assertThat(kibanaAdminRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication),
                is(false));

        assertThat(kibanaAdminRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        assertThat(kibanaAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(kibanaAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(kibanaAdminRole.indices().allowedIndicesMatcher("indices:foo").test(
                mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));

        final String randomApplication = "kibana-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(kibanaAdminRole.application().grants(new ApplicationPrivilege(randomApplication, "app-random", "all"),
                "*"), is(false));

        final String application = "kibana-.kibana";
        assertThat(kibanaAdminRole.application().grants(new ApplicationPrivilege(application, "app-foo", "foo"), "*"),
                is(false));
        assertThat(kibanaAdminRole.application().grants(new ApplicationPrivilege(application, "app-all", "all"), "*"),
                is(true));

        final String applicationWithRandomIndex = "kibana-.kibana_" + randomAlphaOfLengthBetween(8, 24);
        assertThat(
                kibanaAdminRole.application()
                        .grants(new ApplicationPrivilege(applicationWithRandomIndex, "app-random-index", "all"), "*"),
                is(false));

        assertNoAccessAllowed(kibanaAdminRole, RestrictedIndicesNames.RESTRICTED_NAMES);
    }

    public void testKibanaUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("kibana_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
        assertThat(roleDescriptor.getMetadata(), hasEntry("_deprecated", true));

        Role kibanaUserRole = Role.builder(roleDescriptor, null).build();
        assertThat(kibanaUserRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(kibanaUserRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        assertThat(kibanaUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(kibanaUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(kibanaUserRole.indices().allowedIndicesMatcher("indices:foo")
                .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));

        final String randomApplication = "kibana-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(kibanaUserRole.application().grants(new ApplicationPrivilege(randomApplication, "app-random", "all"), "*"), is(false));

        final String application = "kibana-.kibana";
        assertThat(kibanaUserRole.application().grants(new ApplicationPrivilege(application, "app-foo", "foo"), "*"), is(false));
        assertThat(kibanaUserRole.application().grants(new ApplicationPrivilege(application, "app-all", "all"), "*"), is(true));

        final String applicationWithRandomIndex = "kibana-.kibana_" + randomAlphaOfLengthBetween(8, 24);
        assertThat(kibanaUserRole.application().grants(new ApplicationPrivilege(applicationWithRandomIndex, "app-random-index", "all"),
            "*"), is(false));

        assertNoAccessAllowed(kibanaUserRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(kibanaUserRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testMonitoringUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("monitoring_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role monitoringUserRole = Role.builder(roleDescriptor, null).build();
        assertThat(monitoringUserRole.cluster().check(MainAction.NAME, request, authentication), is(true));
        assertThat(monitoringUserRole.cluster().check(XPackInfoAction.NAME, request, authentication), is(true));
        assertThat(monitoringUserRole.cluster().check(RemoteInfoAction.NAME, request, authentication), is(true));
        assertThat(monitoringUserRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(monitoringUserRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".reporting")),
                is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".kibana")),
                is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher("indices:foo").test(
                mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction("foo")),
                is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(".reporting")),
                is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(".kibana")),
                is(false));

        final String index = ".monitoring-" + randomAlphaOfLength(randomIntBetween(0, 13));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME)
                .test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(true));

        assertNoAccessAllowed(monitoringUserRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(monitoringUserRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));

        final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
        assertThat(monitoringUserRole.application().grants(
            new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"), is(false));
        assertThat(monitoringUserRole.application().grants(
            new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-reserved_monitoring", "reserved_monitoring"), "*"), is(true));

        final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(monitoringUserRole.application().grants(
            new ApplicationPrivilege(otherApplication, "app-foo", "foo"), "*"), is(false));
        assertThat(monitoringUserRole.application().grants(
            new ApplicationPrivilege(otherApplication, "app-reserved_monitoring", "reserved_monitoring"), "*"), is(false));
    }

    public void testRemoteMonitoringAgentRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("remote_monitoring_agent");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role remoteMonitoringAgentRole = Role.builder(roleDescriptor, null).build();
        assertThat(remoteMonitoringAgentRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(GetWatchAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(PutWatchAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(DeleteWatchAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(ExecuteWatchAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(AckWatchAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(ActivateWatchAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(WatcherServiceAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        // ILM
        assertThat(remoteMonitoringAgentRole.cluster().check(GetLifecycleAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(PutLifecycleAction.NAME, request, authentication), is(true));

        // we get this from the cluster:monitor privilege
        assertThat(remoteMonitoringAgentRole.cluster().check(WatcherStatsAction.NAME, request, authentication), is(true));

        assertThat(remoteMonitoringAgentRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("foo")),
                is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".reporting")),
                is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".kibana")),
                is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:foo")
                .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));

        final String monitoringIndex = ".monitoring-" + randomAlphaOfLength(randomIntBetween(0, 13));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(monitoringIndex)),
                is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(monitoringIndex)),
                is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME)
                .test(mockIndexAbstraction(monitoringIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME)
                .test(mockIndexAbstraction(monitoringIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(IndexAction.NAME)
                .test(mockIndexAbstraction(monitoringIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(DeleteAction.NAME)
                .test(mockIndexAbstraction(monitoringIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME)
                .test(mockIndexAbstraction(monitoringIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME)
                .test(mockIndexAbstraction(monitoringIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetAction.NAME)
                .test(mockIndexAbstraction(monitoringIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetIndexAction.NAME)
                .test(mockIndexAbstraction(monitoringIndex)), is(true));

        final String metricbeatIndex = "metricbeat-" + randomAlphaOfLength(randomIntBetween(0, 13));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:foo")
                .test(mockIndexAbstraction(metricbeatIndex)), is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:bar")
                .test(mockIndexAbstraction(metricbeatIndex)), is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(IndexAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetIndexAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetAliasesAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(IndicesAliasesAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(true));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(IndicesSegmentsAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(RemoveIndexLifecyclePolicyAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(DeleteAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(false));
        assertThat(remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)), is(false));

        assertNoAccessAllowed(remoteMonitoringAgentRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(remoteMonitoringAgentRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testRemoteMonitoringCollectorRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("remote_monitoring_collector");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role remoteMonitoringCollectorRole = Role.builder(roleDescriptor, null).build();
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringCollectorRole.cluster().check(GetIndexTemplatesAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(DeleteIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(remoteMonitoringCollectorRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(RecoveryAction.NAME)
                .test(mockIndexAbstraction("foo")), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(SearchAction.NAME)
                .test(mockIndexAbstraction("foo")), is(false));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(SearchAction.NAME)
                .test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(SearchAction.NAME)
                .test(mockIndexAbstraction(".kibana")), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetAction.NAME)
                .test(mockIndexAbstraction(".kibana")), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher("indices:foo")
            .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));

        Arrays.asList(
            ".monitoring-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "metricbeat-" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((index) -> {
            logger.info("index name [{}]", index);
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher("indices:foo")
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher("indices:bar")
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME)
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME)
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(IndexAction.NAME)
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetIndexAction.NAME)
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetAliasesAction.NAME)
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(RemoveIndexLifecyclePolicyAction.NAME)
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(DeleteAction.NAME)
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME)
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(SearchAction.NAME)
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetAction.NAME)
                    .test(mockIndexAbstraction(index)), is(false));
            assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetIndexAction.NAME)
                    .test(mockIndexAbstraction(index)), is(false));
        });

        // These tests might need to change if we add new non-security restricted indices that the monitoring user isn't supposed to see
        // (but ideally, the monitoring user should see all indices).
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetSettingsAction.NAME)
                .test(mockIndexAbstraction(randomFrom(RestrictedIndicesNames.RESTRICTED_NAMES))), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetSettingsAction.NAME)
                .test(mockIndexAbstraction(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2))), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(IndicesShardStoresAction.NAME)
                .test(mockIndexAbstraction(randomFrom(RestrictedIndicesNames.RESTRICTED_NAMES))), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(IndicesShardStoresAction.NAME)
                .test(mockIndexAbstraction(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2))), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(RecoveryAction.NAME)
                .test(mockIndexAbstraction(randomFrom(RestrictedIndicesNames.RESTRICTED_NAMES))), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(RecoveryAction.NAME)
                .test(mockIndexAbstraction(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2))), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(IndicesStatsAction.NAME)
                .test(mockIndexAbstraction(randomFrom(RestrictedIndicesNames.RESTRICTED_NAMES))), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(IndicesStatsAction.NAME)
                .test(mockIndexAbstraction(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2))), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(IndicesSegmentsAction.NAME)
                .test(mockIndexAbstraction(randomFrom(RestrictedIndicesNames.RESTRICTED_NAMES))), is(true));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(IndicesSegmentsAction.NAME)
                .test(mockIndexAbstraction(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2))), is(true));

        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(SearchAction.NAME)
                .test(mockIndexAbstraction(randomFrom(RestrictedIndicesNames.RESTRICTED_NAMES))), is(false));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(SearchAction.NAME)
                .test(mockIndexAbstraction(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2))), is(false));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetAction.NAME)
                .test(mockIndexAbstraction(randomFrom(RestrictedIndicesNames.RESTRICTED_NAMES))), is(false));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetAction.NAME)
                .test(mockIndexAbstraction(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2))), is(false));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(DeleteAction.NAME)
                .test(mockIndexAbstraction(randomFrom(RestrictedIndicesNames.RESTRICTED_NAMES))), is(false));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(DeleteAction.NAME)
                .test(mockIndexAbstraction(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2))), is(false));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(IndexAction.NAME)
                .test(mockIndexAbstraction(randomFrom(RestrictedIndicesNames.RESTRICTED_NAMES))), is(false));
        assertThat(remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(IndexAction.NAME)
                .test(mockIndexAbstraction(RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2))), is(false));

        assertMonitoringOnRestrictedIndices(remoteMonitoringCollectorRole);

        assertNoAccessAllowed(remoteMonitoringCollectorRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(remoteMonitoringCollectorRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    private void assertMonitoringOnRestrictedIndices(Role role) {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6,
            RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);
        final Metadata metadata = new Metadata.Builder()
                .put(new IndexMetadata.Builder(internalSecurityIndex)
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetadata.Builder(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).build())
                        .build(), true)
                .build();
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        final List<String> indexMonitoringActionNamesList = Arrays.asList(IndicesStatsAction.NAME, IndicesSegmentsAction.NAME,
                GetSettingsAction.NAME, IndicesShardStoresAction.NAME, RecoveryAction.NAME);
        for (final String indexMonitoringActionName : indexMonitoringActionNamesList) {
            String asyncSearchIndex = RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2);
            final Map<String, IndexAccessControl> authzMap = role.indices().authorize(indexMonitoringActionName,
                Sets.newHashSet(internalSecurityIndex, RestrictedIndicesNames.SECURITY_MAIN_ALIAS, asyncSearchIndex),
                metadata.getIndicesLookup(), fieldPermissionsCache);
            assertThat(authzMap.get(internalSecurityIndex).isGranted(), is(true));
            assertThat(authzMap.get(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).isGranted(), is(true));
            assertThat(authzMap.get(asyncSearchIndex).isGranted(), is(true));
        }
    }

    public void testReportingUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("reporting_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
        assertThat(roleDescriptor.getMetadata(), hasEntry("_deprecated", true));

        Role reportingUserRole = Role.builder(roleDescriptor, null).build();
        assertThat(reportingUserRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(reportingUserRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        assertThat(reportingUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(SearchAction.NAME)
                .test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(SearchAction.NAME)
                .test(mockIndexAbstraction(".kibana")), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher("indices:foo")
                .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));

        final String index = ".reporting-" + randomAlphaOfLength(randomIntBetween(0, 13));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME)
                .test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(UpdateAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(mockIndexAbstraction(index)), is(false));

        assertNoAccessAllowed(reportingUserRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(reportingUserRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testSuperuserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("superuser");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role superuserRole = Role.builder(roleDescriptor, null).build();
        assertThat(superuserRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(PutUserAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(PutRoleAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check("internal:admin/foo", request, authentication), is(false));

        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6,
            RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);
        final Metadata metadata = new Metadata.Builder()
                .put(new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("b")
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetadata.Builder("ab").build())
                        .putAlias(new AliasMetadata.Builder("ba").build())
                        .build(), true)
                .put(new IndexMetadata.Builder(internalSecurityIndex)
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetadata.Builder(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).build())
                        .build(), true)
                .build();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();
        Map<String, IndexAccessControl> authzMap =
                superuserRole.indices().authorize(SearchAction.NAME, Sets.newHashSet("a1", "ba"), lookup, fieldPermissionsCache);
        assertThat(authzMap.get("a1").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
        authzMap =
            superuserRole.indices().authorize(DeleteIndexAction.NAME, Sets.newHashSet("a1", "ba"), lookup, fieldPermissionsCache);
        assertThat(authzMap.get("a1").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
        authzMap = superuserRole.indices().authorize(IndexAction.NAME, Sets.newHashSet("a2", "ba"), lookup, fieldPermissionsCache);
        assertThat(authzMap.get("a2").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
        authzMap = superuserRole.indices()
                .authorize(UpdateSettingsAction.NAME, Sets.newHashSet("aaaaaa", "ba"), lookup, fieldPermissionsCache);
        assertThat(authzMap.get("aaaaaa").isGranted(), is(true));
        assertThat(authzMap.get("b").isGranted(), is(true));
        authzMap = superuserRole.indices().authorize(randomFrom(IndexAction.NAME, DeleteIndexAction.NAME, SearchAction.NAME),
                Sets.newHashSet(RestrictedIndicesNames.SECURITY_MAIN_ALIAS), lookup, fieldPermissionsCache);
        assertThat(authzMap.get(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).isGranted(), is(true));
        assertThat(authzMap.get(internalSecurityIndex).isGranted(), is(true));
        assertTrue(superuserRole.indices().check(SearchAction.NAME));
        assertFalse(superuserRole.indices().check("unknown"));

        assertThat(superuserRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(true));

        assertThat(superuserRole.indices().allowedIndicesMatcher(randomFrom(IndexAction.NAME, DeleteIndexAction.NAME, SearchAction.NAME))
                .test(mockIndexAbstraction(RestrictedIndicesNames.SECURITY_MAIN_ALIAS)), is(true));
        assertThat(superuserRole.indices().allowedIndicesMatcher(randomFrom(IndexAction.NAME, DeleteIndexAction.NAME, SearchAction.NAME))
                .test(mockIndexAbstraction(internalSecurityIndex)), is(true));
    }

    public void testLogstashSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("logstash_system");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role logstashSystemRole = Role.builder(roleDescriptor, null).build();
        assertThat(logstashSystemRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(logstashSystemRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(logstashSystemRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(true));
        assertThat(logstashSystemRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(logstashSystemRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(logstashSystemRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(logstashSystemRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(logstashSystemRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(true));

        assertThat(logstashSystemRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        assertThat(logstashSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(logstashSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME)
                .test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(logstashSystemRole.indices().allowedIndicesMatcher("indices:foo")
                .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));

        assertNoAccessAllowed(logstashSystemRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(logstashSystemRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testBeatsAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        final RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("beats_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));


        final Role beatsAdminRole = Role.builder(roleDescriptor, null).build();
        assertThat(beatsAdminRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(beatsAdminRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        assertThat(beatsAdminRole.indices().allowedIndicesMatcher("indices:foo")
                .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));

        final String index = ".management-beats";
        logger.info("index name [{}]", index);
        assertThat(beatsAdminRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsAdminRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsAdminRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsAdminRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsAdminRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsAdminRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsAdminRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsAdminRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsAdminRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));

        assertNoAccessAllowed(beatsAdminRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(beatsAdminRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testBeatsSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor(BeatsSystemUser.ROLE_NAME);
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role beatsSystemRole = Role.builder(roleDescriptor, null).build();
        assertThat(beatsSystemRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(beatsSystemRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(beatsSystemRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(true));
        assertThat(beatsSystemRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(true));

        assertThat(beatsSystemRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));


        final String index = ".monitoring-beats-" + randomIntBetween(0, 5);;
        logger.info("beats monitoring index name [{}]", index);
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher("indices:foo")
                .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(mockIndexAbstraction(index)), is(true));

        assertNoAccessAllowed(beatsSystemRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(beatsSystemRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testAPMSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor(APMSystemUser.ROLE_NAME);
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role APMSystemRole = Role.builder(roleDescriptor, null).build();
        assertThat(APMSystemRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(APMSystemRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(APMSystemRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(true));
        assertThat(APMSystemRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(APMSystemRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(APMSystemRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(APMSystemRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(APMSystemRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(true));

        assertThat(APMSystemRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        assertThat(APMSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(APMSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(APMSystemRole.indices().allowedIndicesMatcher("indices:foo")
                .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));

        final String index = ".monitoring-beats-" + randomIntBetween(10, 15);
        logger.info("APM beats monitoring index name [{}]", index);

        assertThat(APMSystemRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(APMSystemRole.indices().allowedIndicesMatcher("indices:data/write/index:op_type/create")
                .test(mockIndexAbstraction(index)), is(true));
        assertThat(APMSystemRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(APMSystemRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(mockIndexAbstraction(index)), is(true));

        assertThat(APMSystemRole.indices().allowedIndicesMatcher("indices:data/write/index:op_type/index")
                .test(mockIndexAbstraction(index)), is(false));
        assertThat(APMSystemRole.indices().allowedIndicesMatcher(
            "indices:data/write/index:op_type/" + randomAlphaOfLengthBetween(3,5)).test(mockIndexAbstraction(index)), is(false));

        assertNoAccessAllowed(APMSystemRole, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(APMSystemRole, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testAPMUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        final RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("apm_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.builder(roleDescriptor, null).build();

        assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        assertNoAccessAllowed(role, "foo");
        assertNoAccessAllowed(role, "foo-apm");
        assertNoAccessAllowed(role, "foo-logs-apm.bar");
        assertNoAccessAllowed(role, "foo-logs-apm-bar");
        assertNoAccessAllowed(role, "foo-traces-apm.bar");
        assertNoAccessAllowed(role, "foo-traces-apm-bar");
        assertNoAccessAllowed(role, "foo-metrics-apm.bar");
        assertNoAccessAllowed(role, "foo-metrics-apm-bar");

        assertOnlyReadAllowed(role, "logs-apm." + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "logs-apm-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "traces-apm." + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "traces-apm-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "metrics-apm." + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "metrics-apm-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "apm-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT);

        assertOnlyReadAllowed(role, "observability-annotations");

        final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
        assertThat(role.application().grants(
            new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"), is(false));
        assertThat(role.application().grants(
            new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-reserved_ml_apm_user", "reserved_ml_apm_user"), "*"), is(true));

        final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(role.application().grants(
            new ApplicationPrivilege(otherApplication, "app-foo", "foo"), "*"), is(false));
        assertThat(role.application().grants(
            new ApplicationPrivilege(otherApplication, "app-reserved_ml_apm_user", "reserved_ml_apm_user"), "*"), is(false));
    }

    public void testMachineLearningAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("machine_learning_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.builder(roleDescriptor, null).build();
        assertRoleHasManageMl(role);
        assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        assertNoAccessAllowed(role, "foo");
        assertNoAccessAllowed(role, MlConfigIndex.indexName()); // internal use only
        assertOnlyReadAllowed(role, MlMetaIndex.indexName());
        assertOnlyReadAllowed(role, AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX);
        assertOnlyReadAllowed(role, AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT);
        assertOnlyReadAllowed(role, NotificationsIndex.NOTIFICATIONS_INDEX);
        assertReadWriteDocsButNotDeleteIndexAllowed(role, AnnotationIndex.INDEX_NAME);

        assertNoAccessAllowed(role, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(role, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));

        final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
        assertThat(role.application().grants(
            new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"), is(false));
        assertThat(role.application().grants(
            new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-reserved_ml", "reserved_ml_admin"), "*"), is(true));

        final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(role.application().grants(
            new ApplicationPrivilege(otherApplication, "app-foo", "foo"), "*"), is(false));
        assertThat(role.application().grants(
            new ApplicationPrivilege(otherApplication, "app-reserved_ml", "reserved_ml_admin"), "*"), is(false));
    }

    private void assertRoleHasManageMl(Role role) {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        assertThat(role.cluster().check(CloseJobAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DeleteCalendarAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DeleteCalendarEventAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DeleteDatafeedAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DeleteExpiredDataAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DeleteFilterAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DeleteForecastAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DeleteJobAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DeleteModelSnapshotAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DeleteTrainedModelAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(EstimateModelMemoryAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(EvaluateDataFrameAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(ExplainDataFrameAnalyticsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(FinalizeJobExecutionAction.NAME, request, authentication), is(false)); // internal use only
        assertThat(role.cluster().check(FlushJobAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(ForecastJobAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetBucketsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetCalendarEventsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetCalendarsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetCategoriesAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetDatafeedsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetDatafeedsStatsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetDataFrameAnalyticsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetDataFrameAnalyticsStatsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetFiltersAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetInfluencersAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetJobsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetJobsStatsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetModelSnapshotsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetOverallBucketsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetRecordsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetTrainedModelsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetTrainedModelsStatsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(InternalInferModelAction.NAME, request, authentication), is(false)); // internal use only
        assertThat(role.cluster().check(IsolateDatafeedAction.NAME, request, authentication), is(false)); // internal use only
        assertThat(role.cluster().check(KillProcessAction.NAME, request, authentication), is(false)); // internal use only
        assertThat(role.cluster().check(MlInfoAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(OpenJobAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PersistJobAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PostCalendarEventsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PostDataAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PreviewDatafeedAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PutCalendarAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PutDatafeedAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PutDataFrameAnalyticsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PutFilterAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PutJobAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PutTrainedModelAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(RevertModelSnapshotAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(SetUpgradeModeAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(StartDatafeedAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(StartDataFrameAnalyticsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(StopDatafeedAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(StopDataFrameAnalyticsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(UpdateCalendarJobAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(UpdateDatafeedAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(UpdateFilterAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(UpdateJobAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(UpdateModelSnapshotAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(UpdateProcessAction.NAME, request, authentication), is(false)); // internal use only
        assertThat(role.cluster().check(ValidateDetectorAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(ValidateJobConfigAction.NAME, request, authentication), is(true));
    }

    public void testMachineLearningUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("machine_learning_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.builder(roleDescriptor, null).build();
        assertThat(role.cluster().check(CloseJobAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DeleteCalendarAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DeleteCalendarEventAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DeleteDatafeedAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DeleteExpiredDataAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DeleteFilterAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DeleteForecastAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DeleteJobAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DeleteModelSnapshotAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(FinalizeJobExecutionAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(FlushJobAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ForecastJobAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(GetBucketsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetCalendarEventsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetCalendarsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetCategoriesAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetDatafeedsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetDatafeedsStatsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetFiltersAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(GetInfluencersAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetJobsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetJobsStatsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetModelSnapshotsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetOverallBucketsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetRecordsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(IsolateDatafeedAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(KillProcessAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(MlInfoAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(OpenJobAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PersistJobAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PostCalendarEventsAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PostDataAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PreviewDatafeedAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PutCalendarAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PutDatafeedAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PutFilterAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PutJobAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(RevertModelSnapshotAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(SetUpgradeModeAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(StartDatafeedAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(StopDatafeedAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(UpdateCalendarJobAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(UpdateDatafeedAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(UpdateFilterAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(UpdateJobAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(UpdateModelSnapshotAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(UpdateProcessAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ValidateDetectorAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ValidateJobConfigAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        assertNoAccessAllowed(role, "foo");
        assertNoAccessAllowed(role, MlConfigIndex.indexName());
        assertNoAccessAllowed(role, MlMetaIndex.indexName());
        assertNoAccessAllowed(role, AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX);
        assertOnlyReadAllowed(role, AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT);
        assertOnlyReadAllowed(role, NotificationsIndex.NOTIFICATIONS_INDEX);
        assertReadWriteDocsButNotDeleteIndexAllowed(role, AnnotationIndex.INDEX_NAME);

        assertNoAccessAllowed(role, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(role, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));


        final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
        assertThat(role.application().grants(
            new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"), is(false));
        assertThat(role.application().grants(
            new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-reserved_ml", "reserved_ml_user"), "*"), is(true));

        final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(role.application().grants(
            new ApplicationPrivilege(otherApplication, "app-foo", "foo"), "*"), is(false));
        assertThat(role.application().grants(
            new ApplicationPrivilege(otherApplication, "app-reserved_ml", "reserved_ml_user"), "*"), is(false));
    }

    public void testTransformAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor[] roleDescriptors = {
            new ReservedRolesStore().roleDescriptor("data_frame_transforms_admin"),
            new ReservedRolesStore().roleDescriptor("transform_admin")
        };

        for (RoleDescriptor roleDescriptor : roleDescriptors) {
            assertNotNull(roleDescriptor);
            assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
            if (roleDescriptor.getName().equals("data_frame_transforms_admin")) {
                assertThat(roleDescriptor.getMetadata(), hasEntry("_deprecated", true));
            } else {
                assertThat(roleDescriptor.getMetadata(), not(hasEntry("_deprecated", true)));
            }

            Role role = Role.builder(roleDescriptor, null).build();
            assertThat(role.cluster().check(DeleteTransformAction.NAME, request, authentication), is(true));
            assertThat(role.cluster().check(GetTransformAction.NAME, request, authentication), is(true));
            assertThat(role.cluster().check(GetTransformStatsAction.NAME, request, authentication), is(true));
            assertThat(role.cluster().check(PreviewTransformAction.NAME, request, authentication), is(true));
            assertThat(role.cluster().check(PutTransformAction.NAME, request, authentication), is(true));
            assertThat(role.cluster().check(StartTransformAction.NAME, request, authentication), is(true));
            assertThat(role.cluster().check(StopTransformAction.NAME, request, authentication), is(true));
            assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

            assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

            assertOnlyReadAllowed(role, TransformInternalIndexConstants.AUDIT_INDEX_READ_ALIAS);
            assertOnlyReadAllowed(role, TransformInternalIndexConstants.AUDIT_INDEX_PATTERN);
            assertOnlyReadAllowed(role, TransformInternalIndexConstants.AUDIT_INDEX_PATTERN_DEPRECATED);
            assertNoAccessAllowed(role, "foo");
            assertNoAccessAllowed(role, TransformInternalIndexConstants.LATEST_INDEX_NAME); // internal use only

            assertNoAccessAllowed(role, RestrictedIndicesNames.RESTRICTED_NAMES);
            assertNoAccessAllowed(role, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));

            final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
            assertThat(role.application().grants(
                new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"), is(false));

            if (roleDescriptor.getName().equals("data_frame_transforms_admin")) {
                assertThat(role.application().grants(
                    new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-reserved_ml", "reserved_ml_user"), "*"), is(true));
            }

            final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
            assertThat(role.application().grants(
                new ApplicationPrivilege(otherApplication, "app-foo", "foo"), "*"), is(false));
            if (roleDescriptor.getName().equals("data_frame_transforms_admin")) {
                assertThat(role.application().grants(
                        new ApplicationPrivilege(otherApplication, "app-reserved_ml", "reserved_ml_user"), "*"), is(false));
            }
        }
    }

    public void testTransformUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor[] roleDescriptors = {
            new ReservedRolesStore().roleDescriptor("data_frame_transforms_user"),
            new ReservedRolesStore().roleDescriptor("transform_user")
        };

        for (RoleDescriptor roleDescriptor : roleDescriptors) {
            assertNotNull(roleDescriptor);
            assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
            if (roleDescriptor.getName().equals("data_frame_transforms_user")) {
                assertThat(roleDescriptor.getMetadata(), hasEntry("_deprecated", true));
            } else {
                assertThat(roleDescriptor.getMetadata(), not(hasEntry("_deprecated", true)));
            }

            Role role = Role.builder(roleDescriptor, null).build();
            assertThat(role.cluster().check(DeleteTransformAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(GetTransformAction.NAME, request, authentication), is(true));
            assertThat(role.cluster().check(GetTransformStatsAction.NAME, request, authentication), is(true));
            assertThat(role.cluster().check(PreviewTransformAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(PutTransformAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(StartTransformAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(StopTransformAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

            assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

            assertOnlyReadAllowed(role, TransformInternalIndexConstants.AUDIT_INDEX_READ_ALIAS);
            assertOnlyReadAllowed(role, TransformInternalIndexConstants.AUDIT_INDEX_PATTERN);
            assertOnlyReadAllowed(role, TransformInternalIndexConstants.AUDIT_INDEX_PATTERN_DEPRECATED);
            assertNoAccessAllowed(role, "foo");
            assertNoAccessAllowed(role, TransformInternalIndexConstants.LATEST_INDEX_NAME);

            assertNoAccessAllowed(role, RestrictedIndicesNames.RESTRICTED_NAMES);
            assertNoAccessAllowed(role, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));

            final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
            assertThat(role.application().grants(
                new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"), is(false));

            if (roleDescriptor.getName().equals("data_frame_transforms_user")) {
                assertThat(role.application().grants(
                    new ApplicationPrivilege(kibanaApplicationWithRandomIndex, "app-reserved_ml", "reserved_ml_user"), "*"), is(true));
            }

            final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
            assertThat(role.application().grants(
                new ApplicationPrivilege(otherApplication, "app-foo", "foo"), "*"), is(false));
            if (roleDescriptor.getName().equals("data_frame_transforms_user")) {
                assertThat(role.application().grants(
                    new ApplicationPrivilege(otherApplication, "app-reserved_ml", "reserved_ml_user"), "*"), is(false));
            }
        }
    }

    public void testWatcherAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("watcher_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.builder(roleDescriptor, null).build();
        assertThat(role.cluster().check(PutWatchAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetWatchAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DeleteWatchAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(ExecuteWatchAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(AckWatchAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(ActivateWatchAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(WatcherServiceAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(WatcherStatsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        assertThat(role.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));

        for (String index : new String[]{ Watch.INDEX, HistoryStoreField.DATA_STREAM, TriggeredWatchStoreField.INDEX_NAME }) {
            assertOnlyReadAllowed(role, index);
        }

        assertNoAccessAllowed(role, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(role, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testWatcherUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("watcher_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.builder(roleDescriptor, null).build();
        assertThat(role.cluster().check(PutWatchAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(GetWatchAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DeleteWatchAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ExecuteWatchAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(AckWatchAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ActivateWatchAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(WatcherServiceAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(WatcherStatsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        assertThat(role.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(role.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(TriggeredWatchStoreField.INDEX_NAME)),
                is(false));

        for (String index : new String[]{ Watch.INDEX, HistoryStoreField.DATA_STREAM }) {
            assertOnlyReadAllowed(role, index);
        }

        assertNoAccessAllowed(role, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(role, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testPredefinedViewerRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("viewer");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.builder(roleDescriptor, null).build();
        // No cluster privileges
        assertThat(role.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        // Check index privileges
        assertOnlyReadAllowed(role, "observability-annotations");
        assertOnlyReadAllowed(role, "logs-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "metrics-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "synthetics-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "apm-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "traces-apm." + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "filebeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "metricbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "heardbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "kibana_sample_data_-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, ".siem-signals-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "apm-" + randomIntBetween(0, 5) + "-transaction-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "logs-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "auditbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "filebeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "packetbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "winlogbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "endgame-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, randomAlphaOfLength(5));

        assertNoAccessAllowed(role, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(role, "." + randomAlphaOfLengthBetween(6, 10));
        assertNoAccessAllowed(role, "ilm-history-" + randomIntBetween(0, 5));
        // Check application privileges
        assertThat(role.application().grants(new ApplicationPrivilege("kibana-.kibana", "kibana-read", "read"), "*"), is(true));
        assertThat(role.application().grants(new ApplicationPrivilege("kibana-.kibana", "kibana-all", "all"), "*"), is(false));

        assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 20)), is(false));
    }

    public void testPredefinedEditorRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("editor");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.builder(roleDescriptor, null).build();

        // No cluster privileges
        assertThat(role.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        // Check index privileges
        assertOnlyReadAllowed(role, "logs-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "metrics-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "synthetics-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "apm-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "traces-apm." + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "filebeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "metricbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "heardbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "kibana_sample_data_-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "apm-" + randomIntBetween(0, 5) + "-transaction-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "logs-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "auditbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "filebeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "packetbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "winlogbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "endgame-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, randomAlphaOfLength(5));

        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".siem-signals-" + randomIntBetween(0, 5));
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".lists-" + randomIntBetween(0, 5));
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".items-" + randomIntBetween(0, 5));
        assertReadWriteDocsButNotDeleteIndexAllowed(role, "observability-annotations");

        assertNoAccessAllowed(role, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(role, "." + randomAlphaOfLengthBetween(6, 10));
        assertNoAccessAllowed(role, "ilm-history-" + randomIntBetween(0, 5));

        // Check application privileges
        assertThat(role.application().grants(new ApplicationPrivilege("kibana-.kibana", "kibana-all", "all"), "*"), is(true));

        assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 20)), is(false));
    }

    private void assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(Role role, String index) {
        assertThat(role.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(UpdateAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(BulkAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher("indices:admin/refresh*").test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher("indices:admin/flush*").test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher("indices:admin/synced_flush").test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher("indices:admin/forcemerge*").test(mockIndexAbstraction(index)), is(true));
    }

    private void assertReadWriteDocsButNotDeleteIndexAllowed(Role role, String index) {
        assertThat(role.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(UpdateAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(BulkAction.NAME).test(mockIndexAbstraction(index)), is(true));
    }

    private void assertOnlyReadAllowed(Role role, String index) {
        assertThat(role.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(UpdateAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(BulkAction.NAME).test(mockIndexAbstraction(index)), is(false));

        assertNoAccessAllowed(role, RestrictedIndicesNames.RESTRICTED_NAMES);
        assertNoAccessAllowed(role, RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2));
    }

    private void assertNoAccessAllowed(Role role, Collection<String> indices) {
        for (String index : indices) {
            assertNoAccessAllowed(role, index);
        }
    }

    private void assertNoAccessAllowed(Role role, String index) {
        assertThat(role.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(UpdateAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(role.indices().allowedIndicesMatcher(BulkAction.NAME).test(mockIndexAbstraction(index)), is(false));
    }

    public void testLogstashAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = mock(Authentication.class);

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("logstash_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role logstashAdminRole = Role.builder(roleDescriptor, null).build();
        assertThat(logstashAdminRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(logstashAdminRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(logstashAdminRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(logstashAdminRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(logstashAdminRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(logstashAdminRole.cluster().check("cluster:admin/logstash/pipeline/delete", request, authentication), is(true));
        assertThat(logstashAdminRole.cluster().check("cluster:admin/logstash/pipeline/get", request, authentication), is(true));
        assertThat(logstashAdminRole.cluster().check("cluster:admin/logstash/pipeline/put", request, authentication), is(true));

        assertThat(logstashAdminRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(".logstash")), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher("indices:foo").test(
                mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))), is(false));

        final String index = ".logstash-" + randomIntBetween(0, 5);

        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)),
                is(true));
    }

    private IndexAbstraction mockIndexAbstraction(String name) {
        IndexAbstraction mock = mock(IndexAbstraction.class);
        when(mock.getName()).thenReturn(name);
        when(mock.getType()).thenReturn(randomFrom(IndexAbstraction.Type.CONCRETE_INDEX,
                IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM));
        return mock;
    }
}
