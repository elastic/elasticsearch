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
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.ingest.DeletePipelineAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchShardsAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.root.MainAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoAction;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;
import org.elasticsearch.xpack.core.ilm.action.MoveToStepAction;
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
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
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
import org.elasticsearch.xpack.core.rollup.action.GetRollupIndexCapsAction;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.DeletePrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.GetBuiltinPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.GetProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesAction;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataAction;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlAuthenticateAction;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteIndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeTests;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.core.security.user.RemoteMonitoringUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.UsernamesField;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureAction;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.RESTRICTED_INDICES;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
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
        assertThat(ReservedRolesStore.isReserved(SystemUser.ROLE_NAME), is(false));
        assertThat(ReservedRolesStore.isReserved(UsernamesField.ASYNC_SEARCH_ROLE), is(false));
        assertThat(ReservedRolesStore.isReserved(UsernamesField.SECURITY_PROFILE_ROLE), is(false));
        assertThat(ReservedRolesStore.isReserved(UsernamesField.XPACK_ROLE), is(false));
        assertThat(ReservedRolesStore.isReserved(UsernamesField.XPACK_SECURITY_ROLE), is(false));
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
        assertThat(ReservedRolesStore.isReserved(UsernamesField.LOGSTASH_ROLE), is(true));
        assertThat(ReservedRolesStore.isReserved(UsernamesField.BEATS_ROLE), is(true));
        assertThat(ReservedRolesStore.isReserved(UsernamesField.APM_ROLE), is(true));
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
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("snapshot_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        Role snapshotUserRole = Role.buildFromRoleDescriptor(roleDescriptor, fieldPermissionsCache, RESTRICTED_INDICES);
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

        assertThat(
            snapshotUserRole.indices()
                .allowedIndicesMatcher(IndexAction.NAME)
                .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );
        assertThat(
            snapshotUserRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );
        assertThat(
            snapshotUserRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );
        assertThat(
            snapshotUserRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

        assertThat(
            snapshotUserRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME)
                .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(true)
        );

        for (String index : TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES) {
            // This test might cease to be true if we ever have non-security restricted names
            // but that depends on how users are supposed to perform snapshots of those new indices.
            assertThat(snapshotUserRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        }
        assertThat(
            snapshotUserRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME)
                .test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2))),
            is(true)
        );

        assertNoAccessAllowed(snapshotUserRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(snapshotUserRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testIngestAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("ingest_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role ingestAdminRole = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
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
        assertThat(ingestAdminRole.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));

        assertThat(ingestAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(
            ingestAdminRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );
        assertThat(
            ingestAdminRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

        assertNoAccessAllowed(ingestAdminRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(ingestAdminRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testKibanaSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = randomValueOtherThanMany(
            Authentication::isApiKey,  // cannot be API key for managing API keys
            () -> AuthenticationTestHelper.builder().build()
        );

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("kibana_system");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role kibanaRole = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
        assertThat(kibanaRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(GetIndexTemplatesAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(kibanaRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(kibanaRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(true));

        // SAML and token
        assertThat(kibanaRole.cluster().check(SamlPrepareAuthenticationAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(SamlAuthenticateAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(InvalidateTokenAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(CreateTokenAction.NAME, request, authentication), is(true));

        // API keys
        assertThat(kibanaRole.cluster().check(InvalidateApiKeyAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(GrantApiKeyAction.NAME, request, authentication), is(true));
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(randomAlphaOfLength(8), null, null);
        assertThat(kibanaRole.cluster().check(CreateApiKeyAction.NAME, createApiKeyRequest, authentication), is(true));
        // Can only get and query its own API keys
        assertThat(kibanaRole.cluster().check(GetApiKeyAction.NAME, GetApiKeyRequest.builder().build(), authentication), is(false));
        assertThat(
            kibanaRole.cluster().check(GetApiKeyAction.NAME, GetApiKeyRequest.builder().ownedByAuthenticatedUser().build(), authentication),
            is(true)
        );
        final QueryApiKeyRequest queryApiKeyRequest = new QueryApiKeyRequest();
        assertThat(kibanaRole.cluster().check(QueryApiKeyAction.NAME, queryApiKeyRequest, authentication), is(false));
        queryApiKeyRequest.setFilterForCurrentUser();
        assertThat(kibanaRole.cluster().check(QueryApiKeyAction.NAME, queryApiKeyRequest, authentication), is(true));

        // ML
        assertRoleHasManageMl(kibanaRole);

        // Text Structure
        assertThat(kibanaRole.cluster().check(FindStructureAction.NAME, request, authentication), is(true));

        // Application Privileges
        DeletePrivilegesRequest deleteKibanaPrivileges = new DeletePrivilegesRequest("kibana-.kibana", new String[] { "all", "read" });
        DeletePrivilegesRequest deleteLogstashPrivileges = new DeletePrivilegesRequest("logstash", new String[] { "all", "read" });
        assertThat(kibanaRole.cluster().check(DeletePrivilegesAction.NAME, deleteKibanaPrivileges, authentication), is(true));
        assertThat(kibanaRole.cluster().check(DeletePrivilegesAction.NAME, deleteLogstashPrivileges, authentication), is(false));

        GetPrivilegesRequest getKibanaPrivileges = new GetPrivilegesRequest();
        getKibanaPrivileges.application("kibana-.kibana-sales");
        GetPrivilegesRequest getApmPrivileges = new GetPrivilegesRequest();
        getApmPrivileges.application("apm");
        assertThat(kibanaRole.cluster().check(GetPrivilegesAction.NAME, getKibanaPrivileges, authentication), is(true));
        assertThat(kibanaRole.cluster().check(GetPrivilegesAction.NAME, getApmPrivileges, authentication), is(false));

        PutPrivilegesRequest putKibanaPrivileges = new PutPrivilegesRequest();
        putKibanaPrivileges.setPrivileges(
            Collections.singletonList(
                new ApplicationPrivilegeDescriptor(
                    "kibana-.kibana-" + randomAlphaOfLengthBetween(2, 6),
                    "all",
                    Collections.emptySet(),
                    Collections.emptyMap()
                )
            )
        );
        PutPrivilegesRequest putSwiftypePrivileges = new PutPrivilegesRequest();
        putSwiftypePrivileges.setPrivileges(
            Collections.singletonList(
                new ApplicationPrivilegeDescriptor("swiftype-kibana", "all", Collections.emptySet(), Collections.emptyMap())
            )
        );
        assertThat(kibanaRole.cluster().check(PutPrivilegesAction.NAME, putKibanaPrivileges, authentication), is(true));
        assertThat(kibanaRole.cluster().check(PutPrivilegesAction.NAME, putSwiftypePrivileges, authentication), is(false));

        assertThat(kibanaRole.cluster().check(GetBuiltinPrivilegesAction.NAME, request, authentication), is(true));

        // User profile
        assertThat(kibanaRole.cluster().check(GetProfilesAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(ActivateProfileAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(true));
        UpdateProfileDataRequest updateProfileDataRequest = randomBoolean()
            ? new UpdateProfileDataRequest(
                randomAlphaOfLength(10),
                Map.of("kibana" + randomAlphaOfLengthBetween(0, 4), mock(Object.class)),
                Map.of(),
                randomFrom(-1L, randomLong()),
                randomFrom(-1L, randomLong()),
                randomFrom(WriteRequest.RefreshPolicy.values())
            )
            : new UpdateProfileDataRequest(
                randomAlphaOfLength(10),
                Map.of(),
                Map.of("kibana" + randomAlphaOfLengthBetween(0, 4), mock(Object.class)),
                randomFrom(-1L, randomLong()),
                randomFrom(-1L, randomLong()),
                randomFrom(WriteRequest.RefreshPolicy.values())
            );
        assertThat(kibanaRole.cluster().check(UpdateProfileDataAction.NAME, updateProfileDataRequest, authentication), is(true));
        updateProfileDataRequest = new UpdateProfileDataRequest(
            randomAlphaOfLength(10),
            Map.of("kibana" + randomAlphaOfLengthBetween(0, 4), mock(Object.class)),
            Map.of("kibana" + randomAlphaOfLengthBetween(0, 4), mock(Object.class)),
            randomFrom(-1L, randomLong()),
            randomFrom(-1L, randomLong()),
            randomFrom(WriteRequest.RefreshPolicy.values())
        );
        assertThat(kibanaRole.cluster().check(UpdateProfileDataAction.NAME, updateProfileDataRequest, authentication), is(true));
        updateProfileDataRequest = randomBoolean()
            ? new UpdateProfileDataRequest(
                randomAlphaOfLength(10),
                Map.of(randomValueOtherThan("kibana", () -> randomAlphaOfLengthBetween(0, 6)), mock(Object.class)),
                Map.of(),
                randomFrom(-1L, randomLong()),
                randomFrom(-1L, randomLong()),
                randomFrom(WriteRequest.RefreshPolicy.values())
            )
            : new UpdateProfileDataRequest(
                randomAlphaOfLength(10),
                Map.of(),
                Map.of(randomAlphaOfLengthBetween(0, 6), mock(Object.class)),
                randomFrom(-1L, randomLong()),
                randomFrom(-1L, randomLong()),
                randomFrom(WriteRequest.RefreshPolicy.values())
            );
        assertThat(kibanaRole.cluster().check(UpdateProfileDataAction.NAME, updateProfileDataRequest, authentication), is(false));
        updateProfileDataRequest = randomBoolean()
            ? new UpdateProfileDataRequest(
                randomAlphaOfLength(10),
                Map.of(
                    "kibana" + randomAlphaOfLengthBetween(0, 4),
                    mock(Object.class),
                    randomValueOtherThan("kibana", () -> randomAlphaOfLengthBetween(0, 6)),
                    mock(Object.class)
                ),
                Map.of("kibana" + randomAlphaOfLengthBetween(0, 4), mock(Object.class)),
                randomFrom(-1L, randomLong()),
                randomFrom(-1L, randomLong()),
                randomFrom(WriteRequest.RefreshPolicy.values())
            )
            : new UpdateProfileDataRequest(
                randomAlphaOfLength(10),
                Map.of("kibana" + randomAlphaOfLengthBetween(0, 4), mock(Object.class)),
                Map.of(
                    "kibana" + randomAlphaOfLengthBetween(0, 4),
                    mock(Object.class),
                    randomValueOtherThan("kibana", () -> randomAlphaOfLengthBetween(0, 6)),
                    mock(Object.class)
                ),
                randomFrom(-1L, randomLong()),
                randomFrom(-1L, randomLong()),
                randomFrom(WriteRequest.RefreshPolicy.values())
            );
        assertThat(kibanaRole.cluster().check(UpdateProfileDataAction.NAME, updateProfileDataRequest, authentication), is(false));

        // Everything else
        assertThat(kibanaRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));
        assertThat(kibanaRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(true));

        assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(
            kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

        Arrays.asList(
            ".kibana",
            ".kibana-devnull",
            ".reporting-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            ".apm-agent-configuration",
            ".apm-custom-link",
            ".apm-source-map",
            ReservedRolesStore.ALERTS_LEGACY_INDEX + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.ALERTS_BACKING_INDEX + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.ALERTS_INDEX_ALIAS + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.PREVIEW_ALERTS_INDEX_ALIAS + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.PREVIEW_ALERTS_BACKING_INDEX_ALIAS + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.LISTS_INDEX + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.LISTS_ITEMS_INDEX + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach(index -> assertAllIndicesAccessAllowed(kibanaRole, index));

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
        Arrays.asList("apm-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
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

        // read-only indices for APM telemetry under Fleet
        Arrays.asList(
            "traces-apm-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "traces-apm." + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-apm." + randomAlphaOfLength(randomIntBetween(0, 13)),
            "metrics-apm." + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((index) -> {
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(true));
        });

        // read-only indices for Endpoint diagnostic information
        Arrays.asList(".logs-endpoint.diagnostic.collection-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(false));

            // Privileges needed for Fleet package upgrades
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(mockIndexAbstraction(index)), is(true));
            // Privileges needed for installing current ILM policy with delete action
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        });

        // read-only indices for Endpoint events (to build timelines)
        Arrays.asList("logs-endpoint.events.process-default-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));
        });

        // read-only indices for Endpoint events (to build timelines)
        Arrays.asList("logs-endpoint.events.network-default-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));
        });

        Arrays.asList(
            ".fleet",
            ".fleet-agents",
            ".fleet-actions",
            ".fleet-enrollment-api-keys",
            ".fleet-policies",
            ".fleet-actions-results",
            ".fleet-servers"
        ).forEach(index -> assertAllIndicesAccessAllowed(kibanaRole, index));

        // read-only indices for Fleet telemetry
        Arrays.asList("logs-elastic_agent-default", "logs-elastic_agent.fleet_server-default").forEach((index) -> {
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
        });

        // read-only index for Endpoint and Osquery manager specific action responses
        Arrays.asList(".logs-endpoint.action.responses-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));
        });

        Arrays.asList(".logs-osquery_manager.action.responses-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));
        });

        // Index for Endpoint specific actions
        Arrays.asList(".logs-endpoint.actions-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));
        });

        // Data telemetry reads mappings, metadata and stats of indices
        Arrays.asList(randomAlphaOfLengthBetween(8, 24), "packetbeat-*").forEach((index) -> {
            logger.info("index name [{}]", index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndicesStatsAction.NAME).test(mockIndexAbstraction(index)), is(true));
            assertViewIndexMetadata(kibanaRole, index);

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

        // Data telemetry does not have access to system indices that aren't specified
        List.of(".watches", ".geoip_databases", ".logstash", ".snapshot-blob-cache").forEach((index) -> {
            logger.info("index name [{}]", index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetMappingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndicesStatsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(false));
        });

        // Data telemetry does not have access to security and async search
        TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES.forEach((index) -> {
            logger.info("index name [{}]", index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetMappingsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndicesStatsAction.NAME).test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(false));
        });

        // read-only datastream for Endpoint policy responses
        Arrays.asList("metrics-endpoint.policy-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));
        });

        // read-only datastream for Endpoint metrics
        Arrays.asList("metrics-endpoint.metrics-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));
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

        assertNoAccessAllowed(kibanaRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(kibanaRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));

        // Fleet package upgrade
        // 1. Pipeline
        Arrays.asList(
            GetPipelineAction.NAME,
            PutPipelineAction.NAME,
            DeletePipelineAction.NAME,
            SimulatePipelineAction.NAME,
            "cluster:admin/ingest/pipeline/" + randomAlphaOfLengthBetween(3, 8)
        ).forEach(action -> assertThat(kibanaRole.cluster().check(action, request, authentication), is(true)));

        // 2. ILM
        Arrays.asList(
            StartILMAction.NAME,
            DeleteLifecycleAction.NAME,
            GetLifecycleAction.NAME,
            GetStatusAction.NAME,
            MoveToStepAction.NAME,
            PutLifecycleAction.NAME,
            StopILMAction.NAME,
            "cluster:admin/ilm/" + randomAlphaOfLengthBetween(3, 8)
        ).forEach(action -> assertThat(kibanaRole.cluster().check(action, request, authentication), is(true)));

        // 3. Fleet data indices
        Arrays.asList(
            "logs-" + randomAlphaOfLengthBetween(3, 8),
            "metrics-" + randomAlphaOfLengthBetween(3, 8),
            "synthetics-" + randomAlphaOfLengthBetween(3, 8),
            "traces-" + randomAlphaOfLengthBetween(3, 8),
            // Hidden data indices for endpoint package
            ".logs-endpoint.action.responses-" + randomAlphaOfLengthBetween(3, 8),
            ".logs-endpoint.diagnostic.collection-" + randomAlphaOfLengthBetween(3, 8),
            ".logs-endpoint.actions-" + randomAlphaOfLengthBetween(3, 8),
            "profiling-" + randomAlphaOfLengthBetween(3, 8)
        ).forEach(indexName -> {
            logger.info("index name [{}]", indexName);
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            assertThat(
                kibanaRole.indices().allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8)).test(indexAbstraction),
                is(true)
            );

            final boolean isAlsoAutoCreateIndex = indexName.startsWith(".logs-endpoint.actions-");
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(AutoCreateAction.NAME).test(indexAbstraction), is(isAlsoAutoCreateIndex));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateDataStreamAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(isAlsoAutoCreateIndex));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(isAlsoAutoCreateIndex));

            // Endpoint diagnostic and actions data streams also have read access, all others should not.
            final boolean isAlsoReadIndex = indexName.startsWith(".logs-endpoint.diagnostic.collection-")
                || indexName.startsWith(".logs-endpoint.actions-")
                || indexName.startsWith(".logs-endpoint.action.responses-")
                || indexName.startsWith(".logs-osquery_manager.actions-");
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(isAlsoReadIndex));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(isAlsoReadIndex));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(indexAbstraction), is(isAlsoReadIndex));

            // Endpoint diagnostic, APM and Synthetics data streams also have an ILM policy with a delete action, all others should not.
            final boolean isAlsoIlmDeleteIndex = indexName.startsWith(".logs-endpoint.diagnostic.collection-")
                || indexName.startsWith("logs-apm-")
                || indexName.startsWith("logs-apm.")
                || indexName.startsWith("metrics-apm-")
                || indexName.startsWith("metrics-apm.")
                || indexName.startsWith("traces-apm-")
                || indexName.startsWith("traces-apm.")
                || indexName.startsWith("synthetics-http-*")
                || indexName.startsWith("synthetics-tcp-*")
                || indexName.startsWith("synthetics-icmp-*")
                || indexName.startsWith("synthetics-browser-*")
                || indexName.startsWith("synthetics-browser.network-*")
                || indexName.startsWith("synthetics-browser.screenshot-*")
                || indexName.startsWith("profiling-*");
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(isAlsoIlmDeleteIndex));
        });

        // 4. Transform for endpoint package
        Arrays.asList(
            PreviewTransformAction.NAME,
            DeleteTransformAction.NAME,
            GetTransformAction.NAME,
            GetTransformStatsAction.NAME,
            PutTransformAction.NAME,
            StartTransformAction.NAME,
            StopTransformAction.NAME,
            UpdateTransformAction.NAME,
            ValidateTransformAction.NAME,
            "cluster:admin/data_frame/" + randomAlphaOfLengthBetween(3, 8),
            "cluster:monitor/data_frame/" + randomAlphaOfLengthBetween(3, 8),
            "cluster:admin/transform/" + randomAlphaOfLengthBetween(3, 8),
            "cluster:monitor/transform/" + randomAlphaOfLengthBetween(3, 8)
        ).forEach(action -> assertThat(kibanaRole.cluster().check(action, request, authentication), is(true)));

        Arrays.asList(
            "metrics-endpoint.metadata_current_default",
            ".metrics-endpoint.metadata_current_default",
            ".metrics-endpoint.metadata_united_default",
            "metrics-endpoint.metadata_current_default-" + Version.CURRENT,
            ".metrics-endpoint.metadata_current_default-" + Version.CURRENT,
            ".metrics-endpoint.metadata_united_default-" + Version.CURRENT
        ).forEach(indexName -> {
            logger.info("index name [{}]", indexName);
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            // Allow indexing
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(indexAbstraction), is(true));
            // Allow create and delete index, modifying aliases, and updating index settings
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(AutoCreateAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateDataStreamAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteDataStreamAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAliasesAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndicesAliasesAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            assertThat(
                kibanaRole.indices().allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8)).test(indexAbstraction),
                is(true)
            );

            // Granted by bwc for index privilege
            assertThat(
                kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction),
                is(indexAbstraction.getType() != IndexAbstraction.Type.DATA_STREAM)
            );

            // Deny deleting documents and rollover
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(false));
        });

        // Test allow permissions on Threat Intel (ti*) dest indices created by latest transform : "create_index", "delete_index", "read",
        // "index", "delete", IndicesAliasesAction.NAME, UpdateSettingsAction.NAME
        Arrays.asList("logs-ti_recordedfuture_latest.threat", "logs-ti_anomali_latest.threatstream").forEach(indexName -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            // Allow search and indexing
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(indexAbstraction), is(true));
            // Allow create and delete index, modifying aliases, and updating index settings
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAliasesAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndicesAliasesAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));

            // Allow deleting documents
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            assertThat(
                kibanaRole.indices().allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8)).test(indexAbstraction),
                is(true)
            );
        });

        // Test allow permissions on Threat Intel (ti*) source indices required by latest transform : "read", "view_index_metadata",
        // IndicesAliasesAction.NAME, PutMappingAction.NAME, UpdateSettingsAction.NAME, "delete_index"
        Arrays.asList(
            "logs-ti_recordedfuture.threat-default",
            "logs-ti_anomali.threatstream-default",
            "logs-ti_recordedfuture.threat-default" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-ti_anomali.threatstream-default" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach(indexName -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            // Allow read-only
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            assertThat(
                kibanaRole.indices().allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8)).test(indexAbstraction),
                is(true)
            );
        });

        Arrays.asList(
            ".logs-osquery_manager.actions-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            ".logs-osquery_manager.action.responses-" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach(indexName -> {
            logger.info("index name [{}]", indexName);
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            // Allow indexing
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(indexAbstraction), is(true));
            // Allow create and delete index
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(AutoCreateAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateDataStreamAction.NAME).test(indexAbstraction), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            assertThat(
                kibanaRole.indices().allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8)).test(indexAbstraction),
                is(true)
            );
        });

        // read-only datastream for csp indices
        Arrays.asList("logs-cloud_security_posture.findings-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((cspIndex) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(cspIndex);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));
        });

        Arrays.asList("logs-cloud_security_posture.vulnerabilities-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((cspIndex) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(cspIndex);
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:foo").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher("indices:bar").test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(indexAbstraction), is(false));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(PutMappingAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));
        });

        Arrays.asList(
            "logs-cloud_security_posture.findings_latest-default",
            "logs-cloud_security_posture.scores-default",
            "logs-cloud_security_posture.vulnerabilities_latest-default",
            "logs-cloud_security_posture.findings_latest-default-" + Version.CURRENT,
            "logs-cloud_security_posture.scores-default-" + Version.CURRENT,
            "logs-cloud_security_posture.vulnerabilities_latest-default" + Version.CURRENT
        ).forEach(indexName -> {
            logger.info("index name [{}]", indexName);
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            // Allow indexing
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(indexAbstraction), is(true));
            // Allow create and delete index, modifying aliases, and updating index settings
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(AutoCreateAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateDataStreamAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAliasesAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndicesAliasesAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            assertThat(
                kibanaRole.indices().allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8)).test(indexAbstraction),
                is(true)
            );
        });

        // Ensure privileges necessary for ILM policies in APM & Endpoint packages
        Arrays.asList(
            ".logs-endpoint.diagnostic.collection-" + randomAlphaOfLengthBetween(3, 8),
            "logs-apm-" + randomAlphaOfLengthBetween(3, 8),
            "logs-apm." + randomAlphaOfLengthBetween(3, 8) + "-" + randomAlphaOfLengthBetween(3, 8),
            "metrics-apm-" + randomAlphaOfLengthBetween(3, 8),
            "metrics-apm." + randomAlphaOfLengthBetween(3, 8) + "-" + randomAlphaOfLengthBetween(3, 8),
            "traces-apm-" + randomAlphaOfLengthBetween(3, 8),
            "traces-apm." + randomAlphaOfLengthBetween(3, 8) + "-" + randomAlphaOfLengthBetween(3, 8),
            "synthetics-http-" + randomAlphaOfLengthBetween(3, 8),
            "synthetics-icmp-" + randomAlphaOfLengthBetween(3, 8),
            "synthetics-tcp-" + randomAlphaOfLengthBetween(3, 8),
            "synthetics-browser-" + randomAlphaOfLengthBetween(3, 8),
            "synthetics-browser.network-" + randomAlphaOfLengthBetween(3, 8),
            "synthetics-browser.screenshot-" + randomAlphaOfLengthBetween(3, 8)
        ).forEach(indexName -> {
            logger.info("index name [{}]", indexName);
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);

            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));
        });

        // Example transform package
        Arrays.asList("kibana_sample_data_ecommerce", "kibana_sample_data_ecommerce_transform" + randomInt()).forEach(indexName -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            // Allow search and indexing
            assertThat(kibanaRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(indexAbstraction), is(true));
            // Allow create and delete index, modifying aliases, and updating index settings
            assertThat(kibanaRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(GetAliasesAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(IndicesAliasesAction.NAME).test(indexAbstraction), is(true));
            assertThat(kibanaRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(indexAbstraction), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            assertThat(
                kibanaRole.indices().allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8)).test(indexAbstraction),
                is(true)
            );
        });
    }

    public void testKibanaAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("kibana_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
        assertThat(roleDescriptor.getMetadata(), not(hasEntry("_deprecated", true)));

        final String allowedApplicationActionPattern = "example/custom/action/*";
        Role kibanaAdminRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES,
            List.of(new ApplicationPrivilegeDescriptor("kibana-.kibana", "all", Set.of(allowedApplicationActionPattern), Map.of()))
        );
        assertThat(kibanaAdminRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(kibanaAdminRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        assertThat(kibanaAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(kibanaAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(
            kibanaAdminRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

        final String randomApplication = "kibana-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(
            kibanaAdminRole.application().grants(ApplicationPrivilegeTests.createPrivilege(randomApplication, "app-random", "all"), "*"),
            is(false)
        );

        final String application = "kibana-.kibana";
        assertThat(
            kibanaAdminRole.application().grants(ApplicationPrivilegeTests.createPrivilege(application, "app-foo", "foo"), "*"),
            is(false)
        );
        assertThat(
            kibanaAdminRole.application()
                .grants(ApplicationPrivilegeTests.createPrivilege(application, "app-all", allowedApplicationActionPattern), "*"),
            is(true)
        );

        final String applicationWithRandomIndex = "kibana-.kibana_" + randomAlphaOfLengthBetween(8, 24);
        assertThat(
            kibanaAdminRole.application()
                .grants(ApplicationPrivilegeTests.createPrivilege(applicationWithRandomIndex, "app-random-index", "all"), "*"),
            is(false)
        );

        assertNoAccessAllowed(kibanaAdminRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
    }

    public void testKibanaUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("kibana_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
        assertThat(roleDescriptor.getMetadata(), hasEntry("_deprecated", true));

        final String allowedApplicationActionPattern = "example/custom/action/*";
        Role kibanaUserRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES,
            List.of(new ApplicationPrivilegeDescriptor("kibana-.kibana", "all", Set.of(allowedApplicationActionPattern), Map.of()))
        );
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
        assertThat(
            kibanaUserRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

        final String randomApplication = "kibana-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(
            kibanaUserRole.application()
                .grants(ApplicationPrivilegeTests.createPrivilege(randomApplication, "app-random", allowedApplicationActionPattern), "*"),
            is(false)
        );

        final String application = "kibana-.kibana";
        assertThat(
            kibanaUserRole.application().grants(ApplicationPrivilegeTests.createPrivilege(application, "app-foo", "foo"), "*"),
            is(false)
        );
        assertThat(
            kibanaUserRole.application()
                .grants(ApplicationPrivilegeTests.createPrivilege(application, "app-all", allowedApplicationActionPattern), "*"),
            is(true)
        );

        final String applicationWithRandomIndex = "kibana-.kibana_" + randomAlphaOfLengthBetween(8, 24);
        assertThat(
            kibanaUserRole.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(
                        applicationWithRandomIndex,
                        "app-random-index",
                        allowedApplicationActionPattern
                    ),
                    "*"
                ),
            is(false)
        );

        assertNoAccessAllowed(kibanaUserRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(kibanaUserRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testMonitoringUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("monitoring_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        final String allowedApplicationActionPattern = "example/custom/action/*";
        final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
        Role monitoringUserRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES,
            List.of(
                new ApplicationPrivilegeDescriptor(
                    kibanaApplicationWithRandomIndex,
                    "reserved_monitoring",
                    Set.of(allowedApplicationActionPattern),
                    Map.of()
                )
            )
        );
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
        assertThat(monitoringUserRole.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));

        assertThat(monitoringUserRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(
            monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".reporting")),
            is(false)
        );
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".kibana")), is(false));
        assertThat(
            monitoringUserRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );
        assertThat(
            monitoringUserRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction("foo")),
            is(false)
        );
        assertThat(
            monitoringUserRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(".reporting")),
            is(false)
        );
        assertThat(
            monitoringUserRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(".kibana")),
            is(false)
        );

        final String index = ".monitoring-" + randomAlphaOfLength(randomIntBetween(0, 13));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(
            monitoringUserRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)),
            is(false)
        );
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(monitoringUserRole.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(true));

        assertNoAccessAllowed(monitoringUserRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(monitoringUserRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));

        assertThat(
            monitoringUserRole.application()
                .grants(ApplicationPrivilegeTests.createPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"),
            is(false)
        );
        assertThat(
            monitoringUserRole.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(
                        kibanaApplicationWithRandomIndex,
                        "app-reserved_monitoring",
                        allowedApplicationActionPattern
                    ),
                    "*"
                ),
            is(true)
        );

        final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(
            monitoringUserRole.application().grants(ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-foo", "foo"), "*"),
            is(false)
        );
        assertThat(
            monitoringUserRole.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-reserved_monitoring", allowedApplicationActionPattern),
                    "*"
                ),
            is(false)
        );

        final String metricsPrefix = "metrics-";
        assertNoAccessAllowed(monitoringUserRole, metricsPrefix + "system.cpu-default");
        assertNoAccessAllowed(monitoringUserRole, metricsPrefix + "elastic_agent.filebeat-default");

        assertOnlyReadAllowed(monitoringUserRole, metricsPrefix + "elasticsearch");
        assertOnlyReadAllowed(monitoringUserRole, metricsPrefix + "elasticsearch.stack_monitoring.cluster_stats-default");
        assertOnlyReadAllowed(monitoringUserRole, metricsPrefix + "elasticsearch.ingest_pipeline-default");
        assertOnlyReadAllowed(monitoringUserRole, metricsPrefix + "kibana.stack_monitoring.stats-default");
        assertOnlyReadAllowed(monitoringUserRole, metricsPrefix + "logstash.stack_monitoring.node_stats-default");
        assertOnlyReadAllowed(monitoringUserRole, metricsPrefix + "beats.stack_monitoring.stats-default");
        assertOnlyReadAllowed(monitoringUserRole, metricsPrefix + "enterprisesearch.stack_monitoring.health-default");
    }

    public void testRemoteMonitoringAgentRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("remote_monitoring_agent");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role remoteMonitoringAgentRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );
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
        assertThat(remoteMonitoringAgentRole.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringAgentRole.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));
        // ILM
        assertThat(remoteMonitoringAgentRole.cluster().check(GetLifecycleAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(PutLifecycleAction.NAME, request, authentication), is(true));

        // we get this from the cluster:monitor privilege
        assertThat(remoteMonitoringAgentRole.cluster().check(WatcherStatsAction.NAME, request, authentication), is(true));

        assertThat(remoteMonitoringAgentRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("foo")),
            is(false)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".reporting")),
            is(false)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".kibana")),
            is(false)
        );
        assertThat(
            remoteMonitoringAgentRole.indices()
                .allowedIndicesMatcher("indices:foo")
                .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

        final String monitoringIndex = ".monitoring-" + randomAlphaOfLength(randomIntBetween(0, 13));
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(monitoringIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(monitoringIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(monitoringIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(monitoringIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(monitoringIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(monitoringIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices()
                .allowedIndicesMatcher(UpdateSettingsAction.NAME)
                .test(mockIndexAbstraction(monitoringIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(monitoringIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(monitoringIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(monitoringIndex)),
            is(true)
        );

        final String metricbeatIndex = "metricbeat-" + randomAlphaOfLength(randomIntBetween(0, 13));
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(metricbeatIndex)),
            is(false)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(metricbeatIndex)),
            is(false)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(metricbeatIndex)),
            is(false)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(metricbeatIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(metricbeatIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(metricbeatIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetAliasesAction.NAME).test(mockIndexAbstraction(metricbeatIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices()
                .allowedIndicesMatcher(IndicesAliasesAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(RolloverAction.NAME).test(mockIndexAbstraction(metricbeatIndex)),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.indices()
                .allowedIndicesMatcher(IndicesSegmentsAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)),
            is(false)
        );
        assertThat(
            remoteMonitoringAgentRole.indices()
                .allowedIndicesMatcher(RemoveIndexLifecyclePolicyAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)),
            is(false)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(metricbeatIndex)),
            is(false)
        );
        assertThat(
            remoteMonitoringAgentRole.indices()
                .allowedIndicesMatcher(UpdateSettingsAction.NAME)
                .test(mockIndexAbstraction(metricbeatIndex)),
            is(false)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(metricbeatIndex)),
            is(false)
        );
        assertThat(
            remoteMonitoringAgentRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(metricbeatIndex)),
            is(false)
        );

        assertNoAccessAllowed(remoteMonitoringAgentRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(remoteMonitoringAgentRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testRemoteMonitoringCollectorRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("remote_monitoring_collector");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role remoteMonitoringCollectorRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringCollectorRole.cluster().check(GetIndexTemplatesAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringCollectorRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(DeleteIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(remoteMonitoringCollectorRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        assertThat(
            remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(RecoveryAction.NAME).test(mockIndexAbstraction("foo")),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction("foo")),
            is(false)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".reporting")),
            is(false)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".kibana")),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(".kibana")),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher("indices:foo")
                .test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

        Arrays.asList(
            ".monitoring-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "metricbeat-" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((index) -> {
            logger.info("index name [{}]", index);
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetAliasesAction.NAME).test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices()
                    .allowedIndicesMatcher(RemoveIndexLifecyclePolicyAction.NAME)
                    .test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)),
                is(false)
            );
            assertThat(
                remoteMonitoringCollectorRole.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)),
                is(false)
            );
        });

        // These tests might need to change if we add new non-security restricted indices that the monitoring user isn't supposed to see
        // (but ideally, the monitoring user should see all indices).
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(GetSettingsAction.NAME)
                .test(mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES))),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(GetSettingsAction.NAME)
                .test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2))),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(IndicesShardStoresAction.NAME)
                .test(mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES))),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(IndicesShardStoresAction.NAME)
                .test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2))),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(RecoveryAction.NAME)
                .test(mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES))),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(RecoveryAction.NAME)
                .test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2))),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(IndicesStatsAction.NAME)
                .test(mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES))),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(IndicesStatsAction.NAME)
                .test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2))),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(IndicesSegmentsAction.NAME)
                .test(mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES))),
            is(true)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(IndicesSegmentsAction.NAME)
                .test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2))),
            is(true)
        );

        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(SearchAction.NAME)
                .test(mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES))),
            is(false)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(SearchAction.NAME)
                .test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2))),
            is(false)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(GetAction.NAME)
                .test(mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES))),
            is(false)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(GetAction.NAME)
                .test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2))),
            is(false)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(DeleteAction.NAME)
                .test(mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES))),
            is(false)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(DeleteAction.NAME)
                .test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2))),
            is(false)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(IndexAction.NAME)
                .test(mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES))),
            is(false)
        );
        assertThat(
            remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(IndexAction.NAME)
                .test(mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2))),
            is(false)
        );

        assertMonitoringOnRestrictedIndices(remoteMonitoringCollectorRole);

        assertNoAccessAllowed(remoteMonitoringCollectorRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(remoteMonitoringCollectorRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    private void assertMonitoringOnRestrictedIndices(Role role) {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        final Metadata metadata = new Metadata.Builder().put(
            new IndexMetadata.Builder(internalSecurityIndex).settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(new AliasMetadata.Builder(TestRestrictedIndices.SECURITY_MAIN_ALIAS).build())
                .build(),
            true
        ).build();
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        final List<String> indexMonitoringActionNamesList = Arrays.asList(
            IndicesStatsAction.NAME,
            IndicesSegmentsAction.NAME,
            GetSettingsAction.NAME,
            IndicesShardStoresAction.NAME,
            RecoveryAction.NAME
        );
        for (final String indexMonitoringActionName : indexMonitoringActionNamesList) {
            String asyncSearchIndex = XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2);
            final IndicesAccessControl iac = role.indices()
                .authorize(
                    indexMonitoringActionName,
                    Sets.newHashSet(internalSecurityIndex, TestRestrictedIndices.SECURITY_MAIN_ALIAS, asyncSearchIndex),
                    metadata.getIndicesLookup(),
                    fieldPermissionsCache
                );
            assertThat(iac.hasIndexPermissions(internalSecurityIndex), is(true));
            assertThat(iac.hasIndexPermissions(TestRestrictedIndices.SECURITY_MAIN_ALIAS), is(true));
            assertThat(iac.hasIndexPermissions(asyncSearchIndex), is(true));
        }
    }

    public void testReportingUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("reporting_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
        assertThat(roleDescriptor.getMetadata(), hasEntry("_deprecated", true));

        Role reportingUserRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );
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
        assertThat(
            reportingUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".reporting")),
            is(false)
        );
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(".kibana")), is(false));
        assertThat(
            reportingUserRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

        final String index = ".reporting-" + randomAlphaOfLength(randomIntBetween(0, 13));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(
            reportingUserRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)),
            is(false)
        );
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(UpdateAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(reportingUserRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(mockIndexAbstraction(index)), is(false));

        assertNoAccessAllowed(reportingUserRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(reportingUserRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testSuperuserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("superuser");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role superuserRole = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
        assertThat(superuserRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(PutUserAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(PutRoleAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check("internal:admin/foo", request, authentication), is(false));
        assertThat(
            superuserRole.cluster().check(UpdateProfileDataAction.NAME, mock(UpdateProfileDataRequest.class), authentication),
            is(true)
        );
        assertThat(superuserRole.cluster().check(GetProfilesAction.NAME, mock(UpdateProfileDataRequest.class), authentication), is(true));
        assertThat(superuserRole.cluster().check(SuggestProfilesAction.NAME, mock(SuggestProfilesRequest.class), authentication), is(true));
        assertThat(superuserRole.cluster().check(ActivateProfileAction.NAME, mock(ActivateProfileRequest.class), authentication), is(true));

        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        final Metadata metadata = new Metadata.Builder().put(
            new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(),
            true
        )
            .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("aaaaaa").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("bbbbb").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(
                new IndexMetadata.Builder("b").settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder("ab").build())
                    .putAlias(new AliasMetadata.Builder("ba").build())
                    .build(),
                true
            )
            .put(
                new IndexMetadata.Builder(internalSecurityIndex).settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder(TestRestrictedIndices.SECURITY_MAIN_ALIAS).build())
                    .build(),
                true
            )
            .build();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();
        IndicesAccessControl iac = superuserRole.indices()
            .authorize(SearchAction.NAME, Sets.newHashSet("a1", "ba"), lookup, fieldPermissionsCache);
        assertThat(iac.hasIndexPermissions("a1"), is(true));
        assertThat(iac.hasIndexPermissions("b"), is(true));
        iac = superuserRole.indices().authorize(DeleteIndexAction.NAME, Sets.newHashSet("a1", "ba"), lookup, fieldPermissionsCache);
        assertThat(iac.hasIndexPermissions("a1"), is(true));
        assertThat(iac.hasIndexPermissions("b"), is(true));
        iac = superuserRole.indices().authorize(IndexAction.NAME, Sets.newHashSet("a2", "ba"), lookup, fieldPermissionsCache);
        assertThat(iac.hasIndexPermissions("a2"), is(true));
        assertThat(iac.hasIndexPermissions("b"), is(true));
        iac = superuserRole.indices().authorize(UpdateSettingsAction.NAME, Sets.newHashSet("aaaaaa", "ba"), lookup, fieldPermissionsCache);
        assertThat(iac.hasIndexPermissions("aaaaaa"), is(true));
        assertThat(iac.hasIndexPermissions("b"), is(true));

        // Read security indices => allowed
        iac = superuserRole.indices()
            .authorize(
                randomFrom(SearchAction.NAME, GetIndexAction.NAME),
                Sets.newHashSet(TestRestrictedIndices.SECURITY_MAIN_ALIAS),
                lookup,
                fieldPermissionsCache
            );
        assertThat("For " + iac, iac.hasIndexPermissions(TestRestrictedIndices.SECURITY_MAIN_ALIAS), is(true));
        assertThat("For " + iac, iac.hasIndexPermissions(internalSecurityIndex), is(true));

        // Write security indices => denied
        iac = superuserRole.indices()
            .authorize(
                randomFrom(IndexAction.NAME, DeleteIndexAction.NAME),
                Sets.newHashSet(TestRestrictedIndices.SECURITY_MAIN_ALIAS),
                lookup,
                fieldPermissionsCache
            );
        assertThat("For " + iac, iac.hasIndexPermissions(TestRestrictedIndices.SECURITY_MAIN_ALIAS), is(false));
        assertThat("For " + iac, iac.hasIndexPermissions(internalSecurityIndex), is(false));

        assertTrue(superuserRole.indices().check(SearchAction.NAME));
        assertFalse(superuserRole.indices().check("unknown"));

        assertThat(superuserRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(true));

        // Read security indices => allowed
        assertThat(
            superuserRole.indices()
                .allowedIndicesMatcher(randomFrom(GetAction.NAME, IndicesStatsAction.NAME))
                .test(mockIndexAbstraction(TestRestrictedIndices.SECURITY_MAIN_ALIAS)),
            is(true)
        );
        assertThat(
            superuserRole.indices()
                .allowedIndicesMatcher(randomFrom(GetAction.NAME, IndicesStatsAction.NAME))
                .test(mockIndexAbstraction(internalSecurityIndex)),
            is(true)
        );

        // Write security indices => denied
        assertThat(
            superuserRole.indices()
                .allowedIndicesMatcher(randomFrom(IndexAction.NAME, DeleteIndexAction.NAME))
                .test(mockIndexAbstraction(TestRestrictedIndices.SECURITY_MAIN_ALIAS)),
            is(false)
        );
        assertThat(
            superuserRole.indices()
                .allowedIndicesMatcher(randomFrom(IndexAction.NAME, DeleteIndexAction.NAME))
                .test(mockIndexAbstraction(internalSecurityIndex)),
            is(false)
        );
    }

    public void testLogstashSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("logstash_system");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role logstashSystemRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );
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
        assertThat(
            logstashSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(".reporting")),
            is(false)
        );
        assertThat(
            logstashSystemRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

        assertNoAccessAllowed(logstashSystemRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(logstashSystemRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testBeatsAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        final RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("beats_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        final Role beatsAdminRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );
        assertThat(beatsAdminRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));

        assertThat(beatsAdminRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        assertThat(
            beatsAdminRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

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

        assertNoAccessAllowed(beatsAdminRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(beatsAdminRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testBeatsSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor(UsernamesField.BEATS_ROLE);
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role beatsSystemRole = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
        assertThat(beatsSystemRole.cluster().check(ClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(beatsSystemRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(beatsSystemRole.cluster().check(ClusterStatsAction.NAME, request, authentication), is(true));
        assertThat(beatsSystemRole.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(true));
        assertThat(beatsSystemRole.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));

        assertThat(beatsSystemRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        final String index = ".monitoring-beats-" + randomIntBetween(0, 5);
        logger.info("beats monitoring index name [{}]", index);
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction("foo")), is(false));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(".reporting")), is(false));
        assertThat(
            beatsSystemRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(beatsSystemRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(mockIndexAbstraction(index)), is(true));

        assertNoAccessAllowed(beatsSystemRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(beatsSystemRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testAPMSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor(UsernamesField.APM_ROLE);
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role APMSystemRole = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
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
        assertThat(
            APMSystemRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

        final String index = ".monitoring-beats-" + randomIntBetween(10, 15);
        logger.info("APM beats monitoring index name [{}]", index);

        assertThat(APMSystemRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(
            APMSystemRole.indices().allowedIndicesMatcher("indices:data/write/index:op_type/create").test(mockIndexAbstraction(index)),
            is(true)
        );
        assertThat(APMSystemRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(false));
        assertThat(APMSystemRole.indices().allowedIndicesMatcher(BulkAction.NAME).test(mockIndexAbstraction(index)), is(true));

        assertThat(
            APMSystemRole.indices().allowedIndicesMatcher("indices:data/write/index:op_type/index").test(mockIndexAbstraction(index)),
            is(false)
        );
        assertThat(
            APMSystemRole.indices()
                .allowedIndicesMatcher("indices:data/write/index:op_type/" + randomAlphaOfLengthBetween(3, 5))
                .test(mockIndexAbstraction(index)),
            is(false)
        );

        assertNoAccessAllowed(APMSystemRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(APMSystemRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testAPMUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        final RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("apm_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        final String allowedApplicationActionPattern = "example/custom/action/*";
        final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
        Role role = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES,
            List.of(
                new ApplicationPrivilegeDescriptor(
                    kibanaApplicationWithRandomIndex,
                    "reserved_ml_apm_user",
                    Set.of(allowedApplicationActionPattern),
                    Map.of()
                )
            )
        );

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

        assertThat(
            role.application().grants(ApplicationPrivilegeTests.createPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"),
            is(false)
        );
        assertThat(
            role.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(
                        kibanaApplicationWithRandomIndex,
                        "app-reserved_ml_apm_user",
                        allowedApplicationActionPattern
                    ),
                    "*"
                ),
            is(true)
        );

        final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(
            role.application().grants(ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-foo", "foo"), "*"),
            is(false)
        );
        assertThat(
            role.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(
                        otherApplication,
                        "app-reserved_ml_apm_user",
                        allowedApplicationActionPattern
                    ),
                    "*"
                ),
            is(false)
        );
    }

    public void testMachineLearningAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("machine_learning_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        final String allowedApplicationActionPattern = "example/custom/action/*";
        final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
        Role role = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES,
            List.of(
                new ApplicationPrivilegeDescriptor(
                    kibanaApplicationWithRandomIndex,
                    "reserved_ml_admin",
                    Set.of(allowedApplicationActionPattern),
                    Map.of()
                )
            )
        );
        assertRoleHasManageMl(role);
        assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        assertNoAccessAllowed(role, "foo");
        assertNoAccessAllowed(role, MlConfigIndex.indexName()); // internal use only
        assertOnlyReadAllowed(role, MlMetaIndex.indexName());
        assertOnlyReadAllowed(role, AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX);
        assertOnlyReadAllowed(role, AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT);
        assertOnlyReadAllowed(role, NotificationsIndex.NOTIFICATIONS_INDEX);
        assertReadWriteDocsButNotDeleteIndexAllowed(role, AnnotationIndex.LATEST_INDEX_NAME);

        assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(role, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));

        assertThat(
            role.application().grants(ApplicationPrivilegeTests.createPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"),
            is(false)
        );
        assertThat(
            role.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(
                        kibanaApplicationWithRandomIndex,
                        "app-reserved_ml",
                        allowedApplicationActionPattern
                    ),
                    "*"
                ),
            is(true)
        );

        final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(
            role.application().grants(ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-foo", "foo"), "*"),
            is(false)
        );
        assertThat(
            role.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-reserved_ml", allowedApplicationActionPattern),
                    "*"
                ),
            is(false)
        );
    }

    private void assertRoleHasManageMl(Role role) {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

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
        assertThat(role.cluster().check(InferModelAction.EXTERNAL_NAME, request, authentication), is(true));
        assertThat(role.cluster().check(InferModelAction.NAME, request, authentication), is(false)); // internal use only
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
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("machine_learning_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        final String allowedApplicationActionPattern = "example/custom/action/*";
        final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
        Role role = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES,
            List.of(
                new ApplicationPrivilegeDescriptor(
                    kibanaApplicationWithRandomIndex,
                    "reserved_ml_user",
                    Set.of(allowedApplicationActionPattern),
                    Map.of()
                )
            )
        );
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
        assertThat(role.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));

        assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        assertNoAccessAllowed(role, "foo");
        assertNoAccessAllowed(role, MlConfigIndex.indexName());
        assertNoAccessAllowed(role, MlMetaIndex.indexName());
        assertNoAccessAllowed(role, AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX);
        assertOnlyReadAllowed(role, AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + AnomalyDetectorsIndexFields.RESULTS_INDEX_DEFAULT);
        assertOnlyReadAllowed(role, NotificationsIndex.NOTIFICATIONS_INDEX);
        assertReadWriteDocsButNotDeleteIndexAllowed(role, AnnotationIndex.LATEST_INDEX_NAME);

        assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(role, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));

        assertThat(
            role.application().grants(ApplicationPrivilegeTests.createPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"),
            is(false)
        );
        assertThat(
            role.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(
                        kibanaApplicationWithRandomIndex,
                        "app-reserved_ml",
                        allowedApplicationActionPattern
                    ),
                    "*"
                ),
            is(true)
        );

        final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(
            role.application().grants(ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-foo", "foo"), "*"),
            is(false)
        );
        assertThat(
            role.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-reserved_ml", allowedApplicationActionPattern),
                    "*"
                ),
            is(false)
        );
    }

    public void testTransformAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor[] roleDescriptors = {
            new ReservedRolesStore().roleDescriptor("data_frame_transforms_admin"),
            new ReservedRolesStore().roleDescriptor("transform_admin") };

        for (RoleDescriptor roleDescriptor : roleDescriptors) {
            assertNotNull(roleDescriptor);
            assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
            if (roleDescriptor.getName().equals("data_frame_transforms_admin")) {
                assertThat(roleDescriptor.getMetadata(), hasEntry("_deprecated", true));
            } else {
                assertThat(roleDescriptor.getMetadata(), not(hasEntry("_deprecated", true)));
            }

            final String allowedApplicationActionPattern = "example/custom/action/*";
            final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
            List<ApplicationPrivilegeDescriptor> lookup = roleDescriptor.getName().equals("data_frame_transforms_admin")
                ? List.of(
                    new ApplicationPrivilegeDescriptor(
                        kibanaApplicationWithRandomIndex,
                        "reserved_ml_user",
                        Set.of(allowedApplicationActionPattern),
                        Map.of()
                    )
                )
                : List.of();
            Role role = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES, lookup);
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

            assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
            assertNoAccessAllowed(role, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));

            assertThat(
                role.application()
                    .grants(ApplicationPrivilegeTests.createPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"),
                is(false)
            );

            if (roleDescriptor.getName().equals("data_frame_transforms_admin")) {
                assertThat(
                    role.application()
                        .grants(
                            ApplicationPrivilegeTests.createPrivilege(
                                kibanaApplicationWithRandomIndex,
                                "app-reserved_ml",
                                allowedApplicationActionPattern
                            ),
                            "*"
                        ),
                    is(true)
                );
            }

            final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
            assertThat(
                role.application().grants(ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-foo", "foo"), "*"),
                is(false)
            );
            if (roleDescriptor.getName().equals("data_frame_transforms_admin")) {
                assertThat(
                    role.application()
                        .grants(
                            ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-reserved_ml", allowedApplicationActionPattern),
                            "*"
                        ),
                    is(false)
                );
            }
        }
    }

    public void testTransformUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor[] roleDescriptors = {
            new ReservedRolesStore().roleDescriptor("data_frame_transforms_user"),
            new ReservedRolesStore().roleDescriptor("transform_user") };

        for (RoleDescriptor roleDescriptor : roleDescriptors) {
            assertNotNull(roleDescriptor);
            assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
            if (roleDescriptor.getName().equals("data_frame_transforms_user")) {
                assertThat(roleDescriptor.getMetadata(), hasEntry("_deprecated", true));
            } else {
                assertThat(roleDescriptor.getMetadata(), not(hasEntry("_deprecated", true)));
            }

            final String allowedApplicationActionPattern = "example/custom/action/*";
            final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
            List<ApplicationPrivilegeDescriptor> lookup = roleDescriptor.getName().equals("data_frame_transforms_user")
                ? List.of(
                    new ApplicationPrivilegeDescriptor(
                        kibanaApplicationWithRandomIndex,
                        "reserved_ml_user",
                        Set.of(allowedApplicationActionPattern),
                        Map.of()
                    )
                )
                : List.of();
            Role role = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES, lookup);
            assertThat(role.cluster().check(DeleteTransformAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(GetTransformAction.NAME, request, authentication), is(true));
            assertThat(role.cluster().check(GetTransformStatsAction.NAME, request, authentication), is(true));
            assertThat(role.cluster().check(PreviewTransformAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(PutTransformAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(StartTransformAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(StopTransformAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
            assertThat(role.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));

            assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

            assertOnlyReadAllowed(role, TransformInternalIndexConstants.AUDIT_INDEX_READ_ALIAS);
            assertOnlyReadAllowed(role, TransformInternalIndexConstants.AUDIT_INDEX_PATTERN);
            assertOnlyReadAllowed(role, TransformInternalIndexConstants.AUDIT_INDEX_PATTERN_DEPRECATED);
            assertNoAccessAllowed(role, "foo");
            assertNoAccessAllowed(role, TransformInternalIndexConstants.LATEST_INDEX_NAME);

            assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
            assertNoAccessAllowed(role, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));

            assertThat(
                role.application()
                    .grants(ApplicationPrivilegeTests.createPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"),
                is(false)
            );

            if (roleDescriptor.getName().equals("data_frame_transforms_user")) {
                assertThat(
                    role.application()
                        .grants(
                            ApplicationPrivilegeTests.createPrivilege(
                                kibanaApplicationWithRandomIndex,
                                "app-reserved_ml",
                                allowedApplicationActionPattern
                            ),
                            "*"
                        ),
                    is(true)
                );
            }

            final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
            assertThat(
                role.application().grants(ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-foo", "foo"), "*"),
                is(false)
            );
            if (roleDescriptor.getName().equals("data_frame_transforms_user")) {
                assertThat(
                    role.application()
                        .grants(
                            ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-reserved_ml", allowedApplicationActionPattern),
                            "*"
                        ),
                    is(false)
                );
            }
        }
    }

    public void testWatcherAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("watcher_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
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

        for (String index : new String[] { Watch.INDEX, HistoryStoreField.DATA_STREAM, TriggeredWatchStoreField.INDEX_NAME }) {
            assertOnlyReadAllowed(role, index);
        }

        assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(role, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testWatcherUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("watcher_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
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
        assertThat(
            role.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(TriggeredWatchStoreField.INDEX_NAME)),
            is(false)
        );

        for (String index : new String[] { Watch.INDEX, HistoryStoreField.DATA_STREAM }) {
            assertOnlyReadAllowed(role, index);
        }

        assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(role, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testPredefinedViewerRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("viewer");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        final String allowedApplicationActionPattern = "example/custom/action/*";
        Role role = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES,
            List.of(new ApplicationPrivilegeDescriptor("kibana-.kibana", "read", Set.of(allowedApplicationActionPattern), Map.of()))
        );
        // No cluster privileges
        assertThat(role.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));
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
        assertOnlyReadAllowed(role, ".alerts-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, ".preview.alerts-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, ".lists-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, ".items-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "apm-" + randomIntBetween(0, 5) + "-transaction-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "logs-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "auditbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "filebeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "packetbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "winlogbeat-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, "endgame-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, randomAlphaOfLength(5));

        assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(role, "." + randomAlphaOfLengthBetween(6, 10));
        assertNoAccessAllowed(role, "ilm-history-" + randomIntBetween(0, 5));
        // Check application privileges
        assertThat(
            role.application()
                .grants(ApplicationPrivilegeTests.createPrivilege("kibana-.kibana", "kibana-read", allowedApplicationActionPattern), "*"),
            is(true)
        );
        assertThat(
            role.application().grants(ApplicationPrivilegeTests.createPrivilege("kibana-.kibana", "kibana-all", "all"), "*"),
            is(false)
        );

        assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 20)), is(false));
    }

    public void testPredefinedEditorRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("editor");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        final String allowedApplicationActionPattern = "example/custom/action/*";
        Role role = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES,
            List.of(new ApplicationPrivilegeDescriptor("kibana-.kibana", "all", Set.of(allowedApplicationActionPattern), Map.of()))
        );

        // No cluster privileges
        assertThat(role.cluster().check(ClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterStatsAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PutIndexTemplateAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterRerouteAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));

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
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".alerts-" + randomIntBetween(0, 5));
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".internal.alerts-" + randomIntBetween(0, 5));
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".preview.alerts-" + randomIntBetween(0, 5));
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".internal.preview.alerts-" + randomIntBetween(0, 5));

        assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(role, "." + randomAlphaOfLengthBetween(6, 10));
        assertNoAccessAllowed(role, "ilm-history-" + randomIntBetween(0, 5));

        // Check application privileges
        assertThat(
            role.application()
                .grants(ApplicationPrivilegeTests.createPrivilege("kibana-.kibana", "kibana-all", allowedApplicationActionPattern), "*"),
            is(true)
        );

        assertThat(role.runAs().check(randomAlphaOfLengthBetween(1, 20)), is(false));
    }

    public void testRemoteIndicesPrivilegesForSuperuserRole() {
        final RoleDescriptor superuserRoleDescriptor = new ReservedRolesStore().roleDescriptor("superuser");
        final Role superuserRole = Role.buildFromRoleDescriptor(
            superuserRoleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );

        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            assertThat(superuserRoleDescriptor.hasRemoteIndicesPrivileges(), is(true));
            assertThat(
                Arrays.stream(superuserRoleDescriptor.getRemoteIndicesPrivileges())
                    .map(RoleDescriptor.RemoteIndicesPrivileges::indicesPrivileges)
                    .toArray(RoleDescriptor.IndicesPrivileges[]::new),
                equalTo(superuserRoleDescriptor.getIndicesPrivileges())
            );
            final List<RemoteIndicesPermission.RemoteIndicesGroup> remoteIndicesGroups = superuserRole.remoteIndices()
                .remoteIndicesGroups();
            assertThat(remoteIndicesGroups.size(), equalTo(1));
            assertThat(remoteIndicesGroups.get(0).remoteClusterAliasMatcher().isTotal(), is(true));
        } else {
            assertThat(superuserRoleDescriptor.hasRemoteIndicesPrivileges(), is(false));
            assertThat(superuserRoleDescriptor.getRemoteIndicesPrivileges(), emptyArray());
            assertThat(superuserRole.remoteIndices(), is(RemoteIndicesPermission.NONE));
        }
    }

    public void testRemoteIndicesPrivileges() {
        final List<String> rolesWithRemoteIndicesPrivileges = new ArrayList<>();

        for (RoleDescriptor roleDescriptor : new ReservedRolesStore().roleDescriptors()) {
            if (roleDescriptor.getName().equals("superuser")) {
                continue;  // superuser is tested separately
            }
            final Role role = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);

            if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
                // The assumption here is that any read_cross_cluster indices privileges should be paired with
                // a corresponding remote indices privileges
                final var readCrossClusterIndicesPrivileges = Arrays.stream(roleDescriptor.getIndicesPrivileges())
                    .filter(ip -> Arrays.asList(ip.getPrivileges()).contains("read_cross_cluster"))
                    .toArray(RoleDescriptor.IndicesPrivileges[]::new);
                if (readCrossClusterIndicesPrivileges.length == 0) {
                    assertThat(roleDescriptor.hasRemoteIndicesPrivileges(), is(false));
                } else {
                    assertThat(roleDescriptor.hasRemoteIndicesPrivileges(), is(true));
                    assertThat(
                        Arrays.stream(roleDescriptor.getRemoteIndicesPrivileges())
                            .map(RoleDescriptor.RemoteIndicesPrivileges::indicesPrivileges)
                            .toList(),
                        containsInAnyOrder(readCrossClusterIndicesPrivileges)
                    );
                    rolesWithRemoteIndicesPrivileges.add(roleDescriptor.getName());
                }
            } else {
                assertThat(roleDescriptor.hasRemoteIndicesPrivileges(), is(false));
                assertThat(roleDescriptor.getRemoteIndicesPrivileges(), emptyArray());
                assertThat(role.remoteIndices(), is(RemoteIndicesPermission.NONE));
            }
        }

        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            assertThat(rolesWithRemoteIndicesPrivileges, containsInAnyOrder("kibana_system", "monitoring_user"));
        } else {
            assertThat(rolesWithRemoteIndicesPrivileges, emptyIterable());
        }
    }

    private void assertAllIndicesAccessAllowed(Role role, String index) {
        logger.info("index name [{}]", index);
        assertThat(role.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher("indices:bar").test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(GetIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(role.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
        // inherits from 'all'
        assertThat(role.indices().allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME).test(mockIndexAbstraction(index)), is(true));
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

        assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(role, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    private void assertViewIndexMetadata(Role role, String index) {
        Arrays.asList(
            GetAliasesAction.NAME,
            GetIndexAction.NAME,
            GetFieldMappingsAction.NAME + "*",
            GetMappingsAction.NAME,
            ClusterSearchShardsAction.NAME,
            SearchShardsAction.NAME,
            ValidateQueryAction.NAME + "*",
            GetSettingsAction.NAME,
            ExplainLifecycleAction.NAME,
            GetDataStreamAction.NAME,
            ResolveIndexAction.NAME,
            FieldCapabilitiesAction.NAME + "*",
            GetRollupIndexCapsAction.NAME + "*"
        ).forEach(action -> assertThat(role.indices().allowedIndicesMatcher(action).test(mockIndexAbstraction(index)), is(true)));
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
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = new ReservedRolesStore().roleDescriptor("logstash_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role logstashAdminRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );
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
        assertThat(
            logstashAdminRole.indices().allowedIndicesMatcher("indices:foo").test(mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24))),
            is(false)
        );

        final String index = ".logstash-" + randomIntBetween(0, 5);

        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(DeleteAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(DeleteIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(CreateIndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(IndexAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(GetAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(SearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(logstashAdminRole.indices().allowedIndicesMatcher(MultiSearchAction.NAME).test(mockIndexAbstraction(index)), is(true));
        assertThat(
            logstashAdminRole.indices().allowedIndicesMatcher(UpdateSettingsAction.NAME).test(mockIndexAbstraction(index)),
            is(true)
        );
    }

    private IndexAbstraction mockIndexAbstraction(String name) {
        IndexAbstraction mock = mock(IndexAbstraction.class);
        when(mock.getName()).thenReturn(name);
        when(mock.getType()).thenReturn(
            randomFrom(IndexAbstraction.Type.CONCRETE_INDEX, IndexAbstraction.Type.ALIAS, IndexAbstraction.Type.DATA_STREAM)
        );
        return mock;
    }
}
