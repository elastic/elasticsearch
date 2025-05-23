/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportSnapshotsStatusAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.create.AutoCreateAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.ingest.DeletePipelineTransportAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoAction;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetStatusAction;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction;
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
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
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
import org.elasticsearch.xpack.core.transform.action.SetTransformUpgradeModeAction;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore.INCLUDED_RESERVED_ROLES_SETTING;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.RESTRICTED_INDICES;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link ReservedRolesStore}
 */
public class ReservedRolesStoreTests extends ESTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Initialize the reserved roles store so that static fields are populated.
        // In production code, this is guaranteed by how components are initialized by the Security plugin
        new ReservedRolesStore();
    }

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
        assertThat(ReservedRolesStore.isReserved("inference_admin"), is(true));
        assertThat(ReservedRolesStore.isReserved("inference_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("ingest_admin"), is(true));
        assertThat(ReservedRolesStore.isReserved("monitoring_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("reporting_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("machine_learning_user"), is(true));
        assertThat(ReservedRolesStore.isReserved("machine_learning_admin"), is(true));
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

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("snapshot_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        Role snapshotUserRole = Role.buildFromRoleDescriptor(roleDescriptor, fieldPermissionsCache, RESTRICTED_INDICES);
        assertThat(snapshotUserRole.cluster().check(GetRepositoriesAction.NAME, request, authentication), is(true));
        assertThat(snapshotUserRole.cluster().check(TransportCreateSnapshotAction.TYPE.name(), request, authentication), is(true));
        assertThat(snapshotUserRole.cluster().check(TransportSnapshotsStatusAction.TYPE.name(), request, authentication), is(true));
        assertThat(snapshotUserRole.cluster().check(TransportGetSnapshotsAction.TYPE.name(), request, authentication), is(true));

        assertThat(snapshotUserRole.cluster().check(TransportPutRepositoryAction.TYPE.name(), request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(GetIndexTemplatesAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(TransportDeleteIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(PutPipelineTransportAction.TYPE.name(), request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(GetPipelineAction.NAME, request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(DeletePipelineTransportAction.TYPE.name(), request, authentication), is(false));
        assertThat(snapshotUserRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
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

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = snapshotUserRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = snapshotUserRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = snapshotUserRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = snapshotUserRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = snapshotUserRole.indices()
            .allowedIndicesMatcher(GetIndexAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));

        for (String index : TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES) {
            // This test might cease to be true if we ever have non-security restricted names
            // but that depends on how users are supposed to perform snapshots of those new indices.
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = snapshotUserRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        }
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = snapshotUserRole.indices()
            .allowedIndicesMatcher(GetIndexAction.NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

        assertNoAccessAllowed(snapshotUserRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(snapshotUserRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testIngestAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("ingest_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role ingestAdminRole = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
        assertThat(ingestAdminRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(GetIndexTemplatesAction.NAME, request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(TransportDeleteIndexTemplateAction.TYPE.name(), request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(PutPipelineTransportAction.TYPE.name(), request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(GetPipelineAction.NAME, request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(DeletePipelineTransportAction.TYPE.name(), request, authentication), is(true));
        assertThat(ingestAdminRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
        assertThat(ingestAdminRole.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = ingestAdminRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = ingestAdminRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = ingestAdminRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));

        assertNoAccessAllowed(ingestAdminRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(ingestAdminRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testKibanaSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = randomValueOtherThanMany(
            Authentication::isApiKey,  // cannot be API key for managing API keys
            () -> AuthenticationTestHelper.builder().build()
        );

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("kibana_system");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role kibanaRole = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
        assertThat(kibanaRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(GetIndexTemplatesAction.NAME, request, authentication), is(true));
        assertThat(kibanaRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
        assertThat(kibanaRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(kibanaRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(true));

        // Inference
        assertTrue(kibanaRole.cluster().check("cluster:admin/xpack/inference/get", request, authentication));
        assertTrue(kibanaRole.cluster().check("cluster:admin/xpack/inference/put", request, authentication));
        assertTrue(kibanaRole.cluster().check("cluster:admin/xpack/inference/delete", request, authentication));

        // Enrich
        assertThat(kibanaRole.cluster().check("cluster:admin/xpack/enrich/put", request, authentication), is(true));
        assertThat(kibanaRole.cluster().check("cluster:admin/xpack/enrich/execute", request, authentication), is(true));
        assertThat(kibanaRole.cluster().check("cluster:admin/xpack/enrich/get", request, authentication), is(true));
        assertThat(kibanaRole.cluster().check("cluster:admin/xpack/enrich/delete", request, authentication), is(true));
        assertThat(kibanaRole.cluster().check("cluster:admin/xpack/enrich/stats", request, authentication), is(true));

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

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate40 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction16 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate40.test(indexAbstraction16, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate39 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction15 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate39.test(indexAbstraction15, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate38 = kibanaRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction14 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate38.test(indexAbstraction14, IndexComponentSelector.DATA), is(false));

        Arrays.asList(
            ".kibana",
            ".kibana-devnull",
            ".reporting-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            ".apm-agent-configuration",
            ".apm-custom-link",
            ".apm-source-map",
            ReservedRolesStore.ALERTS_LEGACY_INDEX + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.ALERTS_LEGACY_INDEX_REINDEXED_V8 + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.ALERTS_BACKING_INDEX + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.ALERTS_BACKING_INDEX_REINDEXED + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.ALERTS_INDEX_ALIAS + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.PREVIEW_ALERTS_INDEX_ALIAS + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.PREVIEW_ALERTS_BACKING_INDEX + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.PREVIEW_ALERTS_BACKING_INDEX_REINDEXED + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.LISTS_INDEX + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.LISTS_INDEX_REINDEXED_V8 + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.LISTS_ITEMS_INDEX + randomAlphaOfLength(randomIntBetween(0, 13)),
            ReservedRolesStore.LISTS_ITEMS_INDEX_REINDEXED_V8 + randomAlphaOfLength(randomIntBetween(0, 13)),
            ".slo-observability." + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach(index -> assertAllIndicesAccessAllowed(kibanaRole, index));

        // read-only index access, including cross cluster
        Arrays.asList(".monitoring-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            logger.info("index name [{}]", index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // read-only index access, excluding cross cluster
        Arrays.asList(
            ".ml-anomalies-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            ".ml-stats-" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((index) -> {
            logger.trace("index name [{}]", index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
        });

        // read/write index access, excluding cross cluster
        Arrays.asList(
            ".ml-annotations-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            ".ml-notifications-" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((index) -> {
            logger.trace("index name [{}]", index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
        });

        // read-only indices for APM telemetry
        Arrays.asList("apm-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            IndexAbstraction indexAbstraction11 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction11, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // read-only indices for APM telemetry under Fleet
        Arrays.asList(
            "traces-apm-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "traces-apm." + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-apm." + randomAlphaOfLength(randomIntBetween(0, 13)),
            "metrics-apm." + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((index) -> {
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // read-only indices for Endpoint diagnostic information
        Arrays.asList(".logs-endpoint.diagnostic.collection-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            IndexAbstraction indexAbstraction13 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction13, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            IndexAbstraction indexAbstraction12 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction12, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            IndexAbstraction indexAbstraction11 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction11, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));

            // Privileges needed for Fleet package upgrades
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
            // Privileges needed for installing current ILM policy with delete action
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // read-only indices for Endpoint events (to build timelines)
        Arrays.asList("logs-endpoint.events.process-default-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // read-only indices for Endpoint events (to build timelines)
        Arrays.asList("logs-endpoint.events.network-default-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        Arrays.asList(
            ".fleet-agents",
            ".fleet-actions",
            ".fleet-enrollment-api-keys",
            ".fleet-policies",
            ".fleet-actions-results",
            ".fleet-servers",
            ".fleet-fileds"
        ).forEach(index -> assertAllIndicesAccessAllowed(kibanaRole, index));

        final IndexAbstraction dotFleetSecretsIndex = mockIndexAbstraction(".fleet-secrets");
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate37 = kibanaRole.indices()
            .allowedIndicesMatcher("indices:foo");
        assertThat(isResourceAuthorizedPredicate37.test(dotFleetSecretsIndex, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate36 = kibanaRole.indices()
            .allowedIndicesMatcher("indices:bar");
        assertThat(isResourceAuthorizedPredicate36.test(dotFleetSecretsIndex, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate35 = kibanaRole.indices()
            .allowedIndicesMatcher(GetIndexAction.NAME);
        assertThat(isResourceAuthorizedPredicate35.test(dotFleetSecretsIndex, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate34 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        assertThat(isResourceAuthorizedPredicate34.test(dotFleetSecretsIndex, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate33 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        assertThat(isResourceAuthorizedPredicate33.test(dotFleetSecretsIndex, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate32 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        assertThat(isResourceAuthorizedPredicate32.test(dotFleetSecretsIndex, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate31 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        assertThat(isResourceAuthorizedPredicate31.test(dotFleetSecretsIndex, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate30 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
        assertThat(isResourceAuthorizedPredicate30.test(dotFleetSecretsIndex, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate29 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        assertThat(isResourceAuthorizedPredicate29.test(dotFleetSecretsIndex, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate28 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        assertThat(isResourceAuthorizedPredicate28.test(dotFleetSecretsIndex, IndexComponentSelector.DATA), is(false));

        assertThat(kibanaRole.cluster().check("cluster:admin/fleet/secrets/get", request, authentication), is(false));
        assertThat(kibanaRole.cluster().check("cluster:admin/fleet/secrets/post", request, authentication), is(true));
        assertThat(kibanaRole.cluster().check("cluster:admin/fleet/secrets/delete", request, authentication), is(true));

        // read-only indices for Fleet telemetry
        Arrays.asList("logs-elastic_agent-default", "logs-elastic_agent.fleet_server-default").forEach((index) -> {
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // Elastic Defend internal index for response actions results
        Arrays.asList(".logs-endpoint.action.responses-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        Arrays.asList(".logs-osquery_manager.action.responses-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        Arrays.asList("logs-osquery_manager.action.responses-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // Tests for third-party agent indices that `kibana_system` has only `read` access
        Arrays.asList(
            "logs-sentinel_one." + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-crowdstrike." + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-microsoft_defender_endpoint." + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-m365_defender." + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // Index for Endpoint specific actions
        Arrays.asList(".logs-endpoint.actions-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // read-only index for Endpoint specific heartbeats
        Arrays.asList(".logs-endpoint.heartbeat-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // index for Security Solution workflow insights
        Arrays.asList(".edr-workflow-insights-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // Data telemetry reads mappings, metadata and stats of indices
        Arrays.asList(randomAlphaOfLengthBetween(8, 24), "packetbeat-*").forEach((index) -> {
            logger.info("index name [{}]", index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(IndicesStatsAction.NAME);
            IndexAbstraction indexAbstraction11 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction11, IndexComponentSelector.DATA), is(true));
            assertViewIndexMetadata(kibanaRole, index);

            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
        });

        // Data telemetry does not have access to system indices that aren't specified
        List.of(".watches", ".geoip_databases", ".logstash", ".snapshot-blob-cache").forEach((index) -> {
            logger.info("index name [{}]", index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(GetMappingsAction.NAME);
            IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(IndicesStatsAction.NAME);
            IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
        });

        // Data telemetry does not have access to security and async search
        TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES.forEach((index) -> {
            logger.info("index name [{}]", index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(GetMappingsAction.NAME);
            IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(IndicesStatsAction.NAME);
            IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
        });

        // read-only datastream for Endpoint policy responses
        Arrays.asList("metrics-endpoint.policy-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // read-only datastream for Endpoint metrics
        Arrays.asList("metrics-endpoint.metrics-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((index) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // Beats management index
        final String index = ".management-beats";
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate27 = kibanaRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction11 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate27.test(indexAbstraction11, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate26 = kibanaRole.indices()
            .allowedIndicesMatcher("indices:bar");
        IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate26.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate25 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate25.test(indexAbstraction9, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate24 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate24.test(indexAbstraction8, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate23 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate23.test(indexAbstraction7, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate22 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate22.test(indexAbstraction6, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate21 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate21.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate20 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate20.test(indexAbstraction4, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate19 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate19.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate18 = kibanaRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate18.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate17 = kibanaRole.indices()
            .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate17.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));

        assertNoAccessAllowed(kibanaRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(kibanaRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));

        // Fleet package upgrade
        // 1. Pipeline
        Arrays.asList(
            GetPipelineAction.NAME,
            PutPipelineTransportAction.TYPE.name(),
            DeletePipelineTransportAction.TYPE.name(),
            SimulatePipelineAction.NAME,
            "cluster:admin/ingest/pipeline/" + randomAlphaOfLengthBetween(3, 8)
        ).forEach(action -> assertThat(kibanaRole.cluster().check(action, request, authentication), is(true)));

        // 2. ILM
        Arrays.asList(
            ILMActions.START.name(),
            DeleteLifecycleAction.NAME,
            GetLifecycleAction.NAME,
            GetStatusAction.NAME,
            ILMActions.MOVE_TO_STEP.name(),
            ILMActions.PUT.name(),
            ILMActions.STOP.name(),
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
            ".logs-endpoint.heartbeat-" + randomAlphaOfLengthBetween(3, 8),
            "profiling-" + randomAlphaOfLengthBetween(3, 8)
        ).forEach(indexName -> {
            logger.info("index name [{}]", indexName);
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:admin/data_stream/lifecycle/put");
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8));
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

            final boolean isAlsoAutoCreateIndex = indexName.startsWith(".logs-endpoint.actions-")
                || indexName.startsWith(".logs-endpoint.action.responses-");

            final boolean isAlsoCreateIndex = indexName.startsWith(".logs-endpoint.actions-")
                || indexName.startsWith(".logs-endpoint.action.responses-")
                || indexName.startsWith(".logs-endpoint.diagnostic.collection-")
                || indexName.startsWith(".logs-endpoint.heartbeat-");

            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(isAlsoCreateIndex));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(AutoCreateAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(isAlsoCreateIndex));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(CreateDataStreamAction.NAME);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(isAlsoCreateIndex));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(isAlsoAutoCreateIndex));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(isAlsoAutoCreateIndex));

            // Endpoint diagnostic and actions data streams also have read access, all others should not.
            final boolean isAlsoReadIndex = indexName.startsWith(".logs-endpoint.diagnostic.collection-")
                || indexName.startsWith(".logs-endpoint.actions-")
                || indexName.startsWith(".logs-endpoint.action.responses-")
                || indexName.startsWith(".logs-endpoint.heartbeat-")
                || indexName.startsWith(".logs-osquery_manager.actions-");
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(isAlsoReadIndex));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(isAlsoReadIndex));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(isAlsoReadIndex));

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
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(isAlsoIlmDeleteIndex));
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
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate16 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate16.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate15 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate15.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate14 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate14.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateAction.NAME);
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportBulkAction.NAME);
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            // Allow create and delete index, modifying aliases, and updating index settings
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(AutoCreateAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(CreateDataStreamAction.NAME);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(DeleteDataStreamAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(GetAliasesAction.NAME);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndicesAliasesAction.NAME);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8));
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

            // Granted by bwc for index privilege
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(
                isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA),
                is(indexAbstraction.getType() != IndexAbstraction.Type.DATA_STREAM)
            );

            // Deny deleting documents and rollover
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
        });

        // Test allow permissions on Threat Intel (ti*) dest indices created by latest transform : "create_index", "delete_index", "read",
        // "index", "delete", IndicesAliasesAction.NAME, UpdateSettingsAction.NAME
        Arrays.asList("logs-ti_recordedfuture_latest.threat", "logs-ti_anomali_latest.threatstream").forEach(indexName -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            // Allow search and indexing
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportBulkAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            // Allow create and delete index, modifying aliases, and updating index settings
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(GetAliasesAction.NAME);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndicesAliasesAction.NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

            // Allow deleting documents
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8));
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
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
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8));
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        Arrays.asList(
            ".logs-osquery_manager.actions-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            ".logs-osquery_manager.action.responses-" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach(indexName -> {
            logger.info("index name [{}]", indexName);
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            // Allow indexing
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateAction.NAME);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportBulkAction.NAME);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            // Allow create and delete index
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(AutoCreateAction.NAME);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(CreateDataStreamAction.NAME);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8));
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // read-only index for Osquery actions responses
        Arrays.asList("logs-osquery_manager.action.responses-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((cspIndex) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(cspIndex);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // read-only datastream for csp indices
        Arrays.asList("logs-cloud_security_posture.findings-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((cspIndex) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(cspIndex);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            // Ensure privileges necessary for ILM policies in Cloud Security Posture Package
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        Arrays.asList("logs-cloud_security_posture.vulnerabilities-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((cspIndex) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(cspIndex);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate14 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate14.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            // Ensure privileges necessary for ILM policies in Cloud Security Posture Package
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        Arrays.asList(
            "logs-wiz.vulnerability-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-wiz.cloud_configuration_finding-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-wiz.cloud_configuration_finding_full_posture-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-google_scc.finding-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-aws.securityhub_findings-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-aws.securityhub_findings_full_posture-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-aws.inspector-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-aws.config-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-amazon_security_lake.findings-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-qualys_vmdr.asset_host_detection-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-tenable_sc.vulnerability-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-tenable_io.vulnerability-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-rapid7_insightvm.vulnerability-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-carbon_black_cloud.asset_vulnerability_summary-" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach(indexName -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        Arrays.asList(
            "logs-cloud_security_posture.findings_latest-default",
            "logs-cloud_security_posture.scores-default",
            "logs-cloud_security_posture.vulnerabilities_latest-default",
            "logs-cloud_security_posture.findings_latest-default-" + Version.CURRENT,
            "logs-cloud_security_posture.scores-default-" + Version.CURRENT,
            "logs-cloud_security_posture.vulnerabilities_latest-default" + Version.CURRENT,
            "security_solution-*.vulnerability_latest-" + Version.CURRENT,
            "security_solution-*.misconfiguration_latest-" + Version.CURRENT
        ).forEach(indexName -> {
            logger.info("index name [{}]", indexName);
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            // Allow indexing
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportBulkAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            // Allow create and delete index, modifying aliases, and updating index settings
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(AutoCreateAction.NAME);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(CreateDataStreamAction.NAME);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(GetAliasesAction.NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndicesAliasesAction.NAME);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8));
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

        });

        // cloud_defend
        // read-only datastream for cloud_defend indices (for usageCollection)
        Arrays.asList(
            "logs-cloud_defend.file-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-cloud_defend.process-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "logs-cloud_defend.alerts-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "metrics-cloud_defend.metrics-" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((indexName) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            assertViewIndexMetadata(kibanaRole, indexName);
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

            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        // Example transform package
        Arrays.asList("kibana_sample_data_ecommerce", "kibana_sample_data_ecommerce_transform" + randomInt()).forEach(indexName -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            // Allow search and indexing
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportBulkAction.NAME);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            // Allow create and delete index, modifying aliases, and updating index settings
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(GetAliasesAction.NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndicesAliasesAction.NAME);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

            // Implied by the overall view_index_metadata and monitor privilege
            assertViewIndexMetadata(kibanaRole, indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher("indices:monitor/" + randomAlphaOfLengthBetween(3, 8));
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });

        Arrays.asList("risk-score.risk-score-" + randomAlphaOfLength(randomIntBetween(0, 13)))
            .forEach(indexName -> assertAllIndicesAccessAllowed(kibanaRole, indexName));

        Arrays.asList(".asset-criticality.asset-criticality-" + randomAlphaOfLength(randomIntBetween(0, 13))).forEach(indexName -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            assertViewIndexMetadata(kibanaRole, indexName);
        });

        Arrays.asList("metrics-logstash." + randomAlphaOfLength(randomIntBetween(0, 13))).forEach((indexName) -> {
            final IndexAbstraction indexAbstraction = mockIndexAbstraction(indexName);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:foo");
            assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = kibanaRole.indices()
                .allowedIndicesMatcher("indices:bar");
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = kibanaRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = kibanaRole.indices()
                .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaRole.indices()
                .allowedIndicesMatcher(TransportPutMappingAction.TYPE.name());
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaRole.indices()
                .allowedIndicesMatcher(RolloverAction.NAME);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });
    }

    public void testKibanaAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("kibana_admin");
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
        assertThat(kibanaAdminRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(kibanaAdminRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(kibanaAdminRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaAdminRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaAdminRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaAdminRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));

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

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("kibana_user");
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
        assertThat(kibanaUserRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(kibanaUserRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(kibanaUserRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = kibanaUserRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = kibanaUserRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = kibanaUserRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));

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

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("monitoring_user");
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
        assertThat(monitoringUserRole.cluster().check(MainRestPlugin.MAIN_ACTION.name(), request, authentication), is(true));
        assertThat(monitoringUserRole.cluster().check(XPackInfoAction.NAME, request, authentication), is(true));
        assertThat(monitoringUserRole.cluster().check(TransportRemoteInfoAction.TYPE.name(), request, authentication), is(true));
        assertThat(monitoringUserRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
        assertThat(monitoringUserRole.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));

        assertThat(monitoringUserRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate16 = monitoringUserRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction16 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate16.test(indexAbstraction16, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate15 = monitoringUserRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction15 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate15.test(indexAbstraction15, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate14 = monitoringUserRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction14 = mockIndexAbstraction(".kibana");
        assertThat(isResourceAuthorizedPredicate14.test(indexAbstraction14, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = monitoringUserRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction13 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction13, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = monitoringUserRole.indices()
            .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
        IndexAbstraction indexAbstraction12 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction12, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = monitoringUserRole.indices()
            .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
        IndexAbstraction indexAbstraction11 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction11, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = monitoringUserRole.indices()
            .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
        IndexAbstraction indexAbstraction10 = mockIndexAbstraction(".kibana");
        assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));

        final String index = ".monitoring-" + randomAlphaOfLength(randomIntBetween(0, 13));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = monitoringUserRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = monitoringUserRole.indices()
            .allowedIndicesMatcher("indices:bar");
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = monitoringUserRole.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = monitoringUserRole.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = monitoringUserRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = monitoringUserRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = monitoringUserRole.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = monitoringUserRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = monitoringUserRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = monitoringUserRole.indices()
            .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

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

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("remote_monitoring_agent");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role remoteMonitoringAgentRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );
        assertThat(remoteMonitoringAgentRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringAgentRole.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(true));
        assertThat(
            remoteMonitoringAgentRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication),
            is(true)
        );
        assertThat(
            remoteMonitoringAgentRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication),
            is(false)
        );
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
        assertThat(remoteMonitoringAgentRole.cluster().check(ILMActions.PUT.name(), request, authentication), is(true));

        // we get this from the cluster:monitor privilege
        assertThat(remoteMonitoringAgentRole.cluster().check(WatcherStatsAction.NAME, request, authentication), is(true));

        assertThat(remoteMonitoringAgentRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate28 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction28 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate28.test(indexAbstraction28, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate27 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction27 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate27.test(indexAbstraction27, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate26 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction26 = mockIndexAbstraction(".kibana");
        assertThat(isResourceAuthorizedPredicate26.test(indexAbstraction26, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate25 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction25 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate25.test(indexAbstraction25, IndexComponentSelector.DATA), is(false));

        final String monitoringIndex = ".monitoring-" + randomAlphaOfLength(randomIntBetween(0, 13));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate24 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction24 = mockIndexAbstraction(monitoringIndex);
        assertThat(isResourceAuthorizedPredicate24.test(indexAbstraction24, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate23 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher("indices:bar");
        IndexAbstraction indexAbstraction23 = mockIndexAbstraction(monitoringIndex);
        assertThat(isResourceAuthorizedPredicate23.test(indexAbstraction23, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate22 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction22 = mockIndexAbstraction(monitoringIndex);
        assertThat(isResourceAuthorizedPredicate22.test(indexAbstraction22, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate21 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction21 = mockIndexAbstraction(monitoringIndex);
        assertThat(isResourceAuthorizedPredicate21.test(indexAbstraction21, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate20 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction20 = mockIndexAbstraction(monitoringIndex);
        assertThat(isResourceAuthorizedPredicate20.test(indexAbstraction20, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate19 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction19 = mockIndexAbstraction(monitoringIndex);
        assertThat(isResourceAuthorizedPredicate19.test(indexAbstraction19, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate18 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        IndexAbstraction indexAbstraction18 = mockIndexAbstraction(monitoringIndex);
        assertThat(isResourceAuthorizedPredicate18.test(indexAbstraction18, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate17 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction17 = mockIndexAbstraction(monitoringIndex);
        assertThat(isResourceAuthorizedPredicate17.test(indexAbstraction17, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate16 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction16 = mockIndexAbstraction(monitoringIndex);
        assertThat(isResourceAuthorizedPredicate16.test(indexAbstraction16, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate15 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(GetIndexAction.NAME);
        IndexAbstraction indexAbstraction15 = mockIndexAbstraction(monitoringIndex);
        assertThat(isResourceAuthorizedPredicate15.test(indexAbstraction15, IndexComponentSelector.DATA), is(true));

        final String metricbeatIndex = "metricbeat-" + randomAlphaOfLength(randomIntBetween(0, 13));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate14 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction14 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate14.test(indexAbstraction14, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher("indices:bar");
        IndexAbstraction indexAbstraction13 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction13, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction12 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction12, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction11 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction11, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction10 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(GetIndexAction.NAME);
        IndexAbstraction indexAbstraction9 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(GetAliasesAction.NAME);
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportIndicesAliasesAction.NAME);
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(RolloverAction.NAME);
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(IndicesSegmentsAction.NAME);
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(RemoveIndexLifecyclePolicyAction.NAME);
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = remoteMonitoringAgentRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction = mockIndexAbstraction(metricbeatIndex);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));

        assertNoAccessAllowed(remoteMonitoringAgentRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(remoteMonitoringAgentRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testRemoteMonitoringCollectorRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("remote_monitoring_collector");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role remoteMonitoringCollectorRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );
        assertThat(remoteMonitoringCollectorRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(
            remoteMonitoringCollectorRole.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication),
            is(true)
        );
        assertThat(remoteMonitoringCollectorRole.cluster().check(GetIndexTemplatesAction.NAME, request, authentication), is(true));
        assertThat(
            remoteMonitoringCollectorRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication),
            is(false)
        );
        assertThat(
            remoteMonitoringCollectorRole.cluster().check(TransportDeleteIndexTemplateAction.TYPE.name(), request, authentication),
            is(false)
        );
        assertThat(
            remoteMonitoringCollectorRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication),
            is(false)
        );
        assertThat(remoteMonitoringCollectorRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(remoteMonitoringCollectorRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(remoteMonitoringCollectorRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate23 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(RecoveryAction.NAME);
        IndexAbstraction indexAbstraction23 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate23.test(indexAbstraction23, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate22 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction22 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate22.test(indexAbstraction22, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate21 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction21 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate21.test(indexAbstraction21, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate20 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction20 = mockIndexAbstraction(".kibana");
        assertThat(isResourceAuthorizedPredicate20.test(indexAbstraction20, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate19 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction19 = mockIndexAbstraction(".kibana");
        assertThat(isResourceAuthorizedPredicate19.test(indexAbstraction19, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate18 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction18 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate18.test(indexAbstraction18, IndexComponentSelector.DATA), is(false));

        Arrays.asList(
            ".monitoring-" + randomAlphaOfLength(randomIntBetween(0, 13)),
            "metricbeat-" + randomAlphaOfLength(randomIntBetween(0, 13))
        ).forEach((index) -> {
            logger.info("index name [{}]", index);
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher("indices:foo");
            IndexAbstraction indexAbstraction12 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction12, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher("indices:bar");
            IndexAbstraction indexAbstraction11 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction11, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
            IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(TransportIndexAction.NAME);
            IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(GetAliasesAction.NAME);
            IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(RemoveIndexLifecyclePolicyAction.NAME);
            IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(TransportDeleteAction.NAME);
            IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
            IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
            IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(TransportGetAction.TYPE.name());
            IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = remoteMonitoringCollectorRole.indices()
                .allowedIndicesMatcher(GetIndexAction.NAME);
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
        });

        // These tests might need to change if we add new non-security restricted indices that the monitoring user isn't supposed to see
        // (but ideally, the monitoring user should see all indices).
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate17 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(GetSettingsAction.NAME);
        IndexAbstraction indexAbstraction17 = mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES));
        assertThat(isResourceAuthorizedPredicate17.test(indexAbstraction17, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate16 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(GetSettingsAction.NAME);
        IndexAbstraction indexAbstraction16 = mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
        assertThat(isResourceAuthorizedPredicate16.test(indexAbstraction16, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate15 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportIndicesShardStoresAction.TYPE.name());
        IndexAbstraction indexAbstraction15 = mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES));
        assertThat(isResourceAuthorizedPredicate15.test(indexAbstraction15, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate14 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportIndicesShardStoresAction.TYPE.name());
        IndexAbstraction indexAbstraction14 = mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
        assertThat(isResourceAuthorizedPredicate14.test(indexAbstraction14, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(RecoveryAction.NAME);
        IndexAbstraction indexAbstraction13 = mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES));
        assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction13, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(RecoveryAction.NAME);
        IndexAbstraction indexAbstraction12 = mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
        assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction12, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(IndicesStatsAction.NAME);
        IndexAbstraction indexAbstraction11 = mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES));
        assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction11, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(IndicesStatsAction.NAME);
        IndexAbstraction indexAbstraction10 = mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
        assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(IndicesSegmentsAction.NAME);
        IndexAbstraction indexAbstraction9 = mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES));
        assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(IndicesSegmentsAction.NAME);
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(true));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES));
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES));
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES));
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES));
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = remoteMonitoringCollectorRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction(XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));

        assertMonitoringOnRestrictedIndices(remoteMonitoringCollectorRole);

        assertNoAccessAllowed(remoteMonitoringCollectorRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(remoteMonitoringCollectorRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    private void assertMonitoringOnRestrictedIndices(Role role) {
        final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
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
            TransportIndicesShardStoresAction.TYPE.name(),
            RecoveryAction.NAME
        );
        for (final String indexMonitoringActionName : indexMonitoringActionNamesList) {
            String asyncSearchIndex = XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2);
            final IndicesAccessControl iac = role.indices()
                .authorize(
                    indexMonitoringActionName,
                    Sets.newHashSet(internalSecurityIndex, TestRestrictedIndices.SECURITY_MAIN_ALIAS, asyncSearchIndex),
                    metadata.getProject(),
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

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("reporting_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        final String applicationName = "kibana-.kibana";

        final Set<String> applicationPrivilegeNames = Set.of(
            "feature_discover.minimal_read",
            "feature_discover.generate_report",
            "feature_dashboard.minimal_read",
            "feature_dashboard.generate_report",
            "feature_dashboard.download_csv_report",
            "feature_canvas.minimal_read",
            "feature_canvas.generate_report",
            "feature_visualize.minimal_read",
            "feature_visualize.generate_report"
        );

        final Set<String> allowedApplicationActionPatterns = Set.of(
            "login:",
            "app:discover",
            "app:canvas",
            "app:kibana",
            "ui:catalogue/canvas",
            "ui:navLinks/canvas",
            "ui:catalogue/discover",
            "ui:navLinks/discover",
            "ui:navLinks/kibana",
            "saved_object:index-pattern/*",
            "saved_object:search/*",
            "saved_object:query/*",
            "saved_object:config/*",
            "saved_object:config/get",
            "saved_object:config/find",
            "saved_object:config-global/*",
            "saved_object:telemetry/*",
            "saved_object:canvas-workpad/*",
            "saved_object:canvas-element/*",
            "saved_object:url/*",
            "ui:discover/show"
        );

        final List<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors = new ArrayList<>();
        for (String appPrivilegeName : applicationPrivilegeNames) {
            applicationPrivilegeDescriptors.add(
                new ApplicationPrivilegeDescriptor(applicationName, appPrivilegeName, allowedApplicationActionPatterns, Map.of())
            );
        }

        Role reportingUserRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES,
            applicationPrivilegeDescriptors
        );
        assertThat(reportingUserRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(reportingUserRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(reportingUserRole.runAs().check(randomAlphaOfLengthBetween(1, 12)), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate14 = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction14 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate14.test(indexAbstraction14, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction13 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction13, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction12 = mockIndexAbstraction(".kibana");
        assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction12, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = reportingUserRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction11 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction11, IndexComponentSelector.DATA), is(false));

        final String index = ".reporting-" + randomAlphaOfLength(randomIntBetween(0, 13));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = reportingUserRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = reportingUserRole.indices()
            .allowedIndicesMatcher("indices:bar");
        IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportUpdateAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = reportingUserRole.indices()
            .allowedIndicesMatcher(TransportBulkAction.NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));

        assertNoAccessAllowed(reportingUserRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(reportingUserRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));

        applicationPrivilegeNames.forEach(appPrivilege -> {
            assertThat(
                reportingUserRole.application()
                    .grants(
                        ApplicationPrivilegeTests.createPrivilege(
                            applicationName,
                            appPrivilege,
                            allowedApplicationActionPatterns.toArray(new String[0])
                        ),
                        "*"
                    ),
                is(true)
            );
        });
        assertThat(
            reportingUserRole.application()
                .grants(
                    ApplicationPrivilegeTests.createPrivilege(
                        "kibana-.*",
                        "feature_random.minimal_read",
                        allowedApplicationActionPatterns.toArray(new String[0])
                    ),
                    "*"
                ),
            is(false)
        );
    }

    public void testSuperuserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("superuser");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role superuserRole = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
        assertThat(superuserRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(PutUserAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(PutRoleAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(true));
        assertThat(superuserRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(true));
        assertThat(superuserRole.cluster().check("internal:admin/foo", request, authentication), is(false));
        assertThat(
            superuserRole.cluster().check(UpdateProfileDataAction.NAME, mock(UpdateProfileDataRequest.class), authentication),
            is(true)
        );
        assertThat(superuserRole.cluster().check(GetProfilesAction.NAME, mock(UpdateProfileDataRequest.class), authentication), is(true));
        assertThat(superuserRole.cluster().check(SuggestProfilesAction.NAME, mock(SuggestProfilesRequest.class), authentication), is(true));
        assertThat(superuserRole.cluster().check(ActivateProfileAction.NAME, mock(ActivateProfileRequest.class), authentication), is(true));

        final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        final var metadata = new Metadata.Builder().put(
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
            .build()
            .getProject();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        IndicesAccessControl iac = superuserRole.indices()
            .authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("a1", "ba"), metadata, fieldPermissionsCache);
        assertThat(iac.hasIndexPermissions("a1"), is(true));
        assertThat(iac.hasIndexPermissions("b"), is(true));
        iac = superuserRole.indices()
            .authorize(TransportDeleteIndexAction.TYPE.name(), Sets.newHashSet("a1", "ba"), metadata, fieldPermissionsCache);
        assertThat(iac.hasIndexPermissions("a1"), is(true));
        assertThat(iac.hasIndexPermissions("b"), is(true));
        iac = superuserRole.indices().authorize(TransportIndexAction.NAME, Sets.newHashSet("a2", "ba"), metadata, fieldPermissionsCache);
        assertThat(iac.hasIndexPermissions("a2"), is(true));
        assertThat(iac.hasIndexPermissions("b"), is(true));
        iac = superuserRole.indices()
            .authorize(TransportUpdateSettingsAction.TYPE.name(), Sets.newHashSet("aaaaaa", "ba"), metadata, fieldPermissionsCache);
        assertThat(iac.hasIndexPermissions("aaaaaa"), is(true));
        assertThat(iac.hasIndexPermissions("b"), is(true));

        // Read security indices => allowed
        iac = superuserRole.indices()
            .authorize(
                randomFrom(TransportSearchAction.TYPE.name(), GetIndexAction.NAME),
                Sets.newHashSet(TestRestrictedIndices.SECURITY_MAIN_ALIAS),
                metadata,
                fieldPermissionsCache
            );
        assertThat("For " + iac, iac.hasIndexPermissions(TestRestrictedIndices.SECURITY_MAIN_ALIAS), is(true));
        assertThat("For " + iac, iac.hasIndexPermissions(internalSecurityIndex), is(true));

        // Write security indices => denied
        iac = superuserRole.indices()
            .authorize(
                randomFrom(TransportIndexAction.NAME, TransportDeleteIndexAction.TYPE.name()),
                Sets.newHashSet(TestRestrictedIndices.SECURITY_MAIN_ALIAS),
                metadata,
                fieldPermissionsCache
            );
        assertThat("For " + iac, iac.hasIndexPermissions(TestRestrictedIndices.SECURITY_MAIN_ALIAS), is(false));
        assertThat("For " + iac, iac.hasIndexPermissions(internalSecurityIndex), is(false));

        assertTrue(superuserRole.indices().check(TransportSearchAction.TYPE.name()));
        assertFalse(superuserRole.indices().check("unknown"));

        assertThat(superuserRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(true));

        // Read security indices => allowed
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = superuserRole.indices()
            .allowedIndicesMatcher(randomFrom(TransportGetAction.TYPE.name(), IndicesStatsAction.NAME));
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(TestRestrictedIndices.SECURITY_MAIN_ALIAS);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = superuserRole.indices()
            .allowedIndicesMatcher(randomFrom(TransportGetAction.TYPE.name(), IndicesStatsAction.NAME));
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(internalSecurityIndex);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));

        // Write security indices => denied
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = superuserRole.indices()
            .allowedIndicesMatcher(randomFrom(TransportIndexAction.NAME, TransportDeleteIndexAction.TYPE.name()));
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(TestRestrictedIndices.SECURITY_MAIN_ALIAS);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = superuserRole.indices()
            .allowedIndicesMatcher(randomFrom(TransportIndexAction.NAME, TransportDeleteIndexAction.TYPE.name()));
        IndexAbstraction indexAbstraction = mockIndexAbstraction(internalSecurityIndex);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
        assertThat(
            superuserRole.remoteCluster().collapseAndRemoveUnsupportedPrivileges("*", TransportVersion.current()),
            equalTo(RemoteClusterPermissions.getSupportedRemoteClusterPermissions().toArray(new String[0]))
        );
    }

    public void testLogstashSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("logstash_system");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role logstashSystemRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );
        assertThat(logstashSystemRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(logstashSystemRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(logstashSystemRole.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(true));
        assertThat(logstashSystemRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(logstashSystemRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
        assertThat(logstashSystemRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(logstashSystemRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(logstashSystemRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(true));

        assertThat(logstashSystemRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = logstashSystemRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = logstashSystemRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = logstashSystemRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));

        assertNoAccessAllowed(logstashSystemRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(logstashSystemRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testBeatsAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        final RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("beats_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        final Role beatsAdminRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );
        assertThat(beatsAdminRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ActivateProfileAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(SuggestProfilesAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(UpdateProfileDataAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(GetProfilesAction.NAME, request, authentication), is(false));
        assertThat(beatsAdminRole.cluster().check(ProfileHasPrivilegesAction.NAME, request, authentication), is(false));

        assertThat(beatsAdminRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = beatsAdminRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction10 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));

        final String index = ".management-beats";
        logger.info("index name [{}]", index);
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = beatsAdminRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = beatsAdminRole.indices()
            .allowedIndicesMatcher("indices:bar");
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = beatsAdminRole.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = beatsAdminRole.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = beatsAdminRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = beatsAdminRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = beatsAdminRole.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = beatsAdminRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = beatsAdminRole.indices()
            .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = beatsAdminRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

        assertNoAccessAllowed(beatsAdminRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(beatsAdminRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testBeatsSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor(UsernamesField.BEATS_ROLE);
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role beatsSystemRole = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
        assertThat(beatsSystemRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(beatsSystemRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(beatsSystemRole.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(true));
        assertThat(beatsSystemRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(beatsSystemRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
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
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = beatsSystemRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = beatsSystemRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = beatsSystemRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = beatsSystemRole.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = beatsSystemRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = beatsSystemRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = beatsSystemRole.indices()
            .allowedIndicesMatcher(TransportBulkAction.NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));

        assertNoAccessAllowed(beatsSystemRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(beatsSystemRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testAPMSystemRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor(UsernamesField.APM_ROLE);
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role APMSystemRole = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
        assertThat(APMSystemRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(true));
        assertThat(APMSystemRole.cluster().check(ClusterStateAction.NAME, request, authentication), is(true));
        assertThat(APMSystemRole.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(true));
        assertThat(APMSystemRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(APMSystemRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
        assertThat(APMSystemRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(APMSystemRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));
        assertThat(APMSystemRole.cluster().check(MonitoringBulkAction.NAME, request, authentication), is(true));

        assertThat(APMSystemRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = APMSystemRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = APMSystemRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = APMSystemRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));

        final String index = ".monitoring-beats-" + randomIntBetween(10, 15);
        logger.info("APM beats monitoring index name [{}]", index);

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = APMSystemRole.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = APMSystemRole.indices()
            .allowedIndicesMatcher("indices:data/write/index:op_type/create");
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = APMSystemRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = APMSystemRole.indices()
            .allowedIndicesMatcher(TransportBulkAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = APMSystemRole.indices()
            .allowedIndicesMatcher("indices:data/write/index:op_type/index");
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = APMSystemRole.indices()
            .allowedIndicesMatcher("indices:data/write/index:op_type/" + randomAlphaOfLengthBetween(3, 5));
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));

        assertNoAccessAllowed(APMSystemRole, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(APMSystemRole, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testMachineLearningAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("machine_learning_admin");
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

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("machine_learning_user");
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

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("transform_admin");

        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
        assertThat(roleDescriptor.getMetadata(), not(hasEntry("_deprecated", true)));

        final String allowedApplicationActionPattern = "example/custom/action/*";
        final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
        List<ApplicationPrivilegeDescriptor> lookup = List.of();
        Role role = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES, lookup);
        assertThat(role.cluster().check(DeleteTransformAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetTransformAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetTransformStatsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PreviewTransformAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PutTransformAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(StartTransformAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(StopTransformAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(SetTransformUpgradeModeAction.NAME, request, authentication), is(true));
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
            role.application().grants(ApplicationPrivilegeTests.createPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"),
            is(false)
        );

        final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(
            role.application().grants(ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-foo", "foo"), "*"),
            is(false)
        );
    }

    public void testTransformUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("transform_user");

        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));
        assertThat(roleDescriptor.getMetadata(), not(hasEntry("_deprecated", true)));

        final String allowedApplicationActionPattern = "example/custom/action/*";
        final String kibanaApplicationWithRandomIndex = "kibana-" + randomFrom(randomAlphaOfLengthBetween(8, 24), ".kibana");
        List<ApplicationPrivilegeDescriptor> lookup = List.of();
        Role role = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES, lookup);
        assertThat(role.cluster().check(DeleteTransformAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(GetTransformAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(GetTransformStatsAction.NAME, request, authentication), is(true));
        assertThat(role.cluster().check(PreviewTransformAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(PutTransformAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(StartTransformAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(StopTransformAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(SetTransformUpgradeModeAction.NAME, request, authentication), is(false));
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
            role.application().grants(ApplicationPrivilegeTests.createPrivilege(kibanaApplicationWithRandomIndex, "app-foo", "foo"), "*"),
            is(false)
        );

        final String otherApplication = "logstash-" + randomAlphaOfLengthBetween(8, 24);
        assertThat(
            role.application().grants(ApplicationPrivilegeTests.createPrivilege(otherApplication, "app-foo", "foo"), "*"),
            is(false)
        );
    }

    public void testWatcherAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("watcher_admin");
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

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = role.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));

        for (String index : new String[] { Watch.INDEX, HistoryStoreField.DATA_STREAM, TriggeredWatchStoreField.INDEX_NAME }) {
            assertOnlyReadAllowed(role, index);
        }

        assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(role, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testWatcherUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("watcher_user");
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

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = role.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = role.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction(TriggeredWatchStoreField.INDEX_NAME);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));

        for (String index : new String[] { Watch.INDEX, HistoryStoreField.DATA_STREAM }) {
            assertOnlyReadAllowed(role, index);
        }

        assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(role, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    public void testPredefinedViewerRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("viewer");
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
        assertThat(role.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(false));
        assertThat(role.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(role.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
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
        assertOnlyReadAllowed(role, "profiling-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, ".profiling-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, randomAlphaOfLength(5));

        assertOnlyReadAllowed(role, ".slo-observability." + randomIntBetween(0, 5));
        assertViewIndexMetadata(role, ".slo-observability." + randomIntBetween(0, 5));

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

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("editor");
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
        assertThat(role.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(ClusterStateAction.NAME, request, authentication), is(false));
        assertThat(role.cluster().check(TransportClusterStatsAction.TYPE.name(), request, authentication), is(false));
        assertThat(role.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(role.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
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
        assertOnlyReadAllowed(role, "profiling-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, ".profiling-" + randomIntBetween(0, 5));
        assertOnlyReadAllowed(role, randomAlphaOfLength(5));

        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".siem-signals-" + randomIntBetween(0, 5));
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".lists-" + randomIntBetween(0, 5));
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".items-" + randomIntBetween(0, 5));
        assertReadWriteDocsButNotDeleteIndexAllowed(role, "observability-annotations");
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".alerts-" + randomIntBetween(0, 5));
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".internal.alerts-" + randomIntBetween(0, 5));
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".preview.alerts-" + randomIntBetween(0, 5));
        assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(role, ".internal.preview.alerts-" + randomIntBetween(0, 5));

        assertViewIndexMetadata(role, ".slo-observability." + randomIntBetween(0, 5));
        assertReadWriteAndManage(role, ".slo-observability." + randomIntBetween(0, 5));

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
        final RoleDescriptor superuserRoleDescriptor = ReservedRolesStore.roleDescriptor("superuser");
        final Role superuserRole = Role.buildFromRoleDescriptor(
            superuserRoleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );

        assertThat(superuserRoleDescriptor.hasRemoteIndicesPrivileges(), is(true));
        assertThat(
            Arrays.stream(superuserRoleDescriptor.getRemoteIndicesPrivileges())
                .map(RoleDescriptor.RemoteIndicesPrivileges::indicesPrivileges)
                .toArray(RoleDescriptor.IndicesPrivileges[]::new),
            equalTo(superuserRoleDescriptor.getIndicesPrivileges())
        );
        final List<RemoteIndicesPermission.RemoteIndicesGroup> remoteIndicesGroups = superuserRole.remoteIndices().remoteIndicesGroups();
        assertThat(remoteIndicesGroups.size(), equalTo(1));
        assertThat(remoteIndicesGroups.get(0).remoteClusterAliasMatcher().isTotal(), is(true));
    }

    public void testRemoteIndicesPrivileges() {
        final List<String> rolesWithRemoteIndicesPrivileges = new ArrayList<>();

        for (RoleDescriptor roleDescriptor : ReservedRolesStore.roleDescriptors()) {
            if (roleDescriptor.getName().equals("superuser")) {
                continue;  // superuser is tested separately
            }
            final Role role = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);

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
        }

        assertThat(rolesWithRemoteIndicesPrivileges, containsInAnyOrder("kibana_system", "monitoring_user"));
    }

    /**
     * Ensures that all reserved roles are self-documented with a brief description.
     */
    public void testAllReservedRolesHaveDescription() {
        for (RoleDescriptor role : ReservedRolesStore.roleDescriptors()) {
            assertThat("reserved role [" + role.getName() + "] must have description", role.hasDescription(), is(true));
        }
    }

    private void assertAllIndicesAccessAllowed(Role role, String index) {
        logger.info("index name [{}]", index);
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = role.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction11 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction11, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = role.indices()
            .allowedIndicesMatcher("indices:bar");
        IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = role.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = role.indices()
            .allowedIndicesMatcher(GetIndexAction.NAME);
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = role.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = role.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = role.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = role.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = role.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = role.indices()
            .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = role.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
        // inherits from 'all'
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = role.indices()
            .allowedIndicesMatcher(READ_CROSS_CLUSTER_NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
    }

    private void assertReadWriteDocsAndMaintenanceButNotDeleteIndexAllowed(Role role, String index) {
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = role.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = role.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = role.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = role.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = role.indices()
            .allowedIndicesMatcher(TransportUpdateAction.NAME);
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = role.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = role.indices()
            .allowedIndicesMatcher(TransportBulkAction.NAME);
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = role.indices()
            .allowedIndicesMatcher("indices:admin/refresh*");
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = role.indices()
            .allowedIndicesMatcher("indices:admin/flush*");
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = role.indices()
            .allowedIndicesMatcher("indices:admin/synced_flush");
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = role.indices()
            .allowedIndicesMatcher("indices:admin/forcemerge*");
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
    }

    private void assertReadWriteDocsButNotDeleteIndexAllowed(Role role, String index) {
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = role.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = role.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = role.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = role.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = role.indices()
            .allowedIndicesMatcher(TransportUpdateAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = role.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = role.indices()
            .allowedIndicesMatcher(TransportBulkAction.NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
    }

    private void assertReadWriteAndManage(Role role, String index) {
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate14 = role.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction14 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate14.test(indexAbstraction14, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate13 = role.indices()
            .allowedIndicesMatcher(TransportFieldCapabilitiesAction.NAME + "*");
        IndexAbstraction indexAbstraction13 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate13.test(indexAbstraction13, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate12 = role.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction12 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate12.test(indexAbstraction12, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = role.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        IndexAbstraction indexAbstraction11 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction11, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = role.indices()
            .allowedIndicesMatcher(GetRollupIndexCapsAction.NAME + "*");
        IndexAbstraction indexAbstraction10 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = role.indices()
            .allowedIndicesMatcher("indices:admin/*");
        IndexAbstraction indexAbstraction9 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = role.indices()
            .allowedIndicesMatcher("indices:monitor/*");
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = role.indices()
            .allowedIndicesMatcher(TransportAutoPutMappingAction.TYPE.name());
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = role.indices()
            .allowedIndicesMatcher(AutoCreateAction.NAME);
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(true));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = role.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = role.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = role.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = role.indices()
            .allowedIndicesMatcher(TransportUpdateAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = role.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = role.indices()
            .allowedIndicesMatcher(TransportBulkAction.NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
    }

    private void assertOnlyReadAllowed(Role role, String index) {
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = role.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = role.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = role.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = role.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = role.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = role.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = role.indices()
            .allowedIndicesMatcher(TransportUpdateAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = role.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = role.indices()
            .allowedIndicesMatcher(TransportBulkAction.NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));

        assertNoAccessAllowed(role, TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES);
        assertNoAccessAllowed(role, XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2));
    }

    private void assertViewIndexMetadata(Role role, String index) {
        Arrays.asList(
            GetAliasesAction.NAME,
            GetIndexAction.NAME,
            GetFieldMappingsAction.NAME + "*",
            GetMappingsAction.NAME,
            TransportClusterSearchShardsAction.TYPE.name(),
            TransportSearchShardsAction.TYPE.name(),
            ValidateQueryAction.NAME + "*",
            GetSettingsAction.NAME,
            ExplainLifecycleAction.NAME,
            GetDataStreamAction.NAME,
            ResolveIndexAction.NAME,
            TransportFieldCapabilitiesAction.NAME + "*",
            GetRollupIndexCapsAction.NAME + "*"
        ).forEach(action -> {
            IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = role.indices().allowedIndicesMatcher(action);
            IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
            assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
        });
    }

    private void assertNoAccessAllowed(Role role, Collection<String> indices) {
        for (String index : indices) {
            assertNoAccessAllowed(role, index);
        }
    }

    private void assertNoAccessAllowed(Role role, String index) {
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = role.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = role.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = role.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = role.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = role.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = role.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = role.indices()
            .allowedIndicesMatcher(TransportUpdateAction.NAME);
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = role.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = role.indices()
            .allowedIndicesMatcher(TransportBulkAction.NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(false));
    }

    public void testLogstashAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("logstash_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role logstashAdminRole = Role.buildFromRoleDescriptor(
            roleDescriptor,
            new FieldPermissionsCache(Settings.EMPTY),
            RESTRICTED_INDICES
        );
        assertThat(logstashAdminRole.cluster().check(TransportClusterHealthAction.NAME, request, authentication), is(false));
        assertThat(logstashAdminRole.cluster().check(TransportPutIndexTemplateAction.TYPE.name(), request, authentication), is(false));
        assertThat(logstashAdminRole.cluster().check(TransportClusterRerouteAction.TYPE.name(), request, authentication), is(false));
        assertThat(logstashAdminRole.cluster().check(ClusterUpdateSettingsAction.NAME, request, authentication), is(false));
        assertThat(logstashAdminRole.cluster().check(DelegatePkiAuthenticationAction.NAME, request, authentication), is(false));

        assertThat(logstashAdminRole.cluster().check("cluster:admin/logstash/pipeline/delete", request, authentication), is(true));
        assertThat(logstashAdminRole.cluster().check("cluster:admin/logstash/pipeline/get", request, authentication), is(true));
        assertThat(logstashAdminRole.cluster().check("cluster:admin/logstash/pipeline/put", request, authentication), is(true));

        assertThat(logstashAdminRole.runAs().check(randomAlphaOfLengthBetween(1, 30)), is(false));

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate11 = logstashAdminRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction11 = mockIndexAbstraction("foo");
        assertThat(isResourceAuthorizedPredicate11.test(indexAbstraction11, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate10 = logstashAdminRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction10 = mockIndexAbstraction(".reporting");
        assertThat(isResourceAuthorizedPredicate10.test(indexAbstraction10, IndexComponentSelector.DATA), is(false));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate9 = logstashAdminRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction9 = mockIndexAbstraction(".logstash");
        assertThat(isResourceAuthorizedPredicate9.test(indexAbstraction9, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate8 = logstashAdminRole.indices()
            .allowedIndicesMatcher("indices:foo");
        IndexAbstraction indexAbstraction8 = mockIndexAbstraction(randomAlphaOfLengthBetween(8, 24));
        assertThat(isResourceAuthorizedPredicate8.test(indexAbstraction8, IndexComponentSelector.DATA), is(false));

        final String index = ".logstash-" + randomIntBetween(0, 5);

        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate7 = logstashAdminRole.indices()
            .allowedIndicesMatcher(TransportDeleteAction.NAME);
        IndexAbstraction indexAbstraction7 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate7.test(indexAbstraction7, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate6 = logstashAdminRole.indices()
            .allowedIndicesMatcher(TransportDeleteIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction6 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate6.test(indexAbstraction6, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate5 = logstashAdminRole.indices()
            .allowedIndicesMatcher(TransportCreateIndexAction.TYPE.name());
        IndexAbstraction indexAbstraction5 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate5.test(indexAbstraction5, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate4 = logstashAdminRole.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction4 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate4.test(indexAbstraction4, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate3 = logstashAdminRole.indices()
            .allowedIndicesMatcher(TransportGetAction.TYPE.name());
        IndexAbstraction indexAbstraction3 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate3.test(indexAbstraction3, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate2 = logstashAdminRole.indices()
            .allowedIndicesMatcher(TransportSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction2 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate2.test(indexAbstraction2, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate1 = logstashAdminRole.indices()
            .allowedIndicesMatcher(TransportMultiSearchAction.TYPE.name());
        IndexAbstraction indexAbstraction1 = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate1.test(indexAbstraction1, IndexComponentSelector.DATA), is(true));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = logstashAdminRole.indices()
            .allowedIndicesMatcher(TransportUpdateSettingsAction.TYPE.name());
        IndexAbstraction indexAbstraction = mockIndexAbstraction(index);
        assertThat(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA), is(true));
    }

    public void testIncludeReservedRolesSetting() {
        // default value
        final List<String> defaultIncludes = INCLUDED_RESERVED_ROLES_SETTING.get(Settings.EMPTY);
        assertThat(defaultIncludes, containsInAnyOrder(ReservedRolesStore.names().toArray(String[]::new)));

        // must include superuser
        final String[] includedRolesWithoutSuperuser = randomArray(
            0,
            8,
            String[]::new,
            () -> randomValueOtherThanMany(name -> name.equals("superuser"), () -> randomFrom(ReservedRolesStore.names()))
        );
        final IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> INCLUDED_RESERVED_ROLES_SETTING.get(
                Settings.builder().putList("xpack.security.reserved_roles.include", includedRolesWithoutSuperuser).build()
            )
        );
        assertThat(e1.getMessage(), containsString("the [superuser] reserved role must be included"));

        // all roles names must be known
        final List<String> includedRoles = new ArrayList<>();
        includedRoles.add("superuser");
        IntStream.range(0, randomIntBetween(0, 3)).forEach(ignore -> includedRoles.add(randomFrom(ReservedRolesStore.names())));
        IntStream.range(0, randomIntBetween(1, 3))
            .forEach(
                ignore -> includedRoles.add(
                    randomValueOtherThanMany(name -> ReservedRolesStore.names().contains(name), () -> randomAlphaOfLengthBetween(5, 20))
                )
            );
        final IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> INCLUDED_RESERVED_ROLES_SETTING.get(
                Settings.builder().putList("xpack.security.reserved_roles.include", includedRoles).build()
            )
        );
        assertThat(e2.getMessage(), containsString("unknown reserved roles to include"));
    }

    public void testIncludeReservedRoles() {
        final Set<String> allRoleNames = ReservedRolesStore.names();
        final Set<String> includedRoles = new HashSet<>();
        includedRoles.add("superuser");
        IntStream.range(0, randomIntBetween(0, 3)).forEach(ignore -> includedRoles.add(randomFrom(allRoleNames)));

        final var reservedRolesStore = new ReservedRolesStore(includedRoles);
        for (String roleName : allRoleNames) {
            final PlainActionFuture<RoleRetrievalResult> future = new PlainActionFuture<>();
            if (includedRoles.contains(roleName)) {
                assertThat(ReservedRolesStore.isReserved(roleName), is(true));
                assertThat(ReservedRolesStore.names(), hasItem(roleName));

                reservedRolesStore.accept(Set.of(roleName), future);
                final RoleRetrievalResult roleRetrievalResult = future.actionGet();
                assertThat(roleRetrievalResult.isSuccess(), is(true));
                assertThat(roleRetrievalResult.getDescriptors().stream().map(RoleDescriptor::getName).toList(), contains(roleName));

                assertThat(ReservedRolesStore.roleDescriptor(roleName), notNullValue());
            } else {
                assertThat(ReservedRolesStore.isReserved(roleName), is(false));
                assertThat(ReservedRolesStore.names(), not(hasItem(roleName)));

                reservedRolesStore.accept(Set.of(roleName), future);
                final RoleRetrievalResult roleRetrievalResult = future.actionGet();
                assertThat(roleRetrievalResult.isSuccess(), is(true));
                assertThat(roleRetrievalResult.getDescriptors(), emptyIterable());

                assertThat(ReservedRolesStore.roleDescriptor(roleName), nullValue());
            }
        }

        assertThat(
            ReservedRolesStore.roleDescriptors().stream().map(RoleDescriptor::getName).collect(Collectors.toUnmodifiableSet()),
            equalTo(includedRoles)
        );
    }

    public void testEnrichUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("enrich_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
        assertTrue(role.cluster().check("cluster:admin/xpack/enrich/put", request, authentication));
        assertTrue(role.cluster().check("cluster:admin/xpack/enrich/execute", request, authentication));
        assertTrue(role.cluster().check("cluster:admin/xpack/enrich/esql/resolve", request, authentication));
        assertTrue(role.cluster().check("cluster:admin/xpack/enrich/esql/lookup", request, authentication));
        assertFalse(role.runAs().check(randomAlphaOfLengthBetween(1, 30)));
        IndicesPermission.IsResourceAuthorizedPredicate isResourceAuthorizedPredicate = role.indices()
            .allowedIndicesMatcher(TransportIndexAction.NAME);
        IndexAbstraction indexAbstraction = mockIndexAbstraction("foo");
        assertFalse(isResourceAuthorizedPredicate.test(indexAbstraction, IndexComponentSelector.DATA));
        assertOnlyReadAllowed(role, ".enrich-foo");
    }

    public void testInferenceAdminRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("inference_admin");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
        assertTrue(role.cluster().check("cluster:monitor/xpack/inference/post", request, authentication));
        assertTrue(role.cluster().check("cluster:monitor/xpack/inference/get", request, authentication));
        assertTrue(role.cluster().check("cluster:admin/xpack/inference/put", request, authentication));
        assertTrue(role.cluster().check("cluster:admin/xpack/inference/delete", request, authentication));
        assertTrue(role.cluster().check("cluster:monitor/xpack/ml/trained_models/deployment/infer", request, authentication));
        assertTrue(role.cluster().check("cluster:admin/xpack/ml/trained_models/deployment/start", request, authentication));
        assertTrue(role.cluster().check("cluster:admin/xpack/ml/trained_models/deployment/stop", request, authentication));
        assertFalse(role.runAs().check(randomAlphaOfLengthBetween(1, 30)));
        assertNoAccessAllowed(role, ".inference");
    }

    public void testInferenceUserRole() {
        final TransportRequest request = mock(TransportRequest.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();

        RoleDescriptor roleDescriptor = ReservedRolesStore.roleDescriptor("inference_user");
        assertNotNull(roleDescriptor);
        assertThat(roleDescriptor.getMetadata(), hasEntry("_reserved", true));

        Role role = Role.buildFromRoleDescriptor(roleDescriptor, new FieldPermissionsCache(Settings.EMPTY), RESTRICTED_INDICES);
        assertTrue(role.cluster().check("cluster:monitor/xpack/inference/post", request, authentication));
        assertTrue(role.cluster().check("cluster:monitor/xpack/inference/get", request, authentication));
        assertFalse(role.cluster().check("cluster:admin/xpack/inference/put", request, authentication));
        assertFalse(role.cluster().check("cluster:admin/xpack/inference/delete", request, authentication));
        assertTrue(role.cluster().check("cluster:monitor/xpack/ml/trained_models/deployment/infer", request, authentication));
        assertFalse(role.cluster().check("cluster:admin/xpack/ml/trained_models/deployment/start", request, authentication));
        assertFalse(role.cluster().check("cluster:admin/xpack/ml/trained_models/deployment/stop", request, authentication));
        assertFalse(role.runAs().check(randomAlphaOfLengthBetween(1, 30)));
        assertNoAccessAllowed(role, ".inference");
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
