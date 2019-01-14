/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.license.DeleteLicenseAction;
import org.elasticsearch.license.GetBasicStatusAction;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.license.GetTrialStatusAction;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicensesMetaData;
import org.elasticsearch.license.PostStartBasicAction;
import org.elasticsearch.license.PostStartTrialAction;
import org.elasticsearch.license.PutLicenseAction;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.core.action.TransportFreezeIndexAction;
import org.elasticsearch.xpack.core.action.XPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackUsageAction;
import org.elasticsearch.xpack.core.beats.BeatsFeatureSetUsage;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.graph.GraphFeatureSetUsage;
import org.elasticsearch.xpack.core.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.core.indexlifecycle.AllocateAction;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.ForceMergeAction;
import org.elasticsearch.xpack.core.indexlifecycle.FreezeAction;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.ReadOnlyAction;
import org.elasticsearch.xpack.core.indexlifecycle.RolloverAction;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkAction;
import org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.MoveToStepAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.RemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.RetryAction;
import org.elasticsearch.xpack.core.logstash.LogstashFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.MachineLearningFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarEventAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.ml.action.FindFileStructureAction;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.MlUpgradeAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.core.ml.action.ValidateDetectorAction;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupCapsAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.core.rollup.action.StartRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.elasticsearch.xpack.core.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.SecuritySettings;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.core.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.core.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.RefreshTokenAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.core.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.SetEnabledAction;
import org.elasticsearch.xpack.core.security.authc.TokenMetaData;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AllExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AnyExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExceptExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivileges;
import org.elasticsearch.xpack.core.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.core.sql.SqlFeatureSetUsage;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.action.GetCertificateInfoAction;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.core.watcher.WatcherFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.WatcherMetaData;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.delete.DeleteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class XPackClientPlugin extends Plugin implements ActionPlugin, NetworkPlugin {

    static Optional<String> X_PACK_FEATURE = Optional.of("x-pack");

    @Override
    protected Optional<String> getFeature() {
        return X_PACK_FEATURE;
    }

    private final Settings settings;

    public XPackClientPlugin(final Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        // the only licensing one
        settings.add(Setting.groupSetting("license.", Setting.Property.NodeScope));

        //TODO split these settings up
        settings.addAll(XPackSettings.getAllSettings());

        settings.add(LicenseService.SELF_GENERATED_LICENSE_TYPE);

        // we add the `xpack.version` setting to all internal indices
        settings.add(Setting.simpleString("index.xpack.version", Setting.Property.IndexScope));

        return settings;
    }

    @Override
    public Settings additionalSettings() {
        return additionalSettings(settings, XPackSettings.SECURITY_ENABLED.get(settings), XPackPlugin.transportClientMode(settings));
    }

    static Settings additionalSettings(final Settings settings, final boolean enabled, final boolean transportClientMode) {
        if (enabled && transportClientMode) {
            return Settings.builder()
                    .put(SecuritySettings.addTransportSettings(settings))
                    .put(SecuritySettings.addUserSettings(settings))
                    .build();
        } else {
            return Settings.EMPTY;
        }
    }

    @Override
    public List<Action<? extends ActionResponse>> getClientActions() {
        return Arrays.asList(
                // deprecation
                DeprecationInfoAction.INSTANCE,
                // graph
                GraphExploreAction.INSTANCE,
                // ML
                GetJobsAction.INSTANCE,
                GetJobsStatsAction.INSTANCE,
                MlInfoAction.INSTANCE,
                PutJobAction.INSTANCE,
                UpdateJobAction.INSTANCE,
                DeleteJobAction.INSTANCE,
                OpenJobAction.INSTANCE,
                GetFiltersAction.INSTANCE,
                PutFilterAction.INSTANCE,
                UpdateFilterAction.INSTANCE,
                DeleteFilterAction.INSTANCE,
                KillProcessAction.INSTANCE,
                GetBucketsAction.INSTANCE,
                GetInfluencersAction.INSTANCE,
                GetOverallBucketsAction.INSTANCE,
                GetRecordsAction.INSTANCE,
                PostDataAction.INSTANCE,
                CloseJobAction.INSTANCE,
                FinalizeJobExecutionAction.INSTANCE,
                FlushJobAction.INSTANCE,
                ValidateDetectorAction.INSTANCE,
                ValidateJobConfigAction.INSTANCE,
                GetCategoriesAction.INSTANCE,
                GetModelSnapshotsAction.INSTANCE,
                RevertModelSnapshotAction.INSTANCE,
                UpdateModelSnapshotAction.INSTANCE,
                GetDatafeedsAction.INSTANCE,
                GetDatafeedsStatsAction.INSTANCE,
                PutDatafeedAction.INSTANCE,
                UpdateDatafeedAction.INSTANCE,
                DeleteDatafeedAction.INSTANCE,
                PreviewDatafeedAction.INSTANCE,
                StartDatafeedAction.INSTANCE,
                StopDatafeedAction.INSTANCE,
                IsolateDatafeedAction.INSTANCE,
                DeleteModelSnapshotAction.INSTANCE,
                UpdateProcessAction.INSTANCE,
                DeleteExpiredDataAction.INSTANCE,
                ForecastJobAction.INSTANCE,
                DeleteForecastAction.INSTANCE,
                GetCalendarsAction.INSTANCE,
                PutCalendarAction.INSTANCE,
                DeleteCalendarAction.INSTANCE,
                DeleteCalendarEventAction.INSTANCE,
                UpdateCalendarJobAction.INSTANCE,
                GetCalendarEventsAction.INSTANCE,
                PostCalendarEventsAction.INSTANCE,
                PersistJobAction.INSTANCE,
                FindFileStructureAction.INSTANCE,
                MlUpgradeAction.INSTANCE,
                // security
                ClearRealmCacheAction.INSTANCE,
                ClearRolesCacheAction.INSTANCE,
                GetUsersAction.INSTANCE,
                PutUserAction.INSTANCE,
                DeleteUserAction.INSTANCE,
                GetRolesAction.INSTANCE,
                PutRoleAction.INSTANCE,
                DeleteRoleAction.INSTANCE,
                ChangePasswordAction.INSTANCE,
                AuthenticateAction.INSTANCE,
                SetEnabledAction.INSTANCE,
                HasPrivilegesAction.INSTANCE,
                GetRoleMappingsAction.INSTANCE,
                PutRoleMappingAction.INSTANCE,
                DeleteRoleMappingAction.INSTANCE,
                CreateTokenAction.INSTANCE,
                InvalidateTokenAction.INSTANCE,
                GetCertificateInfoAction.INSTANCE,
                RefreshTokenAction.INSTANCE,
                // upgrade
                IndexUpgradeInfoAction.INSTANCE,
                IndexUpgradeAction.INSTANCE,
                // watcher
                PutWatchAction.INSTANCE,
                DeleteWatchAction.INSTANCE,
                GetWatchAction.INSTANCE,
                WatcherStatsAction.INSTANCE,
                AckWatchAction.INSTANCE,
                ActivateWatchAction.INSTANCE,
                WatcherServiceAction.INSTANCE,
                ExecuteWatchAction.INSTANCE,
                // license
                PutLicenseAction.INSTANCE,
                GetLicenseAction.INSTANCE,
                DeleteLicenseAction.INSTANCE,
                PostStartTrialAction.INSTANCE,
                GetTrialStatusAction.INSTANCE,
                PostStartBasicAction.INSTANCE,
                GetBasicStatusAction.INSTANCE,
                // x-pack
                XPackInfoAction.INSTANCE,
                XPackUsageAction.INSTANCE,
                // rollup
                RollupSearchAction.INSTANCE,
                PutRollupJobAction.INSTANCE,
                StartRollupJobAction.INSTANCE,
                StopRollupJobAction.INSTANCE,
                DeleteRollupJobAction.INSTANCE,
                GetRollupJobsAction.INSTANCE,
                GetRollupCapsAction.INSTANCE,
                // ILM
                DeleteLifecycleAction.INSTANCE,
                GetLifecycleAction.INSTANCE,
                PutLifecycleAction.INSTANCE,
                ExplainLifecycleAction.INSTANCE,
                RemoveIndexLifecyclePolicyAction.INSTANCE,
                MoveToStepAction.INSTANCE,
                RetryAction.INSTANCE,
                TransportFreezeIndexAction.FreezeIndexAction.INSTANCE
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
                // graph
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.GRAPH, GraphFeatureSetUsage::new),
                // logstash
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.LOGSTASH, LogstashFeatureSetUsage::new),
                // beats
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.BEATS, BeatsFeatureSetUsage::new),
                // ML - Custom metadata
                new NamedWriteableRegistry.Entry(MetaData.Custom.class, "ml", MlMetadata::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, "ml", MlMetadata.MlMetadataDiff::new),
                // ML - Persistent action requests
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, MlTasks.DATAFEED_TASK_NAME,
                        StartDatafeedAction.DatafeedParams::new),
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, MlTasks.JOB_TASK_NAME,
                        OpenJobAction.JobParams::new),
                // ML - Task states
                new NamedWriteableRegistry.Entry(PersistentTaskState.class, JobTaskState.NAME, JobTaskState::new),
                new NamedWriteableRegistry.Entry(PersistentTaskState.class, DatafeedState.NAME, DatafeedState::fromStream),
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.MACHINE_LEARNING,
                        MachineLearningFeatureSetUsage::new),
                // monitoring
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.MONITORING, MonitoringFeatureSetUsage::new),
                // security
                new NamedWriteableRegistry.Entry(ClusterState.Custom.class, TokenMetaData.TYPE, TokenMetaData::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, TokenMetaData.TYPE, TokenMetaData::readDiffFrom),
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.SECURITY, SecurityFeatureSetUsage::new),
                // security : conditional privileges
                new NamedWriteableRegistry.Entry(ConditionalClusterPrivilege.class,
                    ConditionalClusterPrivileges.ManageApplicationPrivileges.WRITEABLE_NAME,
                    ConditionalClusterPrivileges.ManageApplicationPrivileges::createFrom),
                // security : role-mappings
                new NamedWriteableRegistry.Entry(RoleMapperExpression.class, AllExpression.NAME, AllExpression::new),
                new NamedWriteableRegistry.Entry(RoleMapperExpression.class, AnyExpression.NAME, AnyExpression::new),
                new NamedWriteableRegistry.Entry(RoleMapperExpression.class, FieldExpression.NAME, FieldExpression::new),
                new NamedWriteableRegistry.Entry(RoleMapperExpression.class, ExceptExpression.NAME, ExceptExpression::new),
                // sql
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.SQL, SqlFeatureSetUsage::new),
                // watcher
                new NamedWriteableRegistry.Entry(MetaData.Custom.class, WatcherMetaData.TYPE, WatcherMetaData::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, WatcherMetaData.TYPE, WatcherMetaData::readDiffFrom),
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.WATCHER, WatcherFeatureSetUsage::new),
                // licensing
                new NamedWriteableRegistry.Entry(MetaData.Custom.class, LicensesMetaData.TYPE, LicensesMetaData::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, LicensesMetaData.TYPE, LicensesMetaData::readDiffFrom),
                // rollup
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.ROLLUP, RollupFeatureSetUsage::new),
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, RollupJob.NAME, RollupJob::new),
                new NamedWriteableRegistry.Entry(Task.Status.class, RollupJobStatus.NAME, RollupJobStatus::new),
                new NamedWriteableRegistry.Entry(PersistentTaskState.class, RollupJobStatus.NAME, RollupJobStatus::new),
                // ccr
                new NamedWriteableRegistry.Entry(AutoFollowMetadata.class, AutoFollowMetadata.TYPE, AutoFollowMetadata::new),
                new NamedWriteableRegistry.Entry(MetaData.Custom.class, AutoFollowMetadata.TYPE, AutoFollowMetadata::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, AutoFollowMetadata.TYPE,
                    in -> AutoFollowMetadata.readDiffFrom(MetaData.Custom.class, AutoFollowMetadata.TYPE, in)),
                // ILM
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.INDEX_LIFECYCLE,
                    IndexLifecycleFeatureSetUsage::new),
                // ILM - Custom Metadata
                new NamedWriteableRegistry.Entry(MetaData.Custom.class, IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, IndexLifecycleMetadata.TYPE,
                    IndexLifecycleMetadata.IndexLifecycleMetadataDiff::new),
                // ILM - LifecycleTypes
                new NamedWriteableRegistry.Entry(LifecycleType.class, TimeseriesLifecycleType.TYPE,
                    (in) -> TimeseriesLifecycleType.INSTANCE),
                // ILM - Lifecycle Actions
                new NamedWriteableRegistry.Entry(LifecycleAction.class, AllocateAction.NAME, AllocateAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ForceMergeAction.NAME, ForceMergeAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ReadOnlyAction.NAME, ReadOnlyAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, RolloverAction.NAME, RolloverAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ShrinkAction.NAME, ShrinkAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, FreezeAction.NAME, FreezeAction::new)
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
                // ML - Custom metadata
                new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField("ml"),
                        parser -> MlMetadata.LENIENT_PARSER.parse(parser, null).build()),
                // ML - Persistent action requests
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(MlTasks.DATAFEED_TASK_NAME),
                        StartDatafeedAction.DatafeedParams::fromXContent),
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(MlTasks.JOB_TASK_NAME),
                        OpenJobAction.JobParams::fromXContent),
                // ML - Task states
                new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(DatafeedState.NAME), DatafeedState::fromXContent),
                new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(JobTaskState.NAME), JobTaskState::fromXContent),
                // watcher
                new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(WatcherMetaData.TYPE),
                        WatcherMetaData::fromXContent),
                // licensing
                new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(LicensesMetaData.TYPE),
                        LicensesMetaData::fromXContent),
                //rollup
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(RollupField.TASK_NAME),
                        RollupJob::fromXContent),
                new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(RollupJobStatus.NAME),
                        RollupJobStatus::fromXContent),
                new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(RollupJobStatus.NAME),
                        RollupJobStatus::fromXContent)
            );
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(
            final Settings settings,
            final ThreadPool threadPool,
            final PageCacheRecycler pageCacheRecycler,
            final CircuitBreakerService circuitBreakerService,
            final NamedWriteableRegistry namedWriteableRegistry,
            final NetworkService networkService) {
        // this should only be used in the transport layer, so do not add it if it is not in transport mode or we are disabled
        if (XPackPlugin.transportClientMode(settings) == false || XPackSettings.SECURITY_ENABLED.get(settings) == false) {
            return Collections.emptyMap();
        }
        final SSLService sslService;
        try {
            sslService = new SSLService(settings, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return Collections.singletonMap(SecurityField.NAME4, () -> new SecurityNetty4Transport(settings, Version.CURRENT, threadPool,
                networkService, pageCacheRecycler, namedWriteableRegistry, circuitBreakerService, sslService));
    }

}
