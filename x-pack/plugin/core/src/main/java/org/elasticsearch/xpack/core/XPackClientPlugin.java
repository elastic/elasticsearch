/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.license.DeleteLicenseAction;
import org.elasticsearch.license.GetBasicStatusAction;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.license.GetTrialStatusAction;
import org.elasticsearch.license.LicensesMetadata;
import org.elasticsearch.license.PostStartBasicAction;
import org.elasticsearch.license.PostStartTrialAction;
import org.elasticsearch.license.PutLicenseAction;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersAction;
import org.elasticsearch.xpack.core.action.XPackInfoAction;
import org.elasticsearch.xpack.core.action.XPackUsageAction;
import org.elasticsearch.xpack.core.aggregatemetric.AggregateMetricFeatureSetUsage;
import org.elasticsearch.xpack.core.analytics.AnalyticsFeatureSetUsage;
import org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage;
import org.elasticsearch.xpack.core.archive.ArchiveFeatureSetUsage;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultAction;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.datastreams.DataLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.datastreams.DataStreamFeatureSetUsage;
import org.elasticsearch.xpack.core.downsample.DownsampleIndexerAction;
import org.elasticsearch.xpack.core.enrich.EnrichFeatureSetUsage;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.elasticsearch.xpack.core.eql.EqlFeatureSetUsage;
import org.elasticsearch.xpack.core.frozen.FrozenIndicesFeatureSetUsage;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;
import org.elasticsearch.xpack.core.graph.GraphFeatureSetUsage;
import org.elasticsearch.xpack.core.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.ilm.UnfollowAction;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;
import org.elasticsearch.xpack.core.ilm.action.DeleteLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.MoveToStepAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.RemoveIndexLifecyclePolicyAction;
import org.elasticsearch.xpack.core.ilm.action.RetryAction;
import org.elasticsearch.xpack.core.logstash.LogstashFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.MachineLearningFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarEventAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAction;
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
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.core.ml.action.ValidateDetectorAction;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskState;
import org.elasticsearch.xpack.core.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupCapsAction;
import org.elasticsearch.xpack.core.rollup.action.GetRollupJobsAction;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.RollupSearchAction;
import org.elasticsearch.xpack.core.rollup.action.RollupShardStatus;
import org.elasticsearch.xpack.core.rollup.action.StartRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotFeatureSetUsage;
import org.elasticsearch.xpack.core.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.InvalidateApiKeyAction;
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
import org.elasticsearch.xpack.core.security.authc.TokenMetadata;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AllExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AnyExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExceptExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.slm.SLMFeatureSetUsage;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.action.DeleteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleStatsAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.spatial.SpatialFeatureSetUsage;
import org.elasticsearch.xpack.core.sql.SqlFeatureSetUsage;
import org.elasticsearch.xpack.core.ssl.action.GetCertificateInfoAction;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumAction;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureAction;
import org.elasticsearch.xpack.core.transform.TransformFeatureSetUsage;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.NullRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.RetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.SyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.votingonly.VotingOnlyNodeFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.WatcherFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.WatcherMetadata;
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
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

// TODO: merge this into XPackPlugin
public class XPackClientPlugin extends Plugin implements ActionPlugin, NetworkPlugin {

    @Override
    public List<Setting<?>> getSettings() {
        ArrayList<Setting<?>> settings = new ArrayList<>();
        // the only licensing one
        settings.add(Setting.groupSetting("license.", Setting.Property.NodeScope));

        // TODO split these settings up
        settings.addAll(XPackSettings.getAllSettings());

        // we add the `xpack.version` setting to all internal indices
        settings.add(Setting.simpleString("index.xpack.version", Setting.Property.IndexScope));

        return settings;
    }

    @Override
    public List<ActionType<? extends ActionResponse>> getClientActions() {
        return Arrays.asList(
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
            SetUpgradeModeAction.INSTANCE,
            PutDataFrameAnalyticsAction.INSTANCE,
            GetDataFrameAnalyticsAction.INSTANCE,
            GetDataFrameAnalyticsStatsAction.INSTANCE,
            UpdateDataFrameAnalyticsAction.INSTANCE,
            DeleteDataFrameAnalyticsAction.INSTANCE,
            StartDataFrameAnalyticsAction.INSTANCE,
            EvaluateDataFrameAction.INSTANCE,
            ExplainDataFrameAnalyticsAction.INSTANCE,
            InferModelAction.INSTANCE,
            InferModelAction.EXTERNAL_INSTANCE,
            GetTrainedModelsAction.INSTANCE,
            DeleteTrainedModelAction.INSTANCE,
            GetTrainedModelsStatsAction.INSTANCE,
            PutTrainedModelAction.INSTANCE,
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
            CreateApiKeyAction.INSTANCE,
            InvalidateApiKeyAction.INSTANCE,
            GetApiKeyAction.INSTANCE,
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
            PutSnapshotLifecycleAction.INSTANCE,
            GetSnapshotLifecycleAction.INSTANCE,
            DeleteSnapshotLifecycleAction.INSTANCE,
            ExecuteSnapshotLifecycleAction.INSTANCE,
            GetSnapshotLifecycleStatsAction.INSTANCE,
            MigrateToDataTiersAction.INSTANCE,

            // Freeze
            FreezeIndexAction.INSTANCE,
            // Data Frame
            PutTransformAction.INSTANCE,
            StartTransformAction.INSTANCE,
            StopTransformAction.INSTANCE,
            DeleteTransformAction.INSTANCE,
            GetTransformAction.INSTANCE,
            GetTransformStatsAction.INSTANCE,
            PreviewTransformAction.INSTANCE,
            // Async Search
            SubmitAsyncSearchAction.INSTANCE,
            GetAsyncSearchAction.INSTANCE,
            DeleteAsyncResultAction.INSTANCE,
            // Text Structure
            FindStructureAction.INSTANCE,
            // Terms enum API
            TermsEnumAction.INSTANCE,
            // TSDB Downsampling
            DownsampleIndexerAction.INSTANCE,
            org.elasticsearch.xpack.core.downsample.DownsampleAction.INSTANCE
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Stream.of(
            // graph
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.GRAPH, GraphFeatureSetUsage::new),
            // logstash
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.LOGSTASH, LogstashFeatureSetUsage::new),
            // ML
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.MACHINE_LEARNING, MachineLearningFeatureSetUsage::new),
            // monitoring
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.MONITORING, MonitoringFeatureSetUsage::new),
            // security
            new NamedWriteableRegistry.Entry(ClusterState.Custom.class, TokenMetadata.TYPE, TokenMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, TokenMetadata.TYPE, TokenMetadata::readDiffFrom),
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.SECURITY, SecurityFeatureSetUsage::new),
            // security : conditional privileges
            new NamedWriteableRegistry.Entry(
                ConfigurableClusterPrivilege.class,
                ConfigurableClusterPrivileges.ManageApplicationPrivileges.WRITEABLE_NAME,
                ConfigurableClusterPrivileges.ManageApplicationPrivileges::createFrom
            ),
            new NamedWriteableRegistry.Entry(
                ConfigurableClusterPrivilege.class,
                ConfigurableClusterPrivileges.WriteProfileDataPrivileges.WRITEABLE_NAME,
                ConfigurableClusterPrivileges.WriteProfileDataPrivileges::createFrom
            ),
            // security : role-mappings
            new NamedWriteableRegistry.Entry(RoleMapperExpression.class, AllExpression.NAME, AllExpression::new),
            new NamedWriteableRegistry.Entry(RoleMapperExpression.class, AnyExpression.NAME, AnyExpression::new),
            new NamedWriteableRegistry.Entry(RoleMapperExpression.class, FieldExpression.NAME, FieldExpression::new),
            new NamedWriteableRegistry.Entry(RoleMapperExpression.class, ExceptExpression.NAME, ExceptExpression::new),
            // eql
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.EQL, EqlFeatureSetUsage::new),
            // sql
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.SQL, SqlFeatureSetUsage::new),
            // watcher
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, WatcherMetadata.TYPE, WatcherMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, WatcherMetadata.TYPE, WatcherMetadata::readDiffFrom),
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.WATCHER, WatcherFeatureSetUsage::new),
            // licensing
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, LicensesMetadata.TYPE, LicensesMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, LicensesMetadata.TYPE, LicensesMetadata::readDiffFrom),
            // rollup
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.ROLLUP, RollupFeatureSetUsage::new),
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, RollupJob.NAME, RollupJob::new),
            new NamedWriteableRegistry.Entry(Task.Status.class, RollupJobStatus.NAME, RollupJobStatus::new),
            new NamedWriteableRegistry.Entry(PersistentTaskState.class, RollupJobStatus.NAME, RollupJobStatus::new),
            new NamedWriteableRegistry.Entry(Task.Status.class, RollupShardStatus.NAME, RollupShardStatus::new),
            // ccr
            new NamedWriteableRegistry.Entry(AutoFollowMetadata.class, AutoFollowMetadata.TYPE, AutoFollowMetadata::new),
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, AutoFollowMetadata.TYPE, AutoFollowMetadata::new),
            new NamedWriteableRegistry.Entry(
                NamedDiff.class,
                AutoFollowMetadata.TYPE,
                in -> AutoFollowMetadata.readDiffFrom(Metadata.Custom.class, AutoFollowMetadata.TYPE, in)
            ),
            // ILM
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.INDEX_LIFECYCLE, IndexLifecycleFeatureSetUsage::new),
            // SLM
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.SNAPSHOT_LIFECYCLE, SLMFeatureSetUsage::new),
            // ILM - Custom Metadata
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata::new),
            new NamedWriteableRegistry.Entry(
                NamedDiff.class,
                IndexLifecycleMetadata.TYPE,
                IndexLifecycleMetadata.IndexLifecycleMetadataDiff::new
            ),
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, LifecycleOperationMetadata.TYPE, LifecycleOperationMetadata::new),
            new NamedWriteableRegistry.Entry(
                NamedDiff.class,
                LifecycleOperationMetadata.TYPE,
                LifecycleOperationMetadata.LifecycleOperationMetadataDiff::new
            ),
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata::new),
            new NamedWriteableRegistry.Entry(
                NamedDiff.class,
                SnapshotLifecycleMetadata.TYPE,
                SnapshotLifecycleMetadata.SnapshotLifecycleMetadataDiff::new
            ),
            // ILM - LifecycleTypes
            new NamedWriteableRegistry.Entry(LifecycleType.class, TimeseriesLifecycleType.TYPE, (in) -> TimeseriesLifecycleType.INSTANCE),
            // ILM - Lifecycle Actions
            new NamedWriteableRegistry.Entry(LifecycleAction.class, AllocateAction.NAME, AllocateAction::new),
            new NamedWriteableRegistry.Entry(LifecycleAction.class, ForceMergeAction.NAME, ForceMergeAction::new),
            new NamedWriteableRegistry.Entry(LifecycleAction.class, ReadOnlyAction.NAME, ReadOnlyAction::new),
            new NamedWriteableRegistry.Entry(LifecycleAction.class, RolloverAction.NAME, RolloverAction::read),
            new NamedWriteableRegistry.Entry(LifecycleAction.class, ShrinkAction.NAME, ShrinkAction::new),
            new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::readFrom),
            new NamedWriteableRegistry.Entry(LifecycleAction.class, FreezeAction.NAME, in -> FreezeAction.INSTANCE),
            new NamedWriteableRegistry.Entry(LifecycleAction.class, SetPriorityAction.NAME, SetPriorityAction::new),
            new NamedWriteableRegistry.Entry(LifecycleAction.class, UnfollowAction.NAME, in -> UnfollowAction.INSTANCE),
            new NamedWriteableRegistry.Entry(LifecycleAction.class, WaitForSnapshotAction.NAME, WaitForSnapshotAction::new),
            new NamedWriteableRegistry.Entry(LifecycleAction.class, SearchableSnapshotAction.NAME, SearchableSnapshotAction::new),
            new NamedWriteableRegistry.Entry(LifecycleAction.class, MigrateAction.NAME, MigrateAction::readFrom),
            // Transforms
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, TransformMetadata.TYPE, TransformMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, TransformMetadata.TYPE, TransformMetadata.TransformMetadataDiff::new),
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.TRANSFORM, TransformFeatureSetUsage::new),
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, TransformField.TASK_NAME, TransformTaskParams::new),
            new NamedWriteableRegistry.Entry(Task.Status.class, TransformField.TASK_NAME, TransformState::new),
            new NamedWriteableRegistry.Entry(PersistentTaskState.class, TransformField.TASK_NAME, TransformState::new),
            new NamedWriteableRegistry.Entry(SyncConfig.class, TransformField.TIME.getPreferredName(), TimeSyncConfig::new),
            new NamedWriteableRegistry.Entry(
                RetentionPolicyConfig.class,
                TransformField.TIME.getPreferredName(),
                TimeRetentionPolicyConfig::new
            ),
            new NamedWriteableRegistry.Entry(
                RetentionPolicyConfig.class,
                NullRetentionPolicyConfig.NAME.getPreferredName(),
                i -> NullRetentionPolicyConfig.INSTANCE
            ),
            // Voting Only Node
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.VOTING_ONLY, VotingOnlyNodeFeatureSetUsage::new),
            // Frozen indices
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.FROZEN_INDICES, FrozenIndicesFeatureSetUsage::new),
            // Spatial
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.SPATIAL, SpatialFeatureSetUsage::new),
            // Analytics
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.ANALYTICS, AnalyticsFeatureSetUsage::new),
            // Aggregate metric field type
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.AGGREGATE_METRIC, AggregateMetricFeatureSetUsage::new),
            // Enrich
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.ENRICH, EnrichFeatureSetUsage::new),
            new NamedWriteableRegistry.Entry(Task.Status.class, ExecuteEnrichPolicyStatus.NAME, ExecuteEnrichPolicyStatus::new),
            // Searchable snapshots
            new NamedWriteableRegistry.Entry(
                XPackFeatureSet.Usage.class,
                XPackField.SEARCHABLE_SNAPSHOTS,
                SearchableSnapshotFeatureSetUsage::new
            ),
            // Data Streams
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.DATA_STREAMS, DataStreamFeatureSetUsage::new),
            DataLifecycle.isEnabled()
                ? new NamedWriteableRegistry.Entry(
                    XPackFeatureSet.Usage.class,
                    XPackField.DATA_LIFECYCLE,
                    DataLifecycleFeatureSetUsage::new
                )
                : null,
            // Data Tiers
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.DATA_TIERS, DataTiersFeatureSetUsage::new),
            // Archive
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.ARCHIVE, ArchiveFeatureSetUsage::new),
            // TSDB Downsampling
            new NamedWriteableRegistry.Entry(LifecycleAction.class, DownsampleAction.NAME, DownsampleAction::new),
            // Health API usage
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.HEALTH_API, HealthApiFeatureSetUsage::new),
            // Remote cluster usage
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.REMOTE_CLUSTERS, RemoteClusterFeatureSetUsage::new),
            // Enterprise Search
            new NamedWriteableRegistry.Entry(
                XPackFeatureSet.Usage.class,
                XPackField.ENTERPRISE_SEARCH,
                EnterpriseSearchFeatureSetUsage::new
            )
        ).filter(Objects::nonNull).toList();
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
            // ML - Custom metadata
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField("ml"),
                parser -> MlMetadata.LENIENT_PARSER.parse(parser, null).build()
            ),
            // ML - Persistent action requests
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(MlTasks.DATAFEED_TASK_NAME),
                StartDatafeedAction.DatafeedParams::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(MlTasks.JOB_TASK_NAME),
                OpenJobAction.JobParams::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME),
                StartDataFrameAnalyticsAction.TaskParams::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME),
                SnapshotUpgradeTaskParams::fromXContent
            ),
            // ML - Task states
            new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(DatafeedState.NAME), DatafeedState::fromXContent),
            new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(JobTaskState.NAME), JobTaskState::fromXContent),
            new NamedXContentRegistry.Entry(
                PersistentTaskState.class,
                new ParseField(DataFrameAnalyticsTaskState.NAME),
                DataFrameAnalyticsTaskState::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                PersistentTaskState.class,
                new ParseField(SnapshotUpgradeTaskState.NAME),
                SnapshotUpgradeTaskState::fromXContent
            ),
            // watcher
            new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(WatcherMetadata.TYPE), WatcherMetadata::fromXContent),
            // licensing
            new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(LicensesMetadata.TYPE), LicensesMetadata::fromXContent),
            // rollup
            new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(RollupField.TASK_NAME), RollupJob::fromXContent),
            new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(RollupJobStatus.NAME), RollupJobStatus::fromXContent),
            new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(RollupJobStatus.NAME), RollupJobStatus::fromXContent),
            // Transforms
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(TransformField.TASK_NAME),
                TransformTaskParams::fromXContent
            ),
            new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(TransformField.TASK_NAME), TransformState::fromXContent),
            new NamedXContentRegistry.Entry(
                PersistentTaskState.class,
                new ParseField(TransformField.TASK_NAME),
                TransformState::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(TransformMetadata.TYPE),
                parser -> TransformMetadata.LENIENT_PARSER.parse(parser, null).build()
            )
        );
    }
}
