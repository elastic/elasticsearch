/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.action.GenericAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.license.DeleteLicenseAction;
import org.elasticsearch.license.GetLicenseAction;
import org.elasticsearch.license.GetTrialStatusAction;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicensesMetaData;
import org.elasticsearch.license.PostStartTrialAction;
import org.elasticsearch.license.PutLicenseAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.action.XPackInfoAction;
import org.elasticsearch.xpack.action.XPackUsageAction;
import org.elasticsearch.xpack.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.graph.GraphFeatureSetUsage;
import org.elasticsearch.xpack.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.ml.MachineLearningFeatureSetUsage;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.CloseJobAction;
import org.elasticsearch.xpack.ml.action.DeleteCalendarAction;
import org.elasticsearch.xpack.ml.action.DeleteCalendarEventAction;
import org.elasticsearch.xpack.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.ml.action.FlushJobAction;
import org.elasticsearch.xpack.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.ml.action.GetCalendarEventsAction;
import org.elasticsearch.xpack.ml.action.GetCalendarsAction;
import org.elasticsearch.xpack.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.ml.action.GetJobsAction;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.action.GetOverallBucketsAction;
import org.elasticsearch.xpack.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.ml.action.KillProcessAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.ml.action.PostDataAction;
import org.elasticsearch.xpack.ml.action.PreviewDatafeedAction;
import org.elasticsearch.xpack.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.ml.action.PutFilterAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.ml.action.ValidateDetectorAction;
import org.elasticsearch.xpack.ml.action.ValidateJobConfigAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.JobTaskStatus;
import org.elasticsearch.xpack.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.xpack.persistent.CompletionPersistentTaskAction;
import org.elasticsearch.xpack.persistent.PersistentTaskParams;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksNodeService;
import org.elasticsearch.xpack.persistent.RemovePersistentTaskAction;
import org.elasticsearch.xpack.persistent.StartPersistentTaskAction;
import org.elasticsearch.xpack.persistent.UpdatePersistentTaskStatusAction;
import org.elasticsearch.xpack.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.security.SecurityField;
import org.elasticsearch.xpack.security.SecuritySettings;
import org.elasticsearch.xpack.security.action.realm.ClearRealmCacheAction;
import org.elasticsearch.xpack.security.action.role.ClearRolesCacheAction;
import org.elasticsearch.xpack.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.security.action.role.GetRolesAction;
import org.elasticsearch.xpack.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.security.action.rolemapping.PutRoleMappingAction;
import org.elasticsearch.xpack.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.security.action.token.InvalidateTokenAction;
import org.elasticsearch.xpack.security.action.token.RefreshTokenAction;
import org.elasticsearch.xpack.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.security.action.user.DeleteUserAction;
import org.elasticsearch.xpack.security.action.user.GetUsersAction;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.security.action.user.PutUserAction;
import org.elasticsearch.xpack.security.action.user.SetEnabledAction;
import org.elasticsearch.xpack.security.authc.TokenMetaData;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl.AllExpression;
import org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl.AnyExpression;
import org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl.ExceptExpression;
import org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.ssl.action.GetCertificateInfoAction;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.watcher.WatcherFeatureSetUsage;
import org.elasticsearch.xpack.watcher.WatcherMetaData;
import org.elasticsearch.xpack.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.activate.ActivateWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.delete.DeleteWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchAction;
import org.elasticsearch.xpack.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.watcher.transport.actions.stats.WatcherStatsAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class XPackClientPlugin extends Plugin implements ActionPlugin, NetworkPlugin {

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
            final Settings.Builder builder = Settings.builder();
            builder.put(SecuritySettings.addTransportSettings(settings));
            builder.put(SecuritySettings.addUserSettings(settings));
            return builder.build();
        } else {
            return Settings.EMPTY;
        }
    }

    @Override
    public List<GenericAction> getClientActions() {
        return Arrays.asList(
                // deprecation
                DeprecationInfoAction.INSTANCE,
                // graph
                GraphExploreAction.INSTANCE,
                // ML
                GetJobsAction.INSTANCE,
                GetJobsStatsAction.INSTANCE,
                PutJobAction.INSTANCE,
                UpdateJobAction.INSTANCE,
                DeleteJobAction.INSTANCE,
                OpenJobAction.INSTANCE,
                GetFiltersAction.INSTANCE,
                PutFilterAction.INSTANCE,
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
                GetCalendarsAction.INSTANCE,
                PutCalendarAction.INSTANCE,
                DeleteCalendarAction.INSTANCE,
                DeleteCalendarEventAction.INSTANCE,
                UpdateCalendarJobAction.INSTANCE,
                GetCalendarEventsAction.INSTANCE,
                PostCalendarEventsAction.INSTANCE,
                // licensing
                StartPersistentTaskAction.INSTANCE,
                UpdatePersistentTaskStatusAction.INSTANCE,
                RemovePersistentTaskAction.INSTANCE,
                CompletionPersistentTaskAction.INSTANCE,
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
                // x-pack
                XPackInfoAction.INSTANCE,
                XPackUsageAction.INSTANCE
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
                // graph
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.GRAPH, GraphFeatureSetUsage::new),
                // ML - Custom metadata
                new NamedWriteableRegistry.Entry(MetaData.Custom.class, "ml", MlMetadata::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, "ml", MlMetadata.MlMetadataDiff::new),
                new NamedWriteableRegistry.Entry(MetaData.Custom.class, PersistentTasksCustomMetaData.TYPE,
                        PersistentTasksCustomMetaData::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, PersistentTasksCustomMetaData.TYPE,
                        PersistentTasksCustomMetaData::readDiffFrom),
                // ML - Persistent action requests
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, StartDatafeedAction.TASK_NAME,
                        StartDatafeedAction.DatafeedParams::new),
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, OpenJobAction.TASK_NAME,
                        OpenJobAction.JobParams::new),
                // ML - Task statuses
                new NamedWriteableRegistry.Entry(Task.Status.class, PersistentTasksNodeService.Status.NAME,
                        PersistentTasksNodeService.Status::new),
                new NamedWriteableRegistry.Entry(Task.Status.class, JobTaskStatus.NAME, JobTaskStatus::new),
                new NamedWriteableRegistry.Entry(Task.Status.class, DatafeedState.NAME, DatafeedState::fromStream),
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.MACHINE_LEARNING,
                        MachineLearningFeatureSetUsage::new),
                // monitoring
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.MONITORING, MonitoringFeatureSetUsage::new),
                // security
                new NamedWriteableRegistry.Entry(ClusterState.Custom.class, TokenMetaData.TYPE, TokenMetaData::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, TokenMetaData.TYPE, TokenMetaData::readDiffFrom),
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.SECURITY, SecurityFeatureSetUsage::new),
                new NamedWriteableRegistry.Entry(RoleMapperExpression.class, AllExpression.NAME, AllExpression::new),
                new NamedWriteableRegistry.Entry(RoleMapperExpression.class, AnyExpression.NAME, AnyExpression::new),
                new NamedWriteableRegistry.Entry(RoleMapperExpression.class, FieldExpression.NAME, FieldExpression::new),
                new NamedWriteableRegistry.Entry(RoleMapperExpression.class, ExceptExpression.NAME, ExceptExpression::new),
                // watcher
                new NamedWriteableRegistry.Entry(MetaData.Custom.class, WatcherMetaData.TYPE, WatcherMetaData::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, WatcherMetaData.TYPE, WatcherMetaData::readDiffFrom),
                new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.WATCHER, WatcherFeatureSetUsage::new),
                // licensing
                new NamedWriteableRegistry.Entry(MetaData.Custom.class, LicensesMetaData.TYPE, LicensesMetaData::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, LicensesMetaData.TYPE, LicensesMetaData::readDiffFrom)
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
                // ML - Custom metadata
                new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField("ml"),
                        parser -> MlMetadata.METADATA_PARSER.parse(parser, null).build()),
                new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(PersistentTasksCustomMetaData.TYPE),
                        PersistentTasksCustomMetaData::fromXContent),
                // ML - Persistent action requests
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(StartDatafeedAction.TASK_NAME),
                        StartDatafeedAction.DatafeedParams::fromXContent),
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(OpenJobAction.TASK_NAME),
                        OpenJobAction.JobParams::fromXContent),
                // ML - Task statuses
                new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(DatafeedState.NAME), DatafeedState::fromXContent),
                new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(JobTaskStatus.NAME), JobTaskStatus::fromXContent),
                // watcher
                new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(WatcherMetaData.TYPE),
                        WatcherMetaData::fromXContent),
                // licensing
                new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(LicensesMetaData.TYPE),
                        LicensesMetaData::fromXContent)
        );
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(
            final Settings settings,
            final ThreadPool threadPool,
            final BigArrays bigArrays,
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
        return Collections.singletonMap(SecurityField.NAME4, () -> new SecurityNetty4Transport(settings, threadPool,
                networkService, bigArrays, namedWriteableRegistry, circuitBreakerService, sslService));
    }

}
