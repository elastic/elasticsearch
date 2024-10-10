/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.LicensesMetadata;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.aggregatemetric.AggregateMetricFeatureSetUsage;
import org.elasticsearch.xpack.core.analytics.AnalyticsFeatureSetUsage;
import org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage;
import org.elasticsearch.xpack.core.application.ProfilingUsage;
import org.elasticsearch.xpack.core.archive.ArchiveFeatureSetUsage;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.datastreams.DataStreamFeatureSetUsage;
import org.elasticsearch.xpack.core.datastreams.DataStreamLifecycleFeatureSetUsage;
import org.elasticsearch.xpack.core.datatiers.DataTiersFeatureSetUsage;
import org.elasticsearch.xpack.core.downsample.DownsampleShardStatus;
import org.elasticsearch.xpack.core.enrich.EnrichFeatureSetUsage;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.elasticsearch.xpack.core.eql.EqlFeatureSetUsage;
import org.elasticsearch.xpack.core.esql.EsqlFeatureSetUsage;
import org.elasticsearch.xpack.core.frozen.FrozenIndicesFeatureSetUsage;
import org.elasticsearch.xpack.core.graph.GraphFeatureSetUsage;
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
import org.elasticsearch.xpack.core.inference.InferenceFeatureSetUsage;
import org.elasticsearch.xpack.core.logstash.LogstashFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.MachineLearningFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskState;
import org.elasticsearch.xpack.core.ml.search.WeightedTokensQueryBuilder;
import org.elasticsearch.xpack.core.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.xpack.core.rollup.RollupFeatureSetUsage;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotFeatureSetUsage;
import org.elasticsearch.xpack.core.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.core.security.authc.TokenMetadata;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AllExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AnyExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExceptExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.RoleMapperExpression;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissionGroup;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.support.SecurityMigrationTaskParams;
import org.elasticsearch.xpack.core.slm.SLMFeatureSetUsage;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.spatial.SpatialFeatureSetUsage;
import org.elasticsearch.xpack.core.sql.SqlFeatureSetUsage;
import org.elasticsearch.xpack.core.transform.TransformFeatureSetUsage;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

// TODO: merge this into XPackPlugin
public class XPackClientPlugin extends Plugin implements ActionPlugin, SearchPlugin, NetworkPlugin {

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
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Stream.of(
            // graph
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.GRAPH, GraphFeatureSetUsage::new),
            // logstash
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.LOGSTASH, LogstashFeatureSetUsage::new),
            // ML
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.MACHINE_LEARNING, MachineLearningFeatureSetUsage::new),
            // inference
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.INFERENCE, InferenceFeatureSetUsage::new),
            // monitoring
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.MONITORING, MonitoringFeatureSetUsage::new),
            // security
            new NamedWriteableRegistry.Entry(ClusterState.Custom.class, TokenMetadata.TYPE, TokenMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, TokenMetadata.TYPE, TokenMetadata::readDiffFrom),
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.SECURITY, SecurityFeatureSetUsage::new),
            // security : configurable cluster privileges
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
            new NamedWriteableRegistry.Entry(
                ConfigurableClusterPrivilege.class,
                ConfigurableClusterPrivileges.ManageRolesPrivilege.WRITEABLE_NAME,
                ConfigurableClusterPrivileges.ManageRolesPrivilege::createFrom
            ),
            // security : role-mappings
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, RoleMappingMetadata.TYPE, RoleMappingMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, RoleMappingMetadata.TYPE, RoleMappingMetadata::readDiffFrom),
            new NamedWriteableRegistry.Entry(RoleMapperExpression.class, AllExpression.NAME, AllExpression::new),
            new NamedWriteableRegistry.Entry(RoleMapperExpression.class, AnyExpression.NAME, AnyExpression::new),
            new NamedWriteableRegistry.Entry(RoleMapperExpression.class, FieldExpression.NAME, FieldExpression::new),
            new NamedWriteableRegistry.Entry(RoleMapperExpression.class, ExceptExpression.NAME, ExceptExpression::new),
            // security : role descriptors
            new NamedWriteableRegistry.Entry(RemoteClusterPermissions.class, RemoteClusterPermissions.NAME, RemoteClusterPermissions::new),
            new NamedWriteableRegistry.Entry(
                RemoteClusterPermissionGroup.class,
                RemoteClusterPermissionGroup.NAME,
                RemoteClusterPermissionGroup::new
            ),
            // eql
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.EQL, EqlFeatureSetUsage::new),
            // esql
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.ESQL, EsqlFeatureSetUsage::new),
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
            new NamedWriteableRegistry.Entry(Task.Status.class, DownsampleShardStatus.NAME, DownsampleShardStatus::new),
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
            new NamedWriteableRegistry.Entry(
                XPackFeatureSet.Usage.class,
                XPackField.DATA_STREAM_LIFECYCLE,
                DataStreamLifecycleFeatureSetUsage::new
            ),
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
            ),
            new NamedWriteableRegistry.Entry(XPackFeatureSet.Usage.class, XPackField.UNIVERSAL_PROFILING, ProfilingUsage::new),
            new NamedWriteableRegistry.Entry(
                PersistentTaskParams.class,
                SecurityMigrationTaskParams.TASK_NAME,
                SecurityMigrationTaskParams::new
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
            ),
            // security
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(SecurityMigrationTaskParams.TASK_NAME),
                SecurityMigrationTaskParams::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(RoleMappingMetadata.TYPE),
                RoleMappingMetadata::fromXContent
            )
        );
    }

    // TODO: The WeightedTokensBuilder is slated for removal after the SparseVectorQueryBuilder is available.
    // The logic to create a Boolean query based on weighted tokens will remain and/or be moved to server.
    @Override
    public List<SearchPlugin.QuerySpec<?>> getQueries() {
        return List.of(
            new SearchPlugin.QuerySpec<QueryBuilder>(
                WeightedTokensQueryBuilder.NAME,
                WeightedTokensQueryBuilder::new,
                WeightedTokensQueryBuilder::fromXContent
            )
        );
    }
}
