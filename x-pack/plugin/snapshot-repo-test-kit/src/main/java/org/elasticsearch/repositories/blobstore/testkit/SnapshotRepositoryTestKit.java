/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.testkit.analyze.RepositoryAnalyzeAction;
import org.elasticsearch.repositories.blobstore.testkit.analyze.RestRepositoryAnalyzeAction;
import org.elasticsearch.repositories.blobstore.testkit.integrity.RepositoryVerifyIntegrityTask;
import org.elasticsearch.repositories.blobstore.testkit.integrity.RestRepositoryVerifyIntegrityAction;
import org.elasticsearch.repositories.blobstore.testkit.integrity.TransportRepositoryVerifyIntegrityCoordinationAction;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class SnapshotRepositoryTestKit extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(RepositoryAnalyzeAction.INSTANCE, RepositoryAnalyzeAction.class),
            new ActionHandler(
                TransportRepositoryVerifyIntegrityCoordinationAction.INSTANCE,
                TransportRepositoryVerifyIntegrityCoordinationAction.class
            )
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new RestRepositoryAnalyzeAction(), new RestRepositoryVerifyIntegrityAction());
    }

    public static void humanReadableNanos(XContentBuilder builder, String rawFieldName, String readableFieldName, long nanos)
        throws IOException {
        assert rawFieldName.equals(readableFieldName) == false : rawFieldName + " vs " + readableFieldName;

        if (builder.humanReadable()) {
            builder.field(readableFieldName, TimeValue.timeValueNanos(nanos).toHumanReadableString(2));
        }

        builder.field(rawFieldName, nanos);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                Task.Status.class,
                RepositoryVerifyIntegrityTask.Status.NAME,
                RepositoryVerifyIntegrityTask.Status::new
            )
        );
    }
}
