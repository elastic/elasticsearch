/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public class SnapshotRepositoryTestKit extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(RepositoryAnalyzeAction.INSTANCE, RepositoryAnalyzeAction.TransportAction.class),
            new ActionHandler<>(BlobAnalyzeAction.INSTANCE, BlobAnalyzeAction.TransportAction.class),
            new ActionHandler<>(GetBlobChecksumAction.INSTANCE, GetBlobChecksumAction.TransportAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestRepositoryAnalyzeAction());
    }

    static void humanReadableNanos(XContentBuilder builder, String rawFieldName, String readableFieldName, long nanos) throws IOException {
        assert rawFieldName.equals(readableFieldName) == false : rawFieldName + " vs " + readableFieldName;

        if (builder.humanReadable()) {
            builder.field(readableFieldName, TimeValue.timeValueNanos(nanos).toHumanReadableString(2));
        }

        builder.field(rawFieldName, nanos);
    }
}
