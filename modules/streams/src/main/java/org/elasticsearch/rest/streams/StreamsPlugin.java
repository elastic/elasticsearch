/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.streams;

import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.streams.logs.LogsStreamsActivationToggleAction;
import org.elasticsearch.rest.streams.logs.RestSetLogStreamsEnabledAction;
import org.elasticsearch.rest.streams.logs.RestStreamsStatusAction;
import org.elasticsearch.rest.streams.logs.StreamsStatusAction;
import org.elasticsearch.rest.streams.logs.TransportLogsStreamsToggleActivation;
import org.elasticsearch.rest.streams.logs.TransportStreamsStatusAction;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This plugin provides the Streams feature which builds upon data streams to
 * provide the user with a more "batteries included" experience for ingesting large
 * streams of data, such as logs.
 */
public class StreamsPlugin extends Plugin implements ActionPlugin {

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
        if (DataStream.LOGS_STREAM_FEATURE_FLAG) {
            return List.of(new RestSetLogStreamsEnabledAction(), new RestStreamsStatusAction());
        }
        return Collections.emptyList();
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(LogsStreamsActivationToggleAction.INSTANCE, TransportLogsStreamsToggleActivation.class),
            new ActionHandler(StreamsStatusAction.INSTANCE, TransportStreamsStatusAction.class)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, StreamsMetadata.TYPE, StreamsMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, StreamsMetadata.TYPE, StreamsMetadata::readDiffFrom)
        );
    }
}
