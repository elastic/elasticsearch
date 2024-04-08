/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemDataStreamDescriptor;

import java.util.HashSet;
import java.util.Set;

/**
 * Cluster state update request that allows to create an index
 */
public class CreateIndexClusterStateUpdateRequest extends ClusterStateUpdateRequest<CreateIndexClusterStateUpdateRequest> {

    private final String cause;
    private final String index;
    private String dataStreamName;
    private final String providedName;
    private long nameResolvedAt;
    private Index recoverFrom;
    private ResizeType resizeType;
    private boolean copySettings;
    private SystemDataStreamDescriptor systemDataStreamDescriptor;

    private Settings settings = Settings.EMPTY;

    private String mappings = "{}";

    private final Set<Alias> aliases = new HashSet<>();

    private final Set<ClusterBlock> blocks = new HashSet<>();

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    private boolean performReroute = true;

    private ComposableIndexTemplate matchingTemplate;

    public CreateIndexClusterStateUpdateRequest(String cause, String index, String providedName) {
        this.cause = cause;
        this.index = index;
        this.providedName = providedName;
    }

    public CreateIndexClusterStateUpdateRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public CreateIndexClusterStateUpdateRequest mappings(String mappings) {
        this.mappings = mappings;
        return this;
    }

    public CreateIndexClusterStateUpdateRequest aliases(Set<Alias> aliases) {
        this.aliases.addAll(aliases);
        return this;
    }

    public CreateIndexClusterStateUpdateRequest recoverFrom(Index recoverFrom) {
        this.recoverFrom = recoverFrom;
        return this;
    }

    public CreateIndexClusterStateUpdateRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    public CreateIndexClusterStateUpdateRequest resizeType(ResizeType resizeType) {
        this.resizeType = resizeType;
        return this;
    }

    public CreateIndexClusterStateUpdateRequest copySettings(final boolean copySettings) {
        this.copySettings = copySettings;
        return this;
    }

    /**
     * At what point in time the provided name was resolved into the index name
     */
    public CreateIndexClusterStateUpdateRequest nameResolvedInstant(long nameResolvedAt) {
        this.nameResolvedAt = nameResolvedAt;
        return this;
    }

    public CreateIndexClusterStateUpdateRequest systemDataStreamDescriptor(SystemDataStreamDescriptor systemDataStreamDescriptor) {
        this.systemDataStreamDescriptor = systemDataStreamDescriptor;
        return this;
    }

    public String cause() {
        return cause;
    }

    public String index() {
        return index;
    }

    public Settings settings() {
        return settings;
    }

    public String mappings() {
        return mappings;
    }

    public Set<Alias> aliases() {
        return aliases;
    }

    public Set<ClusterBlock> blocks() {
        return blocks;
    }

    public Index recoverFrom() {
        return recoverFrom;
    }

    public SystemDataStreamDescriptor systemDataStreamDescriptor() {
        return systemDataStreamDescriptor;
    }

    /**
     * The name that was provided by the user. This might contain a date math expression.
     * @see IndexMetadata#SETTING_INDEX_PROVIDED_NAME
     */
    public String getProvidedName() {
        return providedName;
    }

    /**
     * The instant at which the name provided by the user was resolved
     */
    public long getNameResolvedAt() {
        return nameResolvedAt;
    }

    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    /**
     * Returns the resize type or null if this is an ordinary create index request
     */
    public ResizeType resizeType() {
        return resizeType;
    }

    public boolean copySettings() {
        return copySettings;
    }

    /**
     * Returns the name of the data stream this new index will be part of.
     * If this new index will not be part of a data stream then this returns <code>null</code>.
     */
    public String dataStreamName() {
        return dataStreamName;
    }

    public CreateIndexClusterStateUpdateRequest dataStreamName(String dataStreamName) {
        this.dataStreamName = dataStreamName;
        return this;
    }

    public boolean performReroute() {
        return performReroute;
    }

    public CreateIndexClusterStateUpdateRequest performReroute(boolean performReroute) {
        this.performReroute = performReroute;
        return this;
    }

    /**
     * @return The composable index template that matches with the index that will be created by this request.
     */
    public ComposableIndexTemplate matchingTemplate() {
        return matchingTemplate;
    }

    /**
     * Sets the composable index template that matches with index that will be created by this request.
     */
    public CreateIndexClusterStateUpdateRequest setMatchingTemplate(ComposableIndexTemplate matchingTemplate) {
        this.matchingTemplate = matchingTemplate;
        return this;
    }

    @Override
    public String toString() {
        return "CreateIndexClusterStateUpdateRequest{"
            + "cause='"
            + cause
            + '\''
            + ", index='"
            + index
            + '\''
            + ", dataStreamName='"
            + dataStreamName
            + '\''
            + ", providedName='"
            + providedName
            + '\''
            + ", recoverFrom="
            + recoverFrom
            + ", resizeType="
            + resizeType
            + ", copySettings="
            + copySettings
            + ", settings="
            + settings
            + ", aliases="
            + aliases
            + ", blocks="
            + blocks
            + ", waitForActiveShards="
            + waitForActiveShards
            + ", systemDataStreamDescriptor="
            + systemDataStreamDescriptor
            + ", matchingTemplate="
            + matchingTemplate
            + '}';
    }
}
