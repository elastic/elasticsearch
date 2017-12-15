/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.transport.TransportMessage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Cluster state update request that allows to create an index
 */
public class CreateIndexClusterStateUpdateRequest extends ClusterStateUpdateRequest<CreateIndexClusterStateUpdateRequest> {

    private final TransportMessage originalMessage;
    private final String cause;
    private final String index;
    private final String providedName;
    private final boolean updateAllTypes;
    private Index recoverFrom;
    private ResizeType resizeType;

    private IndexMetaData.State state = IndexMetaData.State.OPEN;

    private Settings settings = Settings.Builder.EMPTY_SETTINGS;

    private final Map<String, String> mappings = new HashMap<>();

    private final Set<Alias> aliases = new HashSet<>();

    private final Map<String, IndexMetaData.Custom> customs = new HashMap<>();

    private final Set<ClusterBlock> blocks = new HashSet<>();

    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    public CreateIndexClusterStateUpdateRequest(TransportMessage originalMessage, String cause, String index, String providedName,
                                                boolean updateAllTypes) {
        this.originalMessage = originalMessage;
        this.cause = cause;
        this.index = index;
        this.updateAllTypes = updateAllTypes;
        this.providedName = providedName;
    }

    public CreateIndexClusterStateUpdateRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public CreateIndexClusterStateUpdateRequest mappings(Map<String, String> mappings) {
        this.mappings.putAll(mappings);
        return this;
    }

    public CreateIndexClusterStateUpdateRequest aliases(Set<Alias> aliases) {
        this.aliases.addAll(aliases);
        return this;
    }

    public CreateIndexClusterStateUpdateRequest customs(Map<String, IndexMetaData.Custom> customs) {
        this.customs.putAll(customs);
        return this;
    }

    public CreateIndexClusterStateUpdateRequest blocks(Set<ClusterBlock> blocks) {
        this.blocks.addAll(blocks);
        return this;
    }

    public CreateIndexClusterStateUpdateRequest state(IndexMetaData.State state) {
        this.state = state;
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

    public TransportMessage originalMessage() {
        return originalMessage;
    }

    public String cause() {
        return cause;
    }

    public String index() {
        return index;
    }

    public IndexMetaData.State state() {
        return state;
    }

    public Settings settings() {
        return settings;
    }

    public Map<String, String> mappings() {
        return mappings;
    }

    public Set<Alias> aliases() {
        return aliases;
    }

    public Map<String, IndexMetaData.Custom> customs() {
        return customs;
    }

    public Set<ClusterBlock> blocks() {
        return blocks;
    }

    public Index recoverFrom() {
        return recoverFrom;
    }

    /** True if all fields that span multiple types should be updated, false otherwise */
    public boolean updateAllTypes() {
        return updateAllTypes;
    }

    /**
     * The name that was provided by the user. This might contain a date math expression.
     * @see IndexMetaData#SETTING_INDEX_PROVIDED_NAME
     */
    public String getProvidedName() {
        return providedName;
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
}
