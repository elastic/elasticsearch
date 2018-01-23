/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public class AllocateAction implements LifecycleAction {

    public static final String NAME = "allocate";
    public static final ParseField INCLUDE_FIELD = new ParseField("include");
    public static final ParseField EXCLUDE_FIELD = new ParseField("exclude");
    public static final ParseField REQUIRE_FIELD = new ParseField("require");

    private static final Logger logger = ESLoggerFactory.getLogger(AllocateAction.class);
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AllocateAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new AllocateAction((Map<String, String>) a[0], (Map<String, String>) a[1], (Map<String, String>) a[2]));

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapStrings(), INCLUDE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapStrings(), EXCLUDE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapStrings(), REQUIRE_FIELD);
    }

    private final Map<String, String> include;
    private final Map<String, String> exclude;
    private final Map<String, String> require;
    private AllocationDeciders allocationDeciders;

    public static AllocateAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public AllocateAction(Map<String, String> include, Map<String, String> exclude, Map<String, String> require) {
        if (include == null) {
            this.include = Collections.emptyMap();
        } else {
            this.include = include;
        }
        if (exclude == null) {
            this.exclude = Collections.emptyMap();
        } else {
            this.exclude = exclude;
        }
        if (require == null) {
            this.require = Collections.emptyMap();
        } else {
            this.require = require;
        }
        if (this.include.isEmpty() && this.exclude.isEmpty() && this.require.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one of " + INCLUDE_FIELD.getPreferredName() + ", " + EXCLUDE_FIELD.getPreferredName() + " or "
                            + REQUIRE_FIELD.getPreferredName() + "must contain attributes for action " + NAME);
        }
        FilterAllocationDecider decider = new FilterAllocationDecider(Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        this.allocationDeciders = new AllocationDeciders(Settings.EMPTY, Collections.singletonList(decider));
    }

    @SuppressWarnings("unchecked")
    public AllocateAction(StreamInput in) throws IOException {
        this((Map<String, String>) in.readGenericValue(), (Map<String, String>) in.readGenericValue(),
                (Map<String, String>) in.readGenericValue());
    }

    public Map<String, String> getInclude() {
        return include;
    }

    public Map<String, String> getExclude() {
        return exclude;
    }

    public Map<String, String> getRequire() {
        return require;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(include);
        out.writeGenericValue(exclude);
        out.writeGenericValue(require);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INCLUDE_FIELD.getPreferredName(), include);
        builder.field(EXCLUDE_FIELD.getPreferredName(), exclude);
        builder.field(REQUIRE_FIELD.getPreferredName(), require);
        builder.endObject();
        return builder;
    }

    /**
     * Inspects the <code>existingSettings</code> and adds any attributes that
     * are missing for the given <code>settingsPrefix</code> to the
     * <code>newSettingsBuilder</code>.
     */
    private void addMissingAttrs(Map<String, String> newAttrs, String settingPrefix, Settings existingSettings,
            Settings.Builder newSettingsBuilder) {
        newAttrs.entrySet().stream().filter(e -> {
            String existingValue = existingSettings.get(settingPrefix + e.getKey());
            return existingValue == null || (existingValue.equals(e.getValue()) == false);
        }).forEach(e -> newSettingsBuilder.put(settingPrefix + e.getKey(), e.getValue()));
    }

    void execute(Index index, BiConsumer<Settings, Listener> settingsUpdater, ClusterState clusterState, ClusterSettings clusterSettings,
            Listener listener) {
        // We only want to make progress if all shards are active so check that
        // first
        if (ActiveShardCount.ALL.enoughShardsActive(clusterState, index.getName()) == false) {
            logger.debug("[{}] lifecycle action for index [{}] cannot make progress because not all shards are active", NAME,
                    index.getName());
            listener.onSuccess(false);
            return;
        }
        IndexMetaData idxMeta = clusterState.metaData().index(index);
        if (idxMeta == null) {
            listener.onFailure(
                    new IndexNotFoundException("Index not found when executing " + NAME + " lifecycle action.", index.getName()));
            return;
        }
        Settings existingSettings = idxMeta.getSettings();
        Settings.Builder newSettings = Settings.builder();
        addMissingAttrs(include, IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey(), existingSettings, newSettings);
        addMissingAttrs(exclude, IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey(), existingSettings, newSettings);
        addMissingAttrs(require, IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey(), existingSettings, newSettings);
        Settings newAllocationIncludes = newSettings.build();
        if (newAllocationIncludes.isEmpty()) {
            // All the allocation attributes are already set so just need to
            // check if the allocation has happened
            RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState, null,
                    System.nanoTime());
            int allocationPendingShards = 0;
            List<ShardRouting> allShards = clusterState.getRoutingTable().allShards(index.getName());
            for (ShardRouting shardRouting : allShards) {
                assert shardRouting.active() : "Shard not active, found " + shardRouting.state() + "for shard with id: "
                        + shardRouting.shardId();
                String currentNodeId = shardRouting.currentNodeId();
                boolean canRemainOnCurrentNode = allocationDeciders.canRemain(shardRouting,
                        clusterState.getRoutingNodes().node(currentNodeId), allocation).type() == Decision.Type.YES;
                if (canRemainOnCurrentNode == false) {
                    allocationPendingShards++;
                }
            }
            if (allocationPendingShards > 0) {
                logger.debug("[{}] lifecycle action for index [{}] waiting for [{}] shards "
                        + "to be allocated to nodes matching the given filters", NAME, index.getName(), allocationPendingShards);
                listener.onSuccess(false);
            } else {
                logger.debug("[{}] lifecycle action for index [{}] complete", NAME, index.getName());
                listener.onSuccess(true);
            }
        } else {
            // We have some allocation attributes to set
            settingsUpdater.accept(newAllocationIncludes, listener);
        }
    }

    @Override
    public void execute(Index index, Client client, ClusterService clusterService, Listener listener) {
        ClusterState clusterState = clusterService.state();
        BiConsumer<Settings, Listener> settingsUpdater = (s, l) -> {

            client.admin().indices().updateSettings(new UpdateSettingsRequest(s, index.getName()),
                    new ActionListener<UpdateSettingsResponse>() {
                        @Override
                        public void onResponse(UpdateSettingsResponse updateSettingsResponse) {
                            l.onSuccess(false);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            l.onFailure(e);
                        }
                    });
        };
        execute(index, settingsUpdater, clusterState, clusterService.getClusterSettings(), listener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(include, exclude, require);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        AllocateAction other = (AllocateAction) obj;
        return Objects.equals(include, other.include) && Objects.equals(exclude, other.exclude) && Objects.equals(require, other.require);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
