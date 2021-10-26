/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;


public class RolloverRequestBuilder extends MasterNodeOperationRequestBuilder<RolloverRequest, RolloverResponse,
    RolloverRequestBuilder> {
    public RolloverRequestBuilder(ElasticsearchClient client, RolloverAction action) {
        super(client, action, new RolloverRequest());
    }

    public RolloverRequestBuilder setRolloverTarget(String rolloverTarget) {
        this.request.setRolloverTarget(rolloverTarget);
        return this;
    }

    public RolloverRequestBuilder setNewIndexName(String newIndexName) {
        this.request.setNewIndexName(newIndexName);
        return this;
    }

    public RolloverRequestBuilder addMaxIndexAgeCondition(TimeValue age) {
        this.request.addMaxIndexAgeCondition(age);
        return this;
    }

    public RolloverRequestBuilder addMaxIndexDocsCondition(long docs) {
        this.request.addMaxIndexDocsCondition(docs);
        return this;
    }

    public RolloverRequestBuilder addMaxIndexSizeCondition(ByteSizeValue size) {
        this.request.addMaxIndexSizeCondition(size);
        return this;
    }

    public RolloverRequestBuilder addMaxPrimaryShardSizeCondition(ByteSizeValue size) {
        this.request.addMaxPrimaryShardSizeCondition(size);
        return this;
    }

    public RolloverRequestBuilder dryRun(boolean dryRun) {
        this.request.dryRun(dryRun);
        return this;
    }

    public RolloverRequestBuilder settings(Settings settings) {
        this.request.getCreateIndexRequest().settings(settings);
        return this;
    }

    public RolloverRequestBuilder alias(Alias alias) {
        this.request.getCreateIndexRequest().alias(alias);
        return this;
    }

    public RolloverRequestBuilder simpleMapping(String... source) {
        this.request.getCreateIndexRequest().simpleMapping(source);
        return this;
    }

    public RolloverRequestBuilder mapping(String source) {
        this.request.getCreateIndexRequest().mapping(source);
        return this;
    }

    /**
     * Sets the number of shard copies that should be active for creation of the
     * new rollover index to return. Defaults to {@link ActiveShardCount#DEFAULT}, which will
     * wait for one shard copy (the primary) to become active. Set this value to
     * {@link ActiveShardCount#ALL} to wait for all shards (primary and all replicas) to be active
     * before returning. Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Index creation will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link RolloverResponse#isShardsAcknowledged()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public RolloverRequestBuilder waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.request.getCreateIndexRequest().waitForActiveShards(waitForActiveShards);
        return this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public RolloverRequestBuilder waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }
}
