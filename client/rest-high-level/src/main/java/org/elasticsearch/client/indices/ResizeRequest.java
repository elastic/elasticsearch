/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.indices;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Request class to resize an index
 */
public class ResizeRequest extends TimedRequest implements Validatable, ToXContentObject {

    private ActiveShardCount waitForActiveShards;
    private final String sourceIndex;
    private final String targetIndex;
    private Settings settings = Settings.EMPTY;
    private Set<Alias> aliases = new HashSet<>();
    private ByteSizeValue maxPrimaryShardSize;

    /**
     * Creates a new resize request
     * @param targetIndex   the new index to create with resized shards
     * @param sourceIndex   the index to resize
     */
    public ResizeRequest(String targetIndex, String sourceIndex) {
        this.targetIndex = Objects.requireNonNull(targetIndex);
        this.sourceIndex = Objects.requireNonNull(sourceIndex);
    }

    /**
     * Sets the Settings to be used on the target index
     */
    public ResizeRequest setSettings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Returns the Settings to be used on the target index
     */
    public Settings getSettings() {
        return this.settings;
    }

    /**
     * Sets the Aliases to be used on the target index
     */
    public ResizeRequest setAliases(List<Alias> aliases) {
        this.aliases.clear();
        this.aliases.addAll(aliases);
        return this;
    }

    /**
     * Returns the Aliases to be used on the target index
     */
    public Set<Alias> getAliases() {
        return Collections.unmodifiableSet(this.aliases);
    }

    /**
     * Sets the max primary shard size of the target index
     */
    public void setMaxPrimaryShardSize(ByteSizeValue maxPrimaryShardSize) {
        this.maxPrimaryShardSize = maxPrimaryShardSize;
    }

    /**
     * Return the max primary shard size of the target index
     */
    public ByteSizeValue getMaxPrimaryShardSize() {
        return maxPrimaryShardSize;
    }

    @Override
    public Optional<ValidationException> validate() {
        ValidationException validationException = new ValidationException();
        if (settings.getByPrefix("index.sort.").isEmpty() == false) {
            validationException.addValidationError("can't override index sort when resizing an index");
        }
        return validationException.validationErrors().isEmpty() ? Optional.empty() : Optional.of(validationException);
    }

    /**
     * Returns the target index name
     */
    public String getTargetIndex() {
        return targetIndex;
    }

    /**
     * Returns the source index name
     */
    public String getSourceIndex() {
        return sourceIndex;
    }

    /**
     * Sets the number of shard copies that should be active for creation of the
     * new shrunken index to return. Defaults to {@link ActiveShardCount#DEFAULT}, which will
     * wait for one shard copy (the primary) to become active. Set this value to
     * {@link ActiveShardCount#ALL} to wait for all shards (primary and all replicas) to be active
     * before returning. Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Index creation will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link ResizeResponse#isShardsAcknowledged()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public ResizeRequest setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public ResizeRequest setWaitForActiveShards(final int waitForActiveShards) {
        return setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    public ActiveShardCount getWaitForActiveShards() {
        return waitForActiveShards;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(CreateIndexRequest.SETTINGS.getPreferredName());
            {
                settings.toXContent(builder, params);
            }
            builder.endObject();
            builder.startObject(CreateIndexRequest.ALIASES.getPreferredName());
            {
                for (Alias alias : aliases) {
                    alias.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

}
