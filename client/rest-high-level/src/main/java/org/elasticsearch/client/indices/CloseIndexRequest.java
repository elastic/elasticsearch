/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;

import java.util.Optional;

/**
 * A request to close an index.
 */
public class CloseIndexRequest extends TimedRequest implements Validatable {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpen();

    private String[] indices;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
    private ActiveShardCount waitForActiveShards = null;

    /**
     * Creates a new close index request
     *
     * @param indices the indices to close
     */
    public CloseIndexRequest(String... indices) {
        this.indices = indices;
    }

    /**
     * Returns the indices to close
     */
    public String[] indices() {
        return indices;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the current behaviour when it comes to index names and wildcard indices expressions
     */
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     */
    public CloseIndexRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Returns the wait for active shard count or null if the default should be used
     */
    public ActiveShardCount waitForActiveShards() {
        return waitForActiveShards;
    }

    /**
     * Sets the number of shard copies that should be active before a close-index request returns. Defaults to {@code null}, which means not
     * to wait. However the default behaviour is deprecated and will change in version 8. You can opt-in to the new default behaviour now by
     * setting this to {@link ActiveShardCount#DEFAULT}, which will wait according to the setting {@code index.write.wait_for_active_shards}
     * which by default will wait for one shard, the primary. Set this value to {@link ActiveShardCount#ALL} to wait for all shards (primary
     * and all replicas) to be active before returning. Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer up to the number of copies per shard (number of replicas + 1), to wait for the desired amount of shard copies
     * to become active before returning. To explicitly preserve today's default behaviour and suppress the deprecation warning, set this
     * property to {@code ActiveShardCount.from(0)}.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public CloseIndexRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (indices == null || indices.length == 0) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError("index is missing");
            return Optional.of(validationException);
        } else {
            return Optional.empty();
        }
    }
}
