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

package org.elasticsearch.client;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A request to delete a document.
 */
public class DeleteRequest implements Validatable {

    private final String index, id;

    private String routing;
    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;
    private TimeValue timeout;
    private RefreshPolicy refreshPolicy = RefreshPolicy.NONE;
    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    /**
     * Create a request to delete the given {@code id} in {@code index}.
     */
    public DeleteRequest(String index, String id) {
        this.index = Objects.requireNonNull(index);
        this.id = Objects.requireNonNull(id);
    }

    @Override
    public Optional<ValidationException> validate() {
        List<String> validationErrors = new ArrayList<>();
        if (versionType.validateVersionForWrites(version) == false) {
            validationErrors.add("illegal version value [" + version + "] for version type [" + versionType.name() + "]");
        }
        if (versionType == VersionType.FORCE) {
            validationErrors.add("version type [force] may no longer be used");
        }
        if (validationErrors.isEmpty()) {
            return Optional.empty();
        } else {
            ValidationException ex = new ValidationException();
            for (String error : validationErrors) {
                ex.addValidationError(error);
            }
            return Optional.of(ex);
        }
    }

    /**
     * Return the name of the index that has the document to delete.
     */
    public String getIndex() {
        return index;
    }

    /**
     * Return the id of the document to delete.
     */
    public String getId() {
        return id;
    }

    /**
     * Set the routing value.
     * @see #getRouting()
     */
    public DeleteRequest setRouting(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    /**
     * Return the routing value, ie. the value whose hash is used to find ou
     * which shard has the document to delete. {@code null} values mean that
     * the {@link #getId()} should be used.
     */
    public String getRouting() {
        return this.routing;
    }

    /**
     * Set the expected version of the document to delete. If the document
     * has a different version then the delete request will fail.
     */
    public DeleteRequest setVersion(long version) {
        this.version = version;
        return this;
    }

    /**
     * Return the expected version of the document to delete.
     */
    public long getVersion() {
        return this.version;
    }

    /**
     * Sets the type of versioning to use.
     */
    public DeleteRequest setVersionType(VersionType versionType) {
        this.versionType = Objects.requireNonNull(versionType);
        return this;
    }

    /**
     * Return the type of versioning to use.
     */
    public VersionType getVersionType() {
        return this.versionType;
    }

    /** Return the timeout of this delete request. */
    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * Set the timeout of this delete request.
     */
    public DeleteRequest setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Set the timeout of this delete request.
     */
    public DeleteRequest setTimeout(String timeout) {
        return setTimeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    /**
     * Return the refresh policy for this delete request.
     */
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    /**
     * Set the refresh policy for this delete request and return {@code this}.
     */
    public DeleteRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    /**
     * How many shards should be active.
     * @see {@link DeleteRequest#setWaitForActiveShards(ActiveShardCount)}.
     */
    public ActiveShardCount getWaitForActiveShards() {
        return waitForActiveShards;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the replication
     * operation. Defaults to {@link ActiveShardCount#DEFAULT}, which requires one shard copy
     * (the primary) to be active. Set this value to {@link ActiveShardCount#ALL} to
     * wait for all shards (primary and all replicas) to be active. Otherwise, use
     * {@link ActiveShardCount#from(int)} to set this value to any non-negative integer, up to the
     * total number of shard copies (number of replicas + 1).
     */
    public DeleteRequest setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = Objects.requireNonNull(waitForActiveShards);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id, routing, version, versionType, timeout, refreshPolicy, waitForActiveShards);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DeleteRequest that = (DeleteRequest) obj;
        return Objects.equals(index, that.index) &&
                Objects.equals(id, that.id) &&
                Objects.equals(routing, that.routing) &&
                version == that.version &&
                versionType == that.versionType &&
                Objects.equals(timeout, that.timeout) &&
                refreshPolicy == that.refreshPolicy &&
                Objects.equals(waitForActiveShards, that.waitForActiveShards);
    }

    @Override
    public String toString() {
        return "delete {[" + index + "][" + id + "]}";
    }

}
