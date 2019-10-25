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
package org.elasticsearch.client.indices;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Request class to shrink an index into a single shard
 */
public class ResizeRequest extends TimedRequest implements Validatable, ToXContentObject {

    private CreateIndexRequest targetIndexRequest;
    private String sourceIndex;
    private ResizeType type = ResizeType.SHRINK;
    private Boolean copySettings = true;

    public ResizeRequest(String targetIndex, String sourceIndex) {
        this.targetIndexRequest = new CreateIndexRequest(targetIndex);
        this.sourceIndex = sourceIndex;
    }

    @Override
    public Optional<ValidationException> validate() {
        ValidationException validationException = new ValidationException();
        if (sourceIndex == null) {
            validationException.addValidationError("source index is missing");
        }
        if (targetIndexRequest == null) {
            validationException.addValidationError("target index request is missing");
        }
        if (targetIndexRequest.settings().getByPrefix("index.sort.").isEmpty() == false) {
            validationException.addValidationError("can't override index sort when resizing an index");
        }
        if (type == ResizeType.SPLIT && IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.exists(targetIndexRequest.settings()) == false) {
            validationException.addValidationError("index.number_of_shards is required for split operations");
        }
        assert copySettings == null || copySettings;
        return validationException.validationErrors().isEmpty() ? Optional.empty() : Optional.of(validationException);
    }

    public void setSourceIndex(String index) {
        this.sourceIndex = index;
    }

    public void setTargetIndex(CreateIndexRequest targetIndexRequest) {
        this.targetIndexRequest = Objects.requireNonNull(targetIndexRequest, "target index request must not be null");
    }

    /**
     * Returns the {@link CreateIndexRequest} for the shrink index
     */
    public CreateIndexRequest getTargetIndexRequest() {
        return targetIndexRequest;
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
    public void setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.getTargetIndexRequest().waitForActiveShards(waitForActiveShards);
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public void setWaitForActiveShards(final int waitForActiveShards) {
        setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * The type of the resize operation
     */
    public void setResizeType(ResizeType type) {
        this.type = Objects.requireNonNull(type);
    }

    /**
     * Returns the type of the resize operation
     */
    public ResizeType getResizeType() {
        return type;
    }

    public void setCopySettings(final Boolean copySettings) {
        if (copySettings != null && copySettings == false) {
            throw new IllegalArgumentException("[copySettings] can not be explicitly set to [false]");
        }
        this.copySettings = copySettings;
    }

    public Boolean getCopySettings() {
        return copySettings;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(CreateIndexRequest.SETTINGS.getPreferredName());
            {
                targetIndexRequest.settings().toXContent(builder, params);
            }
            builder.endObject();
            builder.startObject(CreateIndexRequest.ALIASES.getPreferredName());
            {
                for (Alias alias : targetIndexRequest.aliases()) {
                    alias.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

}
