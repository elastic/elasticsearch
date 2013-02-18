/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.explain;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.single.shard.SingleShardOperationRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * Explain request encapsulating the explain query and document identifier to get an explanation for.
 */
public class ExplainRequest extends SingleShardOperationRequest<ExplainRequest> {

    private static final XContentType contentType = Requests.CONTENT_TYPE;

    private String type = "_all";
    private String id;
    private String routing;
    private String preference;
    private BytesReference source;
    private String[] fields;
    private boolean sourceUnsafe;

    private String[] filteringAlias = Strings.EMPTY_ARRAY;

    ExplainRequest() {
    }

    public ExplainRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public ExplainRequest setType(String type) {
        this.type = type;
        return this;
    }

    public String getId() {
        return id;
    }

    public ExplainRequest setId(String id) {
        this.id = id;
        return this;
    }

    public String getRouting() {
        return routing;
    }

    public ExplainRequest setRouting(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Simple sets the routing. Since the parent is only used to get to the right shard.
     */
    public ExplainRequest setParent(String parent) {
        this.routing = parent;
        return this;
    }

    public String getPreference() {
        return preference;
    }

    public ExplainRequest setPreference(String preference) {
        this.preference = preference;
        return this;
    }

    public BytesReference getSource() {
        return source;
    }

    public boolean isSourceUnsafe() {
        return sourceUnsafe;
    }

    public ExplainRequest setSource(ExplainSourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(contentType);
        this.sourceUnsafe = false;
        return this;
    }

    public ExplainRequest setSource(BytesReference source, boolean unsafe) {
        this.source = source;
        this.sourceUnsafe = unsafe;
        return this;
    }

    public String[] getFields() {
        return fields;
    }

    public ExplainRequest setFields(String[] fields) {
        this.fields = fields;
        return this;
    }

    public String[] getFilteringAlias() {
        return filteringAlias;
    }

    // TODO Should we change it to ExplainRequest setFilteringAlias(String[] filteringAlias)?
    public void setFilteringAlias(String[] filteringAlias) {
        if (filteringAlias == null) {
            return;
        }

        this.filteringAlias = filteringAlias;
    }

    @Override
    protected void beforeLocalFork() {
        if (sourceUnsafe) {
            source = source.copyBytesArray();
            sourceUnsafe = false;
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = ValidateActions.addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = ValidateActions.addValidationError("id is missing", validationException);
        }
        if (source == null) {
            validationException = ValidateActions.addValidationError("source is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        type = in.readString();
        id = in.readString();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        source = in.readBytesReference();
        sourceUnsafe = false;
        filteringAlias = in.readStringArray();
        if (in.readBoolean()) {
            fields = in.readStringArray();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        out.writeBytesReference(source);
        out.writeStringArray(filteringAlias);
        if (fields != null) {
            out.writeBoolean(true);
            out.writeStringArray(fields);
        } else {
            out.writeBoolean(false);
        }
    }
}
