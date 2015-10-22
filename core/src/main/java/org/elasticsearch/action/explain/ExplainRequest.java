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

package org.elasticsearch.action.explain;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.io.IOException;

/**
 * Explain request encapsulating the explain query and document identifier to get an explanation for.
 */
public class ExplainRequest extends SingleShardRequest<ExplainRequest> {

    private String type = "_all";
    private String id;
    private String routing;
    private String preference;
    private BytesReference source;
    private String[] fields;
    private FetchSourceContext fetchSourceContext;

    private String[] filteringAlias = Strings.EMPTY_ARRAY;

    long nowInMillis;

    public ExplainRequest() {
    }

    public ExplainRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    public String type() {
        return type;
    }

    public ExplainRequest type(String type) {
        this.type = type;
        return this;
    }

    public String id() {
        return id;
    }

    public ExplainRequest id(String id) {
        this.id = id;
        return this;
    }

    public String routing() {
        return routing;
    }

    public ExplainRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Simple sets the routing. Since the parent is only used to get to the right shard.
     */
    public ExplainRequest parent(String parent) {
        this.routing = parent;
        return this;
    }

    public String preference() {
        return preference;
    }

    public ExplainRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public BytesReference source() {
        return source;
    }

    public ExplainRequest source(QuerySourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        return this;
    }

    public ExplainRequest source(BytesReference source) {
        this.source = source;
        return this;
    }

    /**
     * Allows setting the {@link FetchSourceContext} for this request, controlling if and how _source should be returned.
     */
    public ExplainRequest fetchSourceContext(FetchSourceContext context) {
        this.fetchSourceContext = context;
        return this;
    }

    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }


    public String[] fields() {
        return fields;
    }

    public ExplainRequest fields(String[] fields) {
        this.fields = fields;
        return this;
    }

    public String[] filteringAlias() {
        return filteringAlias;
    }

    public ExplainRequest filteringAlias(String[] filteringAlias) {
        if (filteringAlias != null) {
            this.filteringAlias = filteringAlias;
        }

        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validateNonNullIndex();
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
        filteringAlias = in.readStringArray();
        if (in.readBoolean()) {
            fields = in.readStringArray();
        }

        fetchSourceContext = FetchSourceContext.optionalReadFromStream(in);
        nowInMillis = in.readVLong();
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

        FetchSourceContext.optionalWriteToStream(fetchSourceContext, out);
        out.writeVLong(nowInMillis);
    }
}
