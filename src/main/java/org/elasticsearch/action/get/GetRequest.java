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

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.single.shard.SingleShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get a document (its source) from an index based on its type (optional) and id. Best created using
 * {@link org.elasticsearch.client.Requests#getRequest(String)}.
 * <p/>
 * <p>The operation requires the {@link #index()}, {@link #type(String)} and {@link #id(String)}
 * to be set.
 *
 * @see org.elasticsearch.action.get.GetResponse
 * @see org.elasticsearch.client.Requests#getRequest(String)
 * @see org.elasticsearch.client.Client#get(GetRequest)
 */
public class GetRequest extends SingleShardOperationRequest<GetRequest> {

    protected String type;
    protected String id;
    protected String routing;
    protected String preference;

    private String[] fields;

    private boolean refresh = false;

    Boolean realtime;

    GetRequest() {
        type = "_all";
    }

    /**
     * Constructs a new get request against the specified index. The {@link #type(String)} and {@link #id(String)}
     * must be set.
     */
    public GetRequest(String index) {
        super(index);
        this.type = "_all";
    }

    /**
     * Constructs a new get request against the specified index with the type and id.
     *
     * @param index The index to get the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    public GetRequest(String index, String type, String id) {
        super(index);
        this.type = type;
        this.id = id;
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
        return validationException;
    }

    /**
     * Sets the type of the document to fetch.
     */
    public GetRequest type(@Nullable String type) {
        if (type == null) {
            type = "_all";
        }
        this.type = type;
        return this;
    }

    /**
     * Sets the id of the document to fetch.
     */
    public GetRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Sets the parent id of this document. Will simply set the routing to this value, as it is only
     * used for routing with delete requests.
     */
    public GetRequest parent(String parent) {
        if (routing == null) {
            routing = parent;
        }
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public GetRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public GetRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    public String routing() {
        return this.routing;
    }

    public String preference() {
        return this.preference;
    }

    /**
     * Explicitly specify the fields that will be returned. By default, the <tt>_source</tt>
     * field will be returned.
     */
    public GetRequest fields(String... fields) {
        this.fields = fields;
        return this;
    }

    /**
     * Explicitly specify the fields that will be returned. By default, the <tt>_source</tt>
     * field will be returned.
     */
    public String[] fields() {
        return this.fields;
    }

    /**
     * Should a refresh be executed before this get operation causing the operation to
     * return the latest value. Note, heavy get should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public GetRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public boolean realtime() {
        return this.realtime == null ? true : this.realtime;
    }

    public GetRequest realtime(Boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        type = in.readString();
        id = in.readString();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        refresh = in.readBoolean();
        int size = in.readInt();
        if (size >= 0) {
            fields = new String[size];
            for (int i = 0; i < size; i++) {
                fields[i] = in.readString();
            }
        }
        byte realtime = in.readByte();
        if (realtime == 0) {
            this.realtime = false;
        } else if (realtime == 1) {
            this.realtime = true;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(type);
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);

        out.writeBoolean(refresh);
        if (fields == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(fields.length);
            for (String field : fields) {
                out.writeString(field);
            }
        }
        if (realtime == null) {
            out.writeByte((byte) -1);
        } else if (realtime == false) {
            out.writeByte((byte) 0);
        } else {
            out.writeByte((byte) 1);
        }
    }

    @Override
    public String toString() {
        return "[" + index + "][" + type + "][" + id + "]: routing [" + routing + "]";
    }
}
