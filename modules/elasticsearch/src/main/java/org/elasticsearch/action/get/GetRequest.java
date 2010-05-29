/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.action.support.single.SingleOperationRequest;
import org.elasticsearch.util.Required;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get a document (its source) from an index based on its type and id. Best created using
 * {@link org.elasticsearch.client.Requests#getRequest(String)}.
 *
 * <p>The operation requires the {@link #index()}, {@link #type(String)} and {@link #id(String)}
 * to be set.
 *
 * @author kimchy (shay.banon)
 * @see org.elasticsearch.action.get.GetResponse
 * @see org.elasticsearch.client.Requests#getRequest(String)
 * @see org.elasticsearch.client.Client#get(GetRequest)
 */
public class GetRequest extends SingleOperationRequest {

    private String[] fields;

    GetRequest() {
    }

    /**
     * Constructs a new get request against the specified index. The {@link #type(String)} and {@link #id(String)}
     * must be set.
     */
    public GetRequest(String index) {
        super(index, null, null);
    }

    /**
     * Constructs a new get request against the specified index with the type and id.
     *
     * @param index The index to get the document from
     * @param type  The type of the document
     * @param id    The id of the document
     */
    public GetRequest(String index, String type, String id) {
        super(index, type, id);
    }

    /**
     * Sets the index of the document to fetch.
     */
    @Required public GetRequest index(String index) {
        this.index = index;
        return this;
    }

    /**
     * Sets the type of the document to fetch.
     */
    @Required public GetRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * Sets the id of the document to fetch.
     */
    @Required public GetRequest id(String id) {
        this.id = id;
        return this;
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
     * Should the listener be called on a separate thread if needed.
     */
    @Override public GetRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    @Override public GetRequest operationThreaded(boolean threadedOperation) {
        super.operationThreaded(threadedOperation);
        return this;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readInt();
        if (size >= 0) {
            fields = new String[size];
            for (int i = 0; i < size; i++) {
                fields[i] = in.readUTF();
            }
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (fields == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(fields.length);
            for (String field : fields) {
                out.writeUTF(field);
            }
        }
    }

    @Override public String toString() {
        return "[" + index + "][" + type + "][" + id + "]";
    }
}
