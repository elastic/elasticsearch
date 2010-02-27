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

import java.io.DataInput;
import java.io.DataOutput;
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
     * Should the listener be called on a separate thread if needed.
     */
    @Override public GetRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    @Override public GetRequest threadedOperation(boolean threadedOperation) {
        super.threadedOperation(threadedOperation);
        return this;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override public String toString() {
        return "[" + index + "][" + type + "][" + id + "]";
    }
}
