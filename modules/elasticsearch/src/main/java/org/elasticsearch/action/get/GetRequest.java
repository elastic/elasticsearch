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
 * @author kimchy (Shay Banon)
 */
public class GetRequest extends SingleOperationRequest {

    GetRequest() {
    }

    public GetRequest(String index) {
        super(index, null, null);
    }

    public GetRequest(String index, String type, String id) {
        super(index, type, id);
    }

    @Required public GetRequest type(String type) {
        this.type = type;
        return this;
    }

    @Required public GetRequest id(String id) {
        this.id = id;
        return this;
    }

    @Override public GetRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

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
}
