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

package org.elasticsearch.action.support.single;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.Actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class SingleOperationRequest implements ActionRequest {

    protected String index;
    protected String type;
    protected String id;

    private boolean threadedListener = false;
    private boolean threadedOperation = false;

    protected SingleOperationRequest() {
    }

    public SingleOperationRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = Actions.addValidationError("index is missing", validationException);
        }
        if (type == null) {
            validationException = Actions.addValidationError("type is missing", validationException);
        }
        if (id == null) {
            validationException = Actions.addValidationError("id is missing", validationException);
        }
        return validationException;
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    @Override public boolean listenerThreaded() {
        return threadedListener;
    }

    @Override public SingleOperationRequest listenerThreaded(boolean threadedListener) {
        this.threadedListener = threadedListener;
        return this;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    public boolean threadedOperation() {
        return threadedOperation;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    public SingleOperationRequest threadedOperation(boolean threadedOperation) {
        this.threadedOperation = threadedOperation;
        return this;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        index = in.readUTF();
        type = in.readUTF();
        id = in.readUTF();
        // no need to pass threading over the network, they are always false when coming throw a thread pool
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(index);
        out.writeUTF(type);
        out.writeUTF(id);
    }

}

