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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.util.TimeValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.Actions.*;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class ShardReplicationOperationRequest implements ActionRequest {

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    protected TimeValue timeout = DEFAULT_TIMEOUT;

    protected String index;

    private boolean threadedListener = false;
    private boolean threadedOperation = false;

    public TimeValue timeout() {
        return timeout;
    }

    public String index() {
        return this.index;
    }

    @Override public boolean listenerThreaded() {
        return threadedListener;
    }

    @Override public ShardReplicationOperationRequest listenerThreaded(boolean threadedListener) {
        this.threadedListener = threadedListener;
        return this;
    }


    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    public boolean operationThreaded() {
        return threadedOperation;
    }

    /**
     * Controls if the operation will be executed on a separate thread when executed locally.
     */
    public ShardReplicationOperationRequest operationThreaded(boolean threadedOperation) {
        this.threadedOperation = threadedOperation;
        return this;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        timeout = TimeValue.readTimeValue(in);
        index = in.readUTF();
        // no need to serialize threaded* parameters, since they only matter locally
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        timeout.writeTo(out);
        out.writeUTF(index);
    }
}
