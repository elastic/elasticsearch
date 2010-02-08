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

package org.elasticsearch.action.index;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.util.Required;
import org.elasticsearch.util.TimeValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.action.Actions.*;

/**
 * @author kimchy (Shay Banon)
 */
public class IndexRequest extends ShardReplicationOperationRequest {

    public static enum OpType {
        /**
         * Index the source. If there an existing document with the id, it will
         * be replaced.
         */
        INDEX((byte) 0),
        /**
         * Creates the resource. Simply adds it to the index, if there is an existing
         * document with the id, then it won't be removed.
         */
        CREATE((byte) 1);

        private byte id;

        OpType(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static OpType fromId(byte id) {
            if (id == 0) {
                return INDEX;
            } else if (id == 1) {
                return CREATE;
            } else {
                throw new ElasticSearchIllegalArgumentException("No type match for [" + id + "]");
            }
        }
    }

    private String type;
    private String id;
    private String source;
    private OpType opType = OpType.INDEX;

    public IndexRequest(String index) {
        this.index = index;
    }

    public IndexRequest(String index, String type, String id, String source) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = source;
    }

    IndexRequest() {
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        return validationException;
    }

    @Override public IndexRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    @Override public IndexRequest operationThreaded(boolean threadedOperation) {
        super.operationThreaded(threadedOperation);
        return this;
    }

    String type() {
        return type;
    }

    @Required public IndexRequest type(String type) {
        this.type = type;
        return this;
    }

    String id() {
        return id;
    }

    @Required public IndexRequest id(String id) {
        this.id = id;
        return this;
    }

    String source() {
        return source;
    }

    public IndexRequest source(String source) {
        this.source = source;
        return this;
    }

    public IndexRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public IndexRequest opType(OpType opType) {
        this.opType = opType;
        return this;
    }

    public OpType opType() {
        return this.opType;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        type = in.readUTF();
        id = in.readUTF();
        source = in.readUTF();
        opType = OpType.fromId(in.readByte());
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeUTF(type);
        out.writeUTF(id);
        out.writeUTF(source);
        out.writeByte(opType.id());
    }
}