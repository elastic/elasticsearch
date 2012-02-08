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

package org.elasticsearch.index.translog;

import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public class TranslogStreams {

    public static Translog.Operation readTranslogOperation(StreamInput in) throws IOException {
        Translog.Operation.Type type = Translog.Operation.Type.fromId(in.readByte());
        Translog.Operation operation;
        switch (type) {
            case CREATE:
                operation = new Translog.Create();
                break;
            case DELETE:
                operation = new Translog.Delete();
                break;
            case DELETE_BY_QUERY:
                operation = new Translog.DeleteByQuery();
                break;
            case SAVE:
                operation = new Translog.Index();
                break;
            default:
                throw new IOException("No type for [" + type + "]");
        }
        operation.readFrom(in);
        return operation;
    }

    public static Translog.Source readSource(byte[] data) throws IOException {
        BytesStreamInput in = new BytesStreamInput(data, false);
        in.readInt(); // the size header
        Translog.Operation.Type type = Translog.Operation.Type.fromId(in.readByte());
        Translog.Operation operation;
        switch (type) {
            case CREATE:
                operation = new Translog.Create();
                break;
            case DELETE:
                operation = new Translog.Delete();
                break;
            case DELETE_BY_QUERY:
                operation = new Translog.DeleteByQuery();
                break;
            case SAVE:
                operation = new Translog.Index();
                break;
            default:
                throw new IOException("No type for [" + type + "]");
        }
        return operation.readSource(in);
    }

    public static void writeTranslogOperation(StreamOutput out, Translog.Operation op) throws IOException {
        out.writeByte(op.opType().id());
        op.writeTo(out);
    }
}
