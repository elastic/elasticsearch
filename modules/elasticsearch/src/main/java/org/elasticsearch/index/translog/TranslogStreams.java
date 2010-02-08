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

package org.elasticsearch.index.translog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class TranslogStreams {

    public static Translog.Operation readTranslogOperation(DataInput in) throws IOException, ClassNotFoundException {
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

    public static void writeTranslogOperation(DataOutput out, Translog.Operation op) throws IOException {
        out.writeByte(op.opType().id());
        op.writeTo(out);
    }
}
