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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class BulkItemResponse implements Streamable {

    public static class Failure {
        private final String index;
        private final String type;
        private final String id;
        private final String message;

        public Failure(String index, String type, String id, String message) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.message = message;
        }

        public String index() {
            return this.index;
        }

        public String getIndex() {
            return index();
        }

        public String message() {
            return this.message;
        }

        public String getMessage() {
            return message();
        }

        public String type() {
            return type;
        }

        public String getType() {
            return type();
        }

        public String id() {
            return id;
        }

        public String getId() {
            return this.id;
        }
    }

    private int id;

    private String opType;

    private ActionResponse response;

    private Failure failure;

    BulkItemResponse() {

    }

    public BulkItemResponse(int id, String opType, ActionResponse response) {
        this.id = id;
        this.opType = opType;
        this.response = response;
    }

    public BulkItemResponse(int id, String opType, Failure failure) {
        this.id = id;
        this.opType = opType;
        this.failure = failure;
    }

    public int itemId() {
        return id;
    }

    public String opType() {
        return this.opType;
    }

    public String index() {
        if (failure != null) {
            return failure.index();
        }
        if (response instanceof IndexResponse) {
            return ((IndexResponse) response).index();
        } else if (response instanceof DeleteResponse) {
            return ((DeleteResponse) response).index();
        }
        return null;
    }

    public String getIndex() {
        return index();
    }

    public String type() {
        if (failure != null) {
            return failure.type();
        }
        if (response instanceof IndexResponse) {
            return ((IndexResponse) response).type();
        } else if (response instanceof DeleteResponse) {
            return ((DeleteResponse) response).type();
        }
        return null;
    }

    public String getType() {
        return this.type();
    }

    public String id() {
        if (failure != null) {
            return failure.id();
        }
        if (response instanceof IndexResponse) {
            return ((IndexResponse) response).id();
        } else if (response instanceof DeleteResponse) {
            return ((DeleteResponse) response).id();
        }
        return null;
    }

    public String getId() {
        return id();
    }

    public ActionResponse response() {
        return response;
    }

    public boolean failed() {
        return failure != null;
    }

    public boolean isFailed() {
        return failed();
    }

    public Failure failure() {
        return this.failure;
    }

    public Failure getFailure() {
        return failure();
    }

    public static BulkItemResponse readBulkItem(StreamInput in) throws IOException {
        BulkItemResponse response = new BulkItemResponse();
        response.readFrom(in);
        return response;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        opType = in.readUTF();

        byte type = in.readByte();
        if (type == 0) {
            response = new IndexResponse();
            response.readFrom(in);
        } else if (type == 1) {
            response = new DeleteResponse();
            response.readFrom(in);
        }

        if (in.readBoolean()) {
            failure = new Failure(in.readUTF(), in.readUTF(), in.readUTF(), in.readUTF());
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        out.writeUTF(opType);
        if (response == null) {
            out.writeByte((byte) 2);
        } else {
            if (response instanceof IndexResponse) {
                out.writeByte((byte) 0);
            } else if (response instanceof DeleteResponse) {
                out.writeByte((byte) 1);
            }
            response.writeTo(out);
        }
        if (failure == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(failure.index());
            out.writeUTF(failure.type());
            out.writeUTF(failure.id());
            out.writeUTF(failure.message());
        }
    }
}
