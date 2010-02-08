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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.elasticsearch.action.Actions.*;

/**
 * @author kimchy (Shay Banon)
 */
public class ShardDeleteByQueryRequest extends ShardReplicationOperationRequest {

    private int shardId;
    private String querySource;
    private String queryParserName;
    private String[] types = Strings.EMPTY_ARRAY;

    public ShardDeleteByQueryRequest(String index, String querySource, @Nullable String queryParserName, String[] types, int shardId) {
        this.index = index;
        this.querySource = querySource;
        this.queryParserName = queryParserName;
        this.types = types;
        this.shardId = shardId;
    }

    ShardDeleteByQueryRequest(IndexDeleteByQueryRequest request, int shardId) {
        this(request.index(), request.querySource(), request.queryParserName(), request.types(), shardId);
        timeout = request.timeout();
    }

    ShardDeleteByQueryRequest() {
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (querySource == null) {
            addValidationError("querySource is missing", validationException);
        }
        return validationException;
    }

    public int shardId() {
        return this.shardId;
    }

    public String querySource() {
        return querySource;
    }

    public String queryParserName() {
        return queryParserName;
    }

    public String[] types() {
        return this.types;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        querySource = in.readUTF();
        if (in.readBoolean()) {
            queryParserName = in.readUTF();
        }
        shardId = in.readInt();
        int typesSize = in.readInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeUTF(querySource);
        if (queryParserName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(queryParserName);
        }
        out.writeInt(shardId);
        out.writeInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
    }
}