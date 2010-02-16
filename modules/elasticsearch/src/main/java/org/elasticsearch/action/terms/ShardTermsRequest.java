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

package org.elasticsearch.action.terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class ShardTermsRequest extends BroadcastShardOperationRequest {

    private String[] fields;

    private String from;

    private String to;

    private boolean fromInclusive = true;

    private boolean toInclusive = false;

    private String prefix;

    private String regexp;

    private int size = 10;

    private boolean convert = true;

    private TermsRequest.SortType sortType;

    private boolean exact = false;

    ShardTermsRequest() {
    }

    public ShardTermsRequest(String index, int shardId, TermsRequest request) {
        super(index, shardId);
        this.fields = request.fields();
        this.from = request.from();
        this.to = request.to();
        this.fromInclusive = request.fromInclusive();
        this.toInclusive = request.toInclusive();
        this.prefix = request.prefix();
        this.regexp = request.regexp();
        this.size = request.size();
        this.convert = request.convert();
        this.sortType = request.sortType();
        this.exact = request.exact();
    }

    public String[] fields() {
        return fields;
    }

    public String from() {
        return from;
    }

    public String to() {
        return to;
    }

    public boolean fromInclusive() {
        return fromInclusive;
    }

    public boolean toInclusive() {
        return toInclusive;
    }

    public String prefix() {
        return prefix;
    }

    public String regexp() {
        return regexp;
    }

    public int size() {
        return size;
    }

    public boolean convert() {
        return convert;
    }

    public TermsRequest.SortType sortType() {
        return sortType;
    }

    public boolean exact() {
        return this.exact;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        fields = new String[in.readInt()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = in.readUTF();
        }
        if (in.readBoolean()) {
            from = in.readUTF();
        }
        if (in.readBoolean()) {
            to = in.readUTF();
        }
        fromInclusive = in.readBoolean();
        toInclusive = in.readBoolean();
        if (in.readBoolean()) {
            prefix = in.readUTF();
        }
        if (in.readBoolean()) {
            regexp = in.readUTF();
        }
        size = in.readInt();
        convert = in.readBoolean();
        sortType = TermsRequest.SortType.fromValue(in.readByte());
        exact = in.readBoolean();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(fields.length);
        for (String field : fields) {
            out.writeUTF(field);
        }
        if (from == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(from);
        }
        if (to == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(to);
        }
        out.writeBoolean(fromInclusive);
        out.writeBoolean(toInclusive);
        if (prefix == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(prefix);
        }
        if (regexp == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(regexp);
        }
        out.writeInt(size);
        out.writeBoolean(convert);
        out.writeByte(sortType.value());
        out.writeBoolean(exact);
    }
}
