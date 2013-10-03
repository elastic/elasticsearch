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

package org.elasticsearch.action.get;

import com.carrotsearch.hppc.IntArrayList;
import org.elasticsearch.action.support.single.shard.SingleShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiGetShardRequest extends SingleShardOperationRequest<MultiGetShardRequest> {

    private int shardId;
    private String preference;
    Boolean realtime;
    boolean refresh;

    IntArrayList locations;
    List<String> types;
    List<String> ids;
    List<String[]> fields;

    MultiGetShardRequest() {

    }

    MultiGetShardRequest(String index, int shardId) {
        super(index);
        this.shardId = shardId;
        locations = new IntArrayList();
        types = new ArrayList<String>();
        ids = new ArrayList<String>();
        fields = new ArrayList<String[]>();
    }

    public int shardId() {
        return this.shardId;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public MultiGetShardRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    public boolean realtime() {
        return this.realtime == null ? true : this.realtime;
    }

    public MultiGetShardRequest realtime(Boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public MultiGetShardRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public void add(int location, @Nullable String type, String id, String[] fields) {
        this.locations.add(location);
        this.types.add(type);
        this.ids.add(id);
        this.fields.add(fields);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        locations = new IntArrayList(size);
        types = new ArrayList<String>(size);
        ids = new ArrayList<String>(size);
        fields = new ArrayList<String[]>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            if (in.readBoolean()) {
                types.add(in.readString());
            } else {
                types.add(null);
            }
            ids.add(in.readString());
            int size1 = in.readVInt();
            if (size1 > 0) {
                String[] fields = new String[size1];
                for (int j = 0; j < size1; j++) {
                    fields[j] = in.readString();
                }
                this.fields.add(fields);
            } else {
                fields.add(null);
            }
        }

        preference = in.readOptionalString();
        refresh = in.readBoolean();
        byte realtime = in.readByte();
        if (realtime == 0) {
            this.realtime = false;
        } else if (realtime == 1) {
            this.realtime = true;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(types.size());
        for (int i = 0; i < types.size(); i++) {
            out.writeVInt(locations.get(i));
            if (types.get(i) == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(types.get(i));
            }
            out.writeString(ids.get(i));
            if (fields.get(i) == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(fields.get(i).length);
                for (String field : fields.get(i)) {
                    out.writeString(field);
                }
            }
        }

        out.writeOptionalString(preference);
        out.writeBoolean(refresh);
        if (realtime == null) {
            out.writeByte((byte) -1);
        } else if (realtime == false) {
            out.writeByte((byte) 0);
        } else {
            out.writeByte((byte) 1);
        }
    }
}
