/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import com.carrotsearch.hppc.LongArrayList;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.single.shard.SingleShardOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.source.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiGetShardRequest extends SingleShardOperationRequest<MultiGetShardRequest> {

    private int shardId;
    private String preference;
    Boolean realtime;
    boolean refresh;
    boolean ignoreErrorsOnGeneratedFields = false;

    IntArrayList locations;
    List<MultiGetRequest.Item> items;

    MultiGetShardRequest() {

    }

    MultiGetShardRequest(MultiGetRequest multiGetRequest, String index, int shardId) {
        super(multiGetRequest, index);
        this.shardId = shardId;
        locations = new IntArrayList();
        items = new ArrayList<>();
        preference = multiGetRequest.preference;
        realtime = multiGetRequest.realtime;
        refresh = multiGetRequest.refresh;
        ignoreErrorsOnGeneratedFields = multiGetRequest.ignoreErrorsOnGeneratedFields;
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

    public MultiGetShardRequest ignoreErrorsOnGeneratedFields(Boolean ignoreErrorsOnGeneratedFields) {
        this.ignoreErrorsOnGeneratedFields = ignoreErrorsOnGeneratedFields;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public MultiGetShardRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    void add(int location, MultiGetRequest.Item item) {
        this.locations.add(location);
        this.items.add(item);
    }

    @Override
    public String[] indices() {
        String[] indices = new String[items.size()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = items.get(i).index();
        }
        return indices;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        locations = new IntArrayList(size);
        items = new ArrayList<>(size);

        if (in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            for (int i = 0; i < size; i++) {
                locations.add(in.readVInt());
                items.add(MultiGetRequest.Item.readItem(in));
            }
        } else {
            List<String> types = new ArrayList<>(size);
            List<String> ids = new ArrayList<>(size);
            List<String[]> fields = new ArrayList<>(size);
            LongArrayList versions = new LongArrayList(size);
            List<VersionType> versionTypes = new ArrayList<>(size);
            List<FetchSourceContext> fetchSourceContexts = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                locations.add(in.readVInt());
                if (in.readBoolean()) {
                    types.add(in.readSharedString());
                } else {
                    types.add(null);
                }
                ids.add(in.readString());
                int size1 = in.readVInt();
                if (size1 > 0) {
                    String[] fieldsArray = new String[size1];
                    for (int j = 0; j < size1; j++) {
                        fieldsArray[j] = in.readString();
                    }
                    fields.add(fieldsArray);
                } else {
                    fields.add(null);
                }
                versions.add(Versions.readVersionWithVLongForBW(in));
                versionTypes.add(VersionType.fromValue(in.readByte()));

                fetchSourceContexts.add(FetchSourceContext.optionalReadFromStream(in));
            }

            for (int i = 0; i < size; i++) {
                //before 1.4 we have only one index, the concrete one
                MultiGetRequest.Item item = new MultiGetRequest.Item(index, types.get(i), ids.get(i))
                        .fields(fields.get(i)).version(versions.get(i)).versionType(versionTypes.get(i))
                        .fetchSourceContext(fetchSourceContexts.get(i));
                items.add(item);
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
        if(in.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            ignoreErrorsOnGeneratedFields = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(locations.size());

        if (out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            for (int i = 0; i < locations.size(); i++) {
                out.writeVInt(locations.get(i));
                items.get(i).writeTo(out);
            }
        } else {
            for (int i = 0; i < locations.size(); i++) {
                out.writeVInt(locations.get(i));
                MultiGetRequest.Item item = items.get(i);
                if (item.type() == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    out.writeSharedString(item.type());
                }
                out.writeString(item.id());
                if (item.fields() == null) {
                    out.writeVInt(0);
                } else {
                    out.writeVInt(item.fields().length);
                    for (String field : item.fields()) {
                        out.writeString(field);
                    }
                }
                Versions.writeVersionWithVLongForBW(item.version(), out);
                out.writeByte(item.versionType().getValue());
                FetchSourceContext.optionalWriteToStream(item.fetchSourceContext(), out);
            }
        }

        out.writeOptionalString(preference);
        out.writeBoolean(refresh);
        if (realtime == null) {
            out.writeByte((byte) -1);
        } else if (!realtime) {
            out.writeByte((byte) 0);
        } else {
            out.writeByte((byte) 1);
        }
        if(out.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            out.writeBoolean(ignoreErrorsOnGeneratedFields);
        }

    }

    public boolean ignoreErrorsOnGeneratedFields() {
        return ignoreErrorsOnGeneratedFields;
    }
}
