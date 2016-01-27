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

package org.elasticsearch.action.termvectors;

import com.carrotsearch.hppc.IntArrayList;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiTermVectorsShardRequest extends SingleShardRequest<MultiTermVectorsShardRequest> {

    private int shardId;
    private String preference;

    IntArrayList locations;
    List<TermVectorsRequest> requests;

    public MultiTermVectorsShardRequest() {

    }

    MultiTermVectorsShardRequest(String index, int shardId) {
        super(index);
        this.shardId = shardId;
        locations = new IntArrayList();
        requests = new ArrayList<>();
    }

    @Override
    public ActionRequestValidationException validate() {
        return super.validateNonNullIndex();
    }

    public int shardId() {
        return this.shardId;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public MultiTermVectorsShardRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }


    public void add(int location, TermVectorsRequest request) {
        this.locations.add(location);
        this.requests.add(request);
    }

    @Override
    public String[] indices() {
        String[] indices = new String[requests.size()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = requests.get(i).index();
        }
        return indices;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        locations = new IntArrayList(size);
        requests = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            requests.add(TermVectorsRequest.readTermVectorsRequest(in));
        }

        preference = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(locations.size());
        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
            requests.get(i).writeTo(out);
        }

        out.writeOptionalString(preference);
    }
}
