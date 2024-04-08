/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

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

    List<Integer> locations;
    List<TermVectorsRequest> requests;

    MultiTermVectorsShardRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        locations = new ArrayList<>(size);
        requests = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            requests.add(new TermVectorsRequest(in));
        }

        preference = in.readOptionalString();
    }

    MultiTermVectorsShardRequest(String index, int shardId) {
        super(index);
        this.shardId = shardId;
        locations = new ArrayList<>();
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
     * {@code _local} to prefer local shards or a custom value, which guarantees that the same order
     * will be used across different requests.
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
