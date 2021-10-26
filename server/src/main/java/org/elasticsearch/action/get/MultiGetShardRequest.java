/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import com.carrotsearch.hppc.IntArrayList;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiGetShardRequest extends SingleShardRequest<MultiGetShardRequest> {

    private int shardId;
    private String preference;
    private boolean realtime;
    private boolean refresh;

    IntArrayList locations;
    List<MultiGetRequest.Item> items;

    MultiGetShardRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        locations = new IntArrayList(size);
        items = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            items.add(new MultiGetRequest.Item(in));
        }

        preference = in.readOptionalString();
        refresh = in.readBoolean();
        realtime = in.readBoolean();
    }

    MultiGetShardRequest(MultiGetRequest multiGetRequest, String index, int shardId) {
        super(index);
        this.shardId = shardId;
        locations = new IntArrayList();
        items = new ArrayList<>();
        preference = multiGetRequest.preference;
        realtime = multiGetRequest.realtime;
        refresh = multiGetRequest.refresh;
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
    public MultiGetShardRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    public boolean realtime() {
        return this.realtime;
    }

    public MultiGetShardRequest realtime(boolean realtime) {
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
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(locations.size());

        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
            items.get(i).writeTo(out);
        }

        out.writeOptionalString(preference);
        out.writeBoolean(refresh);
        out.writeBoolean(realtime);
    }
}
