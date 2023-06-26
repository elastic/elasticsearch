/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.SourceLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MultiGetShardRequest extends SingleShardRequest<MultiGetShardRequest> {

    private int shardId;
    private String preference;
    private boolean realtime;
    private boolean refresh;

    List<Integer> locations;
    List<MultiGetRequest.Item> items;

    /**
     * Should this request force {@link SourceLoader.Synthetic synthetic source}?
     * Use this to test if the mapping supports synthetic _source and to get a sense
     * of the worst case performance. Fetches with this enabled will be slower the
     * enabling synthetic source natively in the index.
     */
    private boolean forceSyntheticSource;

    MultiGetShardRequest(MultiGetRequest multiGetRequest, String index, int shardId) {
        super(index);
        this.shardId = shardId;
        locations = new ArrayList<>();
        items = new ArrayList<>();
        preference = multiGetRequest.preference;
        realtime = multiGetRequest.realtime;
        refresh = multiGetRequest.refresh;
        forceSyntheticSource = multiGetRequest.isForceSyntheticSource();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof MultiGetShardRequest == false) return false;
        MultiGetShardRequest other = (MultiGetShardRequest) o;
        return shardId == other.shardId
            && realtime == other.realtime
            && refresh == other.refresh
            && forceSyntheticSource == other.forceSyntheticSource
            && Objects.equals(preference, other.preference)
            && Objects.equals(index, other.index)
            && Objects.equals(locations, other.locations)
            && Objects.equals(items, other.items);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, preference, realtime, refresh, index, locations, items, forceSyntheticSource);
    }

    MultiGetShardRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        locations = new ArrayList<>(size);
        items = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            items.add(new MultiGetRequest.Item(in));
        }

        preference = in.readOptionalString();
        refresh = in.readBoolean();
        realtime = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
            forceSyntheticSource = in.readBoolean();
        } else {
            forceSyntheticSource = false;
        }
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
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
            out.writeBoolean(forceSyntheticSource);
        } else {
            if (forceSyntheticSource) {
                throw new IllegalArgumentException("force_synthetic_source is not supported before 8.4.0");
            }
        }
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

    /**
     * Should this request force {@link SourceLoader.Synthetic synthetic source}?
     * Use this to test if the mapping supports synthetic _source and to get a sense
     * of the worst case performance. Fetches with this enabled will be slower the
     * enabling synthetic source natively in the index.
     */
    public boolean isForceSyntheticSource() {
        return forceSyntheticSource;
    }

    public MultiGetShardRequest setForceSyntheticSource(boolean forceSyntheticSource) {
        this.forceSyntheticSource = forceSyntheticSource;
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
}
