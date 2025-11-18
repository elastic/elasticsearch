/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MultiGetShardResponse extends ActionResponse {

    final List<Integer> locations;
    final List<GetResponse> responses;
    final List<MultiGetResponse.Failure> failures;

    MultiGetShardResponse() {
        locations = new ArrayList<>();
        responses = new ArrayList<>();
        failures = new ArrayList<>();
    }

    MultiGetShardResponse(StreamInput in) throws IOException {
        int size = in.readVInt();
        locations = new ArrayList<>(size);
        responses = new ArrayList<>(size);
        failures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            if (in.readBoolean()) {
                responses.add(new GetResponse(in));
            } else {
                responses.add(null);
            }
            if (in.readBoolean()) {
                failures.add(new MultiGetResponse.Failure(in));
            } else {
                failures.add(null);
            }
        }
    }

    public void add(int location, GetResponse response) {
        locations.add(location);
        responses.add(response);
        failures.add(null);
    }

    public void add(int location, MultiGetResponse.Failure failure) {
        locations.add(location);
        responses.add(null);
        failures.add(failure);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(locations.size());
        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
            if (responses.get(i) == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                responses.get(i).writeTo(out);
            }
            if (failures.get(i) == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                failures.get(i).writeTo(out);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof MultiGetShardResponse == false) return false;
        MultiGetShardResponse other = (MultiGetShardResponse) o;
        return Objects.equals(locations, other.locations) && Objects.equals(responses, other.responses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(locations, responses);
    }
}
