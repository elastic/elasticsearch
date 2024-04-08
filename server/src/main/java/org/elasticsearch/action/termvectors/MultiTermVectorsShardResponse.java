/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiTermVectorsShardResponse extends ActionResponse {

    final List<Integer> locations;
    final List<TermVectorsResponse> responses;
    final List<MultiTermVectorsResponse.Failure> failures;

    MultiTermVectorsShardResponse() {
        locations = new ArrayList<>();
        responses = new ArrayList<>();
        failures = new ArrayList<>();
    }

    MultiTermVectorsShardResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        locations = new ArrayList<>(size);
        responses = new ArrayList<>(size);
        failures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            if (in.readBoolean()) {
                responses.add(new TermVectorsResponse(in));
            } else {
                responses.add(null);
            }
            if (in.readBoolean()) {
                failures.add(new MultiTermVectorsResponse.Failure(in));
            } else {
                failures.add(null);
            }
        }
    }

    public void add(int location, TermVectorsResponse response) {
        locations.add(location);
        responses.add(response);
        failures.add(null);
    }

    public void add(int location, MultiTermVectorsResponse.Failure failure) {
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
}
