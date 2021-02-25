/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Since {@link org.elasticsearch.search.internal.SearchContext} no longer hold the states of search, the top K results
 * (i.e., documents that will be rescored by query rescorers) need to be serialized/ deserialized between search phases.
 * A {@link RescoreDocIds} encapsulates the top K results for each rescorer by its ordinal index.
 */
public final class RescoreDocIds implements Writeable {
    public static final RescoreDocIds EMPTY = new RescoreDocIds(Map.of());

    private final Map<Integer, Set<Integer>> docIds;

    public RescoreDocIds(Map<Integer, Set<Integer>> docIds) {
        this.docIds = docIds;
    }

    public RescoreDocIds(StreamInput in) throws IOException {
        docIds = in.readMap(StreamInput::readVInt, i -> i.readSet(StreamInput::readVInt));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(docIds, StreamOutput::writeVInt, (o, v) -> o.writeCollection(v, StreamOutput::writeVInt));
    }

    public Set<Integer> getId(int index) {
        return docIds.get(index);
    }
}
