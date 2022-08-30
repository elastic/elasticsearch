/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A collection of {@link CollectedAggregator} instances representing all top level aggregations
 * in a given query.  This class is responsible for serialization and closing of the classes
 * it holds.
 */
// NOCOMMIT - Does this need to be a named writeable? or just a plain writable?  I think it's okay to just be plain...
public class CollectedAggregators implements Releasable, Writeable {
    protected List<CollectedAggregator> aggregations;

    public CollectedAggregators() {
        aggregations = new ArrayList<>();
    }

    public CollectedAggregators(List<CollectedAggregator> aggregations) {
        this.aggregations = aggregations;
    }

    public void readFrom(StreamInput in) throws IOException {
        aggregations = in.readNamedWriteableList(CollectedAggregator.class);
    }

    public void add(CollectedAggregator aggregator) {
        aggregations.add(aggregator);
    }

    @Override
    public void close() {
        Releasables.close(aggregations);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(aggregations);
    }
}
