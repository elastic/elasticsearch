/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Experimental;

@Experimental
public interface AggregatorStateSerializer<T extends AggregatorState<T>> {

    int size();

    // returns the number of bytes written
    int serialize(T state, byte[] ba, int offset);

    void deserialize(T state, byte[] ba, int offset);

}
