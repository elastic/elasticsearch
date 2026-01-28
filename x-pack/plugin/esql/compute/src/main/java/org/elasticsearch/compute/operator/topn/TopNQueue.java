/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.core.Releasable;

import java.util.List;

interface TopNQueue extends Accountable, Releasable {
    /** The current number of rows in the queue. */
    int size();

    /**
     * Attempts to add the row to the queue, if it is not full or if it is full but the row is "greater" than the max row in the queue.
     * N.B. This method does not write into the row at all (neither key nor value); the caller is responsible for doing so before, and after
     * calling this method.
     * @return
     *  If the row was added and the queue was full, this is the row that was evicted from the queue. If the row was added, and it wasn't
     *  full, this is {@code null}. If the row wasn't added, this is the input row. (A 3-variant ADT would have been better design, but this
     *  uses the fewest allocations.)
     */
    Row addRow(Row row);

    /**
     * Removes and returns all rows in the queue. For an ascending order, the first element will be the min element in the queue (or
     * last in the priority queue), and vice versa.
     */
    List<Row> popAll();
}
