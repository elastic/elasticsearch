/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;

import java.util.function.Consumer;
import java.util.function.IntPredicate;

/**
 * The children of a page of parent rows, which is all a collection operator needs from a source. An index finds them as
 * hidden documents through the parent's {@code _doc}; a dataset carries them in the scan's own page. Neither knows which
 * operator is asking, so a new operator over collections is written once against this.
 */
public interface ChildRows extends Releasable {
    /**
     * Hands the children of the parents at the positions of {@code parents} that are {@code included} to {@code consumer},
     * in parent order, in pages whose block 0 is an {@code IntVector} of each child's parent position and whose remaining
     * blocks are the child fields the reader was built for. A parent's remaining children are skipped once
     * {@code keepReading} says its position needs no more; it is asked at least after each page. Each page is released once
     * {@code consumer} returns.
     */
    void read(Page parents, IntPredicate included, IntPredicate keepReading, Consumer<Page> consumer);

    /** Builds a reader per driver, since a reader may hold an operator. */
    interface Factory {
        ChildRows get(DriverContext context);
    }
}
