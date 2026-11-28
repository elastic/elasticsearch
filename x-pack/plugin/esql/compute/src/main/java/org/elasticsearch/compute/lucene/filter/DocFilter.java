/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.core.Releasable;

import java.io.IOException;

/**
 * A node of the one-pass filter on a shard. A node is given a window of doc ids and the docs still in play, and rejects
 * some of them. It keeps only the docs for which its predicate is true: a doc whose predicate is false or null is
 * rejected, so a node cannot be negated.
 * <p>
 * Windows of one segment arrive in ascending order. A node reads and evaluates only the docs still in play, so what an
 * earlier node rejected costs the later ones nothing. A tree keeps per-segment state, so one driver uses it and no two
 * threads share it.
 */
public interface DocFilter extends Releasable {
    /**
     * Clears the bit of every doc in {@code window} that this node rejects. Bit {@code i} of {@code inPlay} is doc
     * {@code window.from() + i}, and a bit at or past {@code window.length()} is clear on entry and stays clear.
     */
    void filter(DocWindow window, FixedBitSet inPlay) throws IOException;

    @Override
    default void close() {}
}
