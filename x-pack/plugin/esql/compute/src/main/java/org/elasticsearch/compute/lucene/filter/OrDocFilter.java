/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;

/**
 * Keeps the docs any child keeps. Each child sees only the docs the children before it rejected, so an expression after a
 * native leaf reads only what the leaf did not already decide.
 */
public final class OrDocFilter implements DocFilter {
    private final List<DocFilter> children;
    private FixedBitSet remaining;
    private FixedBitSet attempt;
    private FixedBitSet kept;

    public OrDocFilter(DocFilter... children) {
        this.children = List.of(children);
    }

    @Override
    public void filter(DocWindow window, FixedBitSet inPlay) throws IOException {
        if (remaining == null || remaining.length() != inPlay.length()) {
            remaining = new FixedBitSet(inPlay.length());
            attempt = new FixedBitSet(inPlay.length());
            kept = new FixedBitSet(inPlay.length());
        }
        remaining.clear();
        remaining.or(inPlay);
        kept.clear();
        for (DocFilter child : children) {
            if (remaining.scanIsEmpty()) {
                break;
            }
            attempt.clear();
            attempt.or(remaining);
            child.filter(window, attempt);
            kept.or(attempt);
            remaining.andNot(attempt);
        }
        inPlay.clear();
        inPlay.or(kept);
    }

    @Override
    public void close() {
        Releasables.close(children);
    }
}
