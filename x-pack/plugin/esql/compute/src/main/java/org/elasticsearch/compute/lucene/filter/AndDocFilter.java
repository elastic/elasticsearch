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
 * Keeps the docs every child keeps. The children run in the order given, each on what the one before kept, and the
 * pass stops as soon as nothing is left in play. Put the cheap native leaves first: an expression behind them reads
 * only their survivors.
 */
public final class AndDocFilter implements DocFilter {
    private final List<DocFilter> children;

    public AndDocFilter(DocFilter... children) {
        this.children = List.of(children);
    }

    @Override
    public void filter(DocWindow window, FixedBitSet inPlay) throws IOException {
        for (DocFilter child : children) {
            if (inPlay.scanIsEmpty()) {
                return;
            }
            child.filter(window, inPlay);
        }
    }

    @Override
    public void close() {
        Releasables.close(children);
    }
}
