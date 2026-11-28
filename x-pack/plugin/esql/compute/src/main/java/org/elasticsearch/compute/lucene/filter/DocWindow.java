/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.filter;

import org.apache.lucene.index.LeafReaderContext;

/** The docs {@code [from, to)} of one segment. Bit {@code i} of a bit set over this window stands for doc {@code from + i}. */
public record DocWindow(LeafReaderContext leaf, int from, int to) {
    public int length() {
        return to - from;
    }
}
