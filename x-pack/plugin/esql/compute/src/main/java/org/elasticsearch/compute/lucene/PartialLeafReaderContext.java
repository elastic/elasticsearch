/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;

/**
 * A subset of a {@link LeafReaderContext}.
 * @param leafReaderContext the context to subset
 * @param minDoc the first document
 * @param maxDoc one more than the last document
 */
public record PartialLeafReaderContext(LeafReaderContext leafReaderContext, int minDoc, int maxDoc) {
    public PartialLeafReaderContext(LeafReaderContext leafReaderContext) {
        this(leafReaderContext, 0, leafReaderContext.reader().maxDoc());
    }
}
