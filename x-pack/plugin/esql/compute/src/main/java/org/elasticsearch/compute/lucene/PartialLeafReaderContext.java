/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.index.LeafReaderContext;

public final class PartialLeafReaderContext {

    final LeafReaderContext leafReaderContext;
    final int minDoc; // incl
    final int maxDoc; // excl

    public PartialLeafReaderContext(LeafReaderContext leafReaderContext, int minDoc, int maxDoc) {
        this.leafReaderContext = leafReaderContext;
        this.minDoc = minDoc;
        this.maxDoc = maxDoc;
    }

    public PartialLeafReaderContext(LeafReaderContext leafReaderContext) {
        this(leafReaderContext, 0, leafReaderContext.reader().maxDoc());
    }
}
