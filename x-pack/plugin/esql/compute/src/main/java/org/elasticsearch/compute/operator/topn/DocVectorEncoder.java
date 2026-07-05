/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.core.RefCounted;

public class DocVectorEncoder extends DefaultUnsortableTopNEncoder {
    private final IndexedByShardId<? extends RefCounted> refCounteds;

    public DocVectorEncoder(IndexedByShardId<? extends RefCounted> refCounteds) {
        this.refCounteds = refCounteds;
    }

    public IndexedByShardId<? extends RefCounted> refCounteds() {
        return refCounteds;
    }

    @Override
    public String toString() {
        return "Doc";
    }
}
