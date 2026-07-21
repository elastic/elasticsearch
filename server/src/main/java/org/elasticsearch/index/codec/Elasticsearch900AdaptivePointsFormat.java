/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * A {@link PointsFormat} that adaptively increases {@code maxPointsInLeafNode} for large fields
 * to bound the heap memory used by BKD tree metadata structures during indexing. The on-disk
 * format is fully compatible with {@link org.apache.lucene.codecs.lucene90.Lucene90PointsFormat}.
 */
class Elasticsearch900AdaptivePointsFormat extends PointsFormat {

    static final Elasticsearch900AdaptivePointsFormat INSTANCE = new Elasticsearch900AdaptivePointsFormat();

    private Elasticsearch900AdaptivePointsFormat() {}

    @Override
    public PointsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Elasticsearch900AdaptivePointsWriter(state);
    }

    @Override
    public PointsReader fieldsReader(SegmentReadState state) throws IOException {
        return Elasticsearch900AdaptivePointsWriter.fieldsReader(state);
    }
}
