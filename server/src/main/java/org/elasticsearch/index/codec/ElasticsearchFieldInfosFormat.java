/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.store.FieldInfoCachingDirectory;

import java.io.IOException;
import java.util.Objects;

/**
 * Field infos format that shares {@link org.apache.lucene.index.FieldInfo} instances across the segments of a shard, so that a
 * mapping with many fields costs one set of instances per shard rather than one per segment. Whole instances are shared against
 * the per-directory cache when {@link FieldInfoCachingDirectory#FEATURE_FLAG} is enabled, otherwise only their names and
 * attribute maps are interned node-wide.
 *
 * <p>Writes pass through unchanged; only what a read produces differs.
 */
public final class ElasticsearchFieldInfosFormat extends FieldInfosFormat {

    private final FieldInfosFormat impl;

    public ElasticsearchFieldInfosFormat(FieldInfosFormat delegate) {
        Objects.requireNonNull(delegate);
        this.impl = new CachingFieldInfosFormat(delegate);
    }

    @Override
    public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext context) throws IOException {
        return impl.read(directory, segmentInfo, segmentSuffix, context);
    }

    @Override
    public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context)
        throws IOException {
        impl.write(directory, segmentInfo, segmentSuffix, infos, context);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + impl.getClass().getSimpleName() + ")";
    }
}
