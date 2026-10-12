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
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.StringLiteralDeduplicator;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.store.FieldInfoCachingDirectory;

import java.io.IOException;
import java.util.Map;

/**
 * Shares the field infos a segment read produces.
 *
 * <p>Names and attribute maps are interned node-wide, so shards of one index and indices of one data stream share them. Whole
 * {@link FieldInfo} instances are shared as well when the segment's directory is a {@link FieldInfoCachingDirectory}, which
 * holds them for a single shard: field numbers are assigned per IndexWriter and form part of the identity.
 *
 * <p>Selected by {@link ElasticsearchFieldInfosFormat}. Directories are only wrapped when
 * {@link FieldInfoCachingDirectory#FEATURE_FLAG} is enabled, and tooling paths such as snapshot inspection or checkindex do not
 * wrap them at all, so those reads share names and attribute maps but retain no whole instances.
 */
public final class CachingFieldInfosFormat extends FieldInfosFormat {

    private static final Map<Map<String, String>, Map<String, String>> attributeDeduplicator = ConcurrentCollections.newConcurrentMap();

    private static final StringLiteralDeduplicator attributesDeduplicator = new StringLiteralDeduplicator();

    private static Map<String, String> internStringStringMap(Map<String, String> m) {
        if (m.size() > 10) {
            return m;
        }
        var res = attributeDeduplicator.get(m);
        if (res == null) {
            if (attributeDeduplicator.size() > 100) {
                // Unexpected edge case to have more than 100 different attribute maps
                // Just to be safe, don't retain more than 100 maps to prevent a potential memory leak
                attributeDeduplicator.clear();
            }
            final Map<String, String> interned = Maps.newHashMapWithExpectedSize(m.size());
            m.forEach((key, value) -> interned.put(attributesDeduplicator.deduplicate(key), attributesDeduplicator.deduplicate(value)));
            res = Map.copyOf(interned);
            attributeDeduplicator.put(res, res);
        }
        return res;
    }

    private final FieldInfosFormat delegate;

    public CachingFieldInfosFormat(FieldInfosFormat delegate) {
        this.delegate = delegate;
    }

    @Override
    public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
        final FieldInfos fieldInfos = delegate.read(directory, segmentInfo, segmentSuffix, iocontext);
        return share(fieldInfos, FieldInfoCachingDirectory.unwrap(segmentInfo.dir));
    }

    /**
     * @param cache holds whole instances for one directory, or null when the segment's directory does not carry one
     */
    private static FieldInfos share(FieldInfos fieldInfos, @Nullable FieldInfoCachingDirectory cache) {
        final FieldInfo[] deduplicated = new FieldInfo[fieldInfos.size()];
        int i = 0;
        for (FieldInfo fi : fieldInfos) {
            final String name = FieldMapper.internFieldName(fi.getName());
            final Map<String, String> attrs = internStringStringMap(fi.attributes());
            final FieldInfoKey key = new FieldInfoKey(
                name,
                fi.number,
                fi.hasTermVectors(),
                fi.omitsNorms(),
                fi.hasPayloads(),
                fi.getIndexOptions(),
                fi.getDocValuesType(),
                fi.docValuesSkipIndexType(),
                fi.getDocValuesGen(),
                attrs,
                fi.getPointDimensionCount(),
                fi.getPointIndexDimensionCount(),
                fi.getPointNumBytes(),
                fi.getVectorDimension(),
                fi.getVectorEncoding(),
                fi.getVectorSimilarityFunction(),
                fi.isSoftDeletesField(),
                fi.isParentField()
            );
            deduplicated[i++] = cache == null ? key.toFieldInfo() : cache.internFieldInfo(key, key::toFieldInfo);
        }
        return new FieldInfosWithUsages(deduplicated);
    }

    @Override
    public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context)
        throws IOException {
        delegate.write(directory, segmentInfo, segmentSuffix, infos, context);
    }

    /**
     * Cache key for FieldInfo deduplication. All fields included by value; {@code name} and {@code attributes} are expected
     * to already be canonical/interned so equality on them is cheap.
     *
     * <p>{@link #toFieldInfo()} is the single place that calls the {@link FieldInfo} constructor, so any future change to
     * the {@link FieldInfo} constructor in Lucene will surface as a compile error here -- forcing the corresponding new
     * component to be added to this record as well.
     */
    private record FieldInfoKey(
        String name,
        int number,
        boolean hasTermVectors,
        boolean omitsNorms,
        boolean hasPayloads,
        IndexOptions indexOptions,
        DocValuesType docValuesType,
        DocValuesSkipIndexType docValuesSkipIndexType,
        long docValuesGen,
        Map<String, String> attributes,
        int pointDimensionCount,
        int pointIndexDimensionCount,
        int pointNumBytes,
        int vectorDimension,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction vectorSimilarityFunction,
        boolean softDeletesField,
        boolean isParentField
    ) {
        FieldInfo toFieldInfo() {
            return new FieldInfo(
                name,
                number,
                hasTermVectors,
                omitsNorms,
                hasPayloads,
                indexOptions,
                docValuesType,
                docValuesSkipIndexType,
                docValuesGen,
                attributes,
                pointDimensionCount,
                pointIndexDimensionCount,
                pointNumBytes,
                vectorDimension,
                vectorEncoding,
                vectorSimilarityFunction,
                softDeletesField,
                isParentField
            );
        }
    }
}
