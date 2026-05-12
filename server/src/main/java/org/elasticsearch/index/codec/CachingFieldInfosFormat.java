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
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.store.FieldInfoCachingDirectory;

import java.io.IOException;
import java.util.Map;

/**
 * Like {@link DeduplicatingFieldInfosFormat}, but interns the whole {@link FieldInfo} instance (not just names and attribute
 * maps) against a per-Directory cache owned by {@link FieldInfoCachingDirectory}. When the segment's Directory is wrapped,
 * canonical {@link FieldInfo} instances are shared across all segments and DocValues generations of the same shard.
 *
 * <p>Selected by {@link CodecService.DeduplicateFieldInfosCodec} when
 * {@link FieldInfoCachingDirectory#FEATURE_FLAG} is enabled. When the segment's Directory is not wrapped (e.g. tooling paths
 * like snapshot inspection or checkindex), the format falls through to a per-call read with no retention.
 */
public final class CachingFieldInfosFormat extends FieldInfosFormat {

    private final FieldInfosFormat delegate;

    public CachingFieldInfosFormat(FieldInfosFormat delegate) {
        this.delegate = delegate;
    }

    @Override
    public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
        final FieldInfos fieldInfos = delegate.read(directory, segmentInfo, segmentSuffix, iocontext);
        final FieldInfoCachingDirectory cache = FieldInfoCachingDirectory.unwrap(segmentInfo.dir);
        if (cache == null) {
            return wrapPassthrough(fieldInfos);
        }
        return readWithDirectoryCache(fieldInfos, cache);
    }

    private static FieldInfos wrapPassthrough(FieldInfos fieldInfos) {
        final FieldInfo[] copy = new FieldInfo[fieldInfos.size()];
        int i = 0;
        for (FieldInfo fi : fieldInfos) {
            copy[i++] = fi;
        }
        return new FieldInfosWithUsages(copy);
    }

    private static FieldInfos readWithDirectoryCache(FieldInfos fieldInfos, FieldInfoCachingDirectory cache) {
        final FieldInfo[] deduplicated = new FieldInfo[fieldInfos.size()];
        int i = 0;
        for (FieldInfo fi : fieldInfos) {
            // Field names are interned node-wide via FieldMapper#internFieldName rather than via the per-Directory cache.
            // Different shards (and different indices in the same data stream) share the same mapping and therefore the
            // same field names, but each shard has its own IndexWriter and so its own field-number assignment -- which
            // means FieldInfo instances cannot be shared across shards (number is part of the cache key), but the name
            // String can. The node-wide intern collapses name allocations across every shard on the node.
            final String name = FieldMapper.internFieldName(fi.getName());
            final Map<String, String> attrs = cache.internAttributes(fi.attributes());
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
            deduplicated[i++] = cache.internFieldInfo(
                key,
                () -> new FieldInfo(
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
                )
            );
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
    ) {}
}
