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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.StringLiteralDeduplicator;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.mapper.FieldMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Wrapper around a {@link FieldInfosFormat} that will deduplicate and intern all field names, attribute-keys and -values, and in most
 * cases attribute maps on read. We use this to reduce the per-field overhead for Elasticsearch instances holding a large number of
 * segments.
 */
public final class DeduplicatingFieldInfosFormat extends FieldInfosFormat {

    private static final Map<Map<String, String>, Map<String, String>> attributeDeduplicator = ConcurrentCollections.newConcurrentMap();

    private static final StringLiteralDeduplicator attributesDeduplicator = new StringLiteralDeduplicator();

    private final FieldInfosFormat delegate;

    public DeduplicatingFieldInfosFormat(FieldInfosFormat delegate) {
        this.delegate = delegate;
    }

    @Override
    public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
        final FieldInfos fieldInfos = delegate.read(directory, segmentInfo, segmentSuffix, iocontext);
        final FieldInfo[] deduplicated = new FieldInfo[fieldInfos.size()];
        int i = 0;
        for (FieldInfo fi : fieldInfos) {
            deduplicated[i++] = new FieldInfo(
                FieldMapper.internFieldName(fi.getName()),
                fi.number,
                fi.hasTermVectors(),
                fi.omitsNorms(),
                fi.hasPayloads(),
                fi.getIndexOptions(),
                fi.getDocValuesType(),
                fi.docValuesSkipIndexType(),
                fi.getDocValuesGen(),
                internStringStringMap(fi.attributes()),
                fi.getPointDimensionCount(),
                fi.getPointIndexDimensionCount(),
                fi.getPointNumBytes(),
                fi.getVectorDimension(),
                fi.getVectorEncoding(),
                fi.getVectorSimilarityFunction(),
                fi.isSoftDeletesField(),
                fi.isParentField()
            );
        }
        return new FieldInfosWithUsages(deduplicated);
    }

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

    @Override
    public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context)
        throws IOException {
        delegate.write(directory, segmentInfo, segmentSuffix, infos, context);
    }

}
