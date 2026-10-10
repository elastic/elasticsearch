/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;

/**
 * Observes doc values while segments are written in order to derive per-segment statistics that are persisted as
 * {@link SegmentInfo#putAttribute(String, String) segment attributes}. A collector only reads values; it never changes what,
 * or how, the codec writes.
 *
 * <p>Collectors are handed to the {@link CodecService} of an index. Whether a collector applies to the index is decided by
 * {@link #appliesTo(MapperService)} against the index's current mapping whenever a segment is flushed or a merge starts. An
 * applicable collector is then invoked once per segment: the flush path is driven by
 * {@link org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat}, which hands over every doc values field of the
 * segment right before the field is written; the merge path is driven by an engine supporting it from
 * {@link org.apache.lucene.index.MergePolicy.OneMerge}, handing over each source segment of a merge before the merged
 * segment is sealed.
 *
 * <p>Collecting stats never fails the flush or merge it observes: an exception thrown by a collector is logged and the failing
 * call is skipped; collection continues with the next field or source, see {@link SegmentStatsCollectors}.
 */
public interface SegmentStatsCollector {

    /**
     * Whether this collector applies to segments of the given index. Evaluated against the current mapping each time a
     * segment is flushed or a merge starts, so mapping updates are picked up.
     *
     * @param mapperService the index's mapper service, giving access to its settings and current mapping, or {@code null} if
     *                      the index has none
     */
    boolean appliesTo(@Nullable MapperService mapperService);

    /**
     * Called for every doc values field of a newly flushed segment; collectors ignore the fields they are not interested in.
     * Doc values updates to existing segments are not reported. The producer may be consumed independently of the format
     * writing the values.
     *
     * @param segment the segment being flushed, attributes may be added to it
     * @param field   the field whose values are about to be written
     * @param values  producer for the values of the field in the segment
     */
    void onFlush(SegmentInfo segment, FieldInfo field, DocValuesProducer values) throws IOException;

    /**
     * Called once per merge, before any source segment is merged.
     *
     * @param mergedSegment the segment produced by the merge, attributes may be added to it by the returned collector
     * @return a collector receiving the source segments of this merge
     */
    MergeCollector onMergeStart(SegmentInfo mergedSegment);

    /**
     * Receives the source segments of a single merge.
     */
    interface MergeCollector {
        /**
         * Called for each source segment of the merge, after all merge policy wrapping has been applied. The reader's live docs
         * therefore reflect the documents that are actually merged. Use
         * {@link org.elasticsearch.common.lucene.Lucene#tryUnwrapSegmentReader} to get at the source {@code SegmentCommitInfo}.
         * Implementations are expected to update the merged segment's attributes incrementally, as there is no explicit
         * completion callback before the merged segment is sealed.
         */
        void onSource(CodecReader source) throws IOException;
    }
}
