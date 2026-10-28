/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.store.FsDirectoryFactory;

import java.io.IOException;

public abstract class DirectIOCapableFlatVectorsFormat extends AbstractFlatVectorsFormat {
    protected DirectIOCapableFlatVectorsFormat(String name) {
        super(name);
    }

    protected abstract FlatVectorsReader createReader(SegmentReadState state) throws IOException;

    protected abstract FlatVectorsWriter createWriter(SegmentWriteState state) throws IOException;

    /** A writer whose merges go through the page cache: the field did not ask for {@code on_disk_merge}. */
    @Override
    public final FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return fieldsWriter(state, false);
    }

    /**
     * @param directIOMergeWrites whether a merge writes the raw vector data with direct I/O: the field's
     *                            {@code on_disk_merge} option, unless the format declines the write side
     *                            because it reads the merged vectors back by random access right after
     *                            writing them, see {@code ES93GenericFlatVectorsFormat#withBufferedMergeWrites}.
     *                            The read side is decided separately, see {@link #fieldsReader(SegmentReadState, boolean, boolean)}.
     */
    public final FlatVectorsWriter fieldsWriter(SegmentWriteState state, boolean directIOMergeWrites) throws IOException {
        return createWriter(directIOMergeWrites ? directIOMergeWriteState(state) : state);
    }

    protected static boolean canUseDirectIO(SegmentReadState state) {
        return FsDirectoryFactory.isHybridFs(state.directory);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return fieldsReader(state, false, false);
    }

    /**
     * @param useDirectIO whether searches read the raw vectors with direct I/O (the field's {@code on_disk_rescore} option)
     * @param onDiskMerge whether merges read the raw vectors with direct I/O (the field's {@code on_disk_merge} option,
     *                    as recorded in the segment being read)
     */
    public FlatVectorsReader fieldsReader(SegmentReadState state, boolean useDirectIO, boolean onDiskMerge) throws IOException {
        // only readers opened for searching (DEFAULT context) get special treatment: they are the pooled
        // readers a merge later borrows through getMergeInstance()
        if (state.context.context() != IOContext.Context.DEFAULT || canUseDirectIO(state) == false) {
            return createReader(state);
        }
        // the two options are independent, but either one means merges need a reader of their own: with
        // on_disk_merge a merge-context direct I/O reader, with on_disk_rescore anything but the random-access
        // direct I/O search reader
        SegmentReadState searchState = useDirectIO ? directIOSearchState(state) : state;
        SegmentReadState mergeState = onDiskMerge ? directIOMergeState(state) : state;
        if (useDirectIO || onDiskMerge) {
            return new MergeReaderWrapper(createReader(searchState), () -> createReader(mergeState), useDirectIO);
        }
        return createReader(state);
    }

    private static SegmentReadState directIOSearchState(SegmentReadState state) {
        return new SegmentReadState(
            state.directory,
            state.segmentInfo,
            state.fieldInfos,
            DirectIOContext.searchRead(state.context.hints()),
            state.segmentSuffix
        );
    }

    /**
     * Returns the merge-side state with the direct I/O hint; see {@link DirectIOContext#mergeRead}. A merge reads each
     * source at least twice through this reader (checksum, then stream; the bbq types stream it again), all direct on
     * purpose: verifying through the page cache would fault the whole source in, the eviction the option exists to avoid.
     */
    private static SegmentReadState directIOMergeState(SegmentReadState state) {
        return new SegmentReadState(
            state.directory,
            state.segmentInfo,
            state.fieldInfos,
            DirectIOContext.mergeRead(state.context.hints()),
            state.segmentSuffix
        );
    }

    /**
     * Returns the {@link SegmentWriteState} to construct a raw vector writer with. When the state is
     * for a merge on a hybrid fs directory, the raw writer's files (the raw vector data file and its
     * metadata sibling) are created with a context carrying direct I/O hints, so that
     * streaming the merged raw vectors to disk does not evict hotter data from the page cache. The
     * raw flat writers create all of their outputs from {@code state.context} in their constructors
     * and merges write straight to the final vector data file (no temp files), so substituting the
     * context here scopes the hint to exactly those files; the directory then sends only the raw vector
     * data file to the merge delegate (see FsDirectoryFactory.HybridDirectory#isRawVectorFile).
     * Flush-time writes (small, imminently searched segments) and every other file the formats
     * wrapping the raw format write (quantized vectors, HNSW graph, IVF clusters, per-field metadata,
     * temp files) keep the original context and stay buffered, so they remain page-cache-warm after
     * the merge.
     * <p>
     * {@link #fieldsWriter(SegmentWriteState, boolean)} applies this when the field's {@code on_disk_merge}
     * option asks for it, so a format wrapping a raw {@link DirectIOCapableFlatVectorsFormat} gets the
     * write side along with the read side from {@link #fieldsReader}, unless it declines the write side;
     * the read side engages either way. Plain HNSW is the one format that declines it, see
     * {@code ES93GenericFlatVectorsFormat#withBufferedMergeWrites}.
     */
    private static SegmentWriteState directIOMergeWriteState(SegmentWriteState state) {
        if (state.context.context() != IOContext.Context.MERGE || FsDirectoryFactory.isHybridFs(state.directory) == false) {
            return state;
        }
        SegmentWriteState directIOState = new SegmentWriteState(
            state.infoStream,
            state.directory,
            state.segmentInfo,
            state.fieldInfos,
            state.segUpdates,
            DirectIOContext.mergeWrite(state.context),
            state.segmentSuffix
        );
        // copied by value: both are only set by Lucene for flushes, and this state is only ever
        // built for merges, so nothing can change underneath the copy
        directIOState.liveDocs = state.liveDocs;
        directIOState.delCountOnFlush = state.delCountOnFlush;
        return directIOState;
    }
}
