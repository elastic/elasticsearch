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
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MergeInfo;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.codec.vectors.es818.DirectIOHint;
import org.elasticsearch.index.store.FsDirectoryFactory;

import java.io.IOException;
import java.util.Set;

public abstract class DirectIOCapableFlatVectorsFormat extends AbstractFlatVectorsFormat {
    protected DirectIOCapableFlatVectorsFormat(String name) {
        super(name);
    }

    protected abstract FlatVectorsReader createReader(SegmentReadState state) throws IOException;

    protected abstract FlatVectorsWriter createWriter(SegmentWriteState state) throws IOException;

    @Override
    public final FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return fieldsWriter(state, true);
    }

    /**
     * @param directIOMergeWrites whether a merge may write the raw vector data with direct I/O under
     *                            {@code index.store.fs.direct_io.vector_merge}. {@code false} only for a
     *                            format that reads the merged vectors back by random access right after
     *                            writing them, see {@code ES93GenericFlatVectorsFormat#withBufferedMergeWrites}.
     *                            The read side of the setting is unaffected.
     */
    public final FlatVectorsWriter fieldsWriter(SegmentWriteState state, boolean directIOMergeWrites) throws IOException {
        return createWriter(directIOMergeWrites ? directIOMergeWriteState(state) : state);
    }

    protected static boolean canUseDirectIO(SegmentReadState state) {
        return FsDirectoryFactory.isHybridFs(state.directory);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return fieldsReader(state, false);
    }

    public FlatVectorsReader fieldsReader(SegmentReadState state, boolean useDirectIO) throws IOException {
        // only readers opened for searching (DEFAULT context) get special treatment: they are the pooled
        // readers a merge later borrows through getMergeInstance()
        if (state.context.context() != IOContext.Context.DEFAULT || canUseDirectIO(state) == false) {
            return createReader(state);
        }
        // two independent decisions: on_disk_rescore says how the search-side reader reads, the
        // directory's index.store.fs.direct_io.vector_merge setting says how merges read
        boolean directIOReads = useDirectIO;
        boolean directIOMerges = FsDirectoryFactory.isDirectIOForVectorMerges(state.directory);
        if (directIOReads == false && directIOMerges == false) {
            return createReader(state);
        }
        SegmentReadState mainState = directIOReads
            ? new SegmentReadState(
                state.directory,
                state.segmentInfo,
                state.fieldInfos,
                new DirectIOContext(state.context.hints()),
                state.segmentSuffix
            )
            : state;
        if (directIOMerges == false) {
            // direct I/O searches, page-cache merges: the merge instance must not be the
            // random-access direct I/O reader, so merges get a plain reader of their own
            return new MergeReaderWrapper(createReader(mainState), () -> createReader(state), directIOReads);
        }
        // MERGE here only selects the merge-sized direct I/O delegate in HybridDirectory; the reader is
        // created from the search-time state, so the context carries no MergeInfo (see DirectIOContext#mergeInfo)
        SegmentReadState mergeDirectIOState = new SegmentReadState(
            state.directory,
            state.segmentInfo,
            state.fieldInfos,
            new DirectIOContext(IOContext.Context.MERGE, state.context.hints()),
            state.segmentSuffix
        );
        // the wrapper serves searches from the main reader and merges from a lazily-created reader
        // whose MERGE-context direct I/O hint the directory routes to its merge-sized delegate. A merge
        // reads each source twice through the instance it takes from getMergeInstance(): once to verify
        // its checksum, once to stream it. Both stay direct on purpose: verifying through the page cache
        // would fault the whole source in, the eviction this setting exists to avoid, so the second
        // device read is the price paid
        return new MergeReaderWrapper(createReader(mainState), () -> createReader(mergeDirectIOState), directIOReads);
    }

    /**
     * Returns the {@link SegmentWriteState} to construct a raw vector writer with. When the state is
     * for a merge and the directory reads and writes raw vectors with direct I/O during merges
     * ({@code index.store.fs.direct_io.vector_merge}), the raw writer's files (the raw vector data
     * file and its metadata sibling) are created with a context carrying direct I/O hints, so that
     * streaming the merged raw vectors to disk does not evict hotter data from the page cache. The
     * raw flat writers create all of their outputs from {@code state.context} in their constructors
     * and merges write straight to the final vector data file (no temp files), so substituting the
     * context here scopes direct I/O to exactly those files. Flush-time writes (small, imminently
     * searched segments) and every other file the formats wrapping the raw format write (quantized
     * vectors, HNSW graph, IVF clusters, per-field metadata, temp files) keep the original context and
     * stay buffered, so they remain page-cache-warm after the merge.
     * <p>
     * {@link #fieldsWriter(SegmentWriteState, boolean)} applies this, so a format wrapping a raw
     * {@link DirectIOCapableFlatVectorsFormat} gets the write side of the setting along with the read
     * side from {@link #fieldsReader}, unless it declines the write side; the read side engages either
     * way. Plain HNSW is the one format that declines it, see
     * {@code ES93GenericFlatVectorsFormat#withBufferedMergeWrites}.
     */
    protected static SegmentWriteState directIOMergeWriteState(SegmentWriteState state) {
        if (state.context.context() != IOContext.Context.MERGE || FsDirectoryFactory.isDirectIOForVectorMerges(state.directory) == false) {
            return state;
        }
        SegmentWriteState directIOState = new SegmentWriteState(
            state.infoStream,
            state.directory,
            state.segmentInfo,
            state.fieldInfos,
            state.segUpdates,
            new DirectIOWriteContext(state.context),
            state.segmentSuffix
        );
        // copied by value: both are only set by Lucene for flushes, and this state is only ever
        // built for merges, so nothing can change underneath the copy
        directIOState.liveDocs = state.liveDocs;
        directIOState.delCountOnFlush = state.delCountOnFlush;
        return directIOState;
    }

    protected static class DirectIOContext implements IOContext {

        private final Context context;
        final Set<FileOpenHint> hints;

        public DirectIOContext(Set<FileOpenHint> hints) {
            this(Context.DEFAULT, hints);
        }

        public DirectIOContext(Context context, Set<FileOpenHint> hints) {
            this.context = context;
            // always add DirectIOHint to the hints given
            this.hints = Sets.union(hints, Set.of(DirectIOHint.INSTANCE));
        }

        @Override
        public Context context() {
            return context;
        }

        // null on purpose: this context only selects the merge-sized direct I/O delegate in HybridDirectory,
        // there is no merge to describe. Lucene's own DirectIODirectory#useDirectIO would dereference it;
        // AlwaysDirectIODirectory overrides that method and never reads it
        @Override
        public MergeInfo mergeInfo() {
            return null;
        }

        @Override
        public FlushInfo flushInfo() {
            return null;
        }

        @Override
        public Set<FileOpenHint> hints() {
            return hints;
        }

        @Override
        public IOContext withHints(FileOpenHint... hints) {
            return new DirectIOContext(context, Set.of(hints));
        }
    }
}
