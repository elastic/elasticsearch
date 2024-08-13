/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * A REST response with {@code Content-type: application/zip} to which the caller can write entries in an asynchronous and streaming
 * fashion.
 * <p>
 * Callers obtain a listener for individual entries using {@link #newEntryListener} and complete these listeners to submit the corresponding
 * entries for transmission. Internally, the output entries are held in a queue in the order in which the entry listeners are completed.
 * If the queue becomes empty then the response transmission is paused until the next entry becomes available.
 * <p>
 * The internal queue is unbounded. It is the caller's responsibility to ensure that the response does not consume an excess of resources
 * while it's being sent.
 * <p>
 * The caller must eventually call {@link ChunkedZipResponse#close} to finish the transmission of the response.
 * <p>
 * Note that individual entries can also pause themselves mid-transmission, since listeners returned by {@link #newEntryListener} accept a
 * pauseable {@link ChunkedRestResponseBodyPart}. Zip files do not have any mechanism which supports the multiplexing of outputs, so if the
 * entry at the head of the queue is paused then that will hold up the transmission of all subsequent entries too.
 */
public final class ChunkedZipResponse implements Releasable {

    public static final String ZIP_CONTENT_TYPE = "application/zip";

    /**
     * The underlying stream that collects the raw bytes to be transmitted. Mutable, because we collect the contents of each chunk in a
     * distinct stream that is held in this field while that chunk is under construction.
     */
    @Nullable // if there's no chunk under construction
    private BytesStream targetStream;

    private final ZipOutputStream zipOutputStream = new ZipOutputStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
            assert targetStream != null;
            targetStream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            assert targetStream != null;
            targetStream.write(b, off, len);
        }
    }, StandardCharsets.UTF_8);

    private final String filename;
    private final RestChannel restChannel;

    /**
     * A listener for the first part (i.e. sequence of chunks of zipped data) of the next entry to become available for transmission after a
     * pause. Completed with the newly-created unique active {@link AvailableChunksZipResponseBodyPart} within {@link #enqueueEntry}, and
     * subscribed to via {@link AvailableChunksZipResponseBodyPart#getNextPart} when the current {@link AvailableChunksZipResponseBodyPart}
     * becomes inactive because of a transmission pause.
     */
    @Nullable // if the first part hasn't been sent yet
    private SubscribableListener<ChunkedRestResponseBodyPart> nextAvailableChunksListener;

    /**
     * A resource to be released when the transmission of the current entry is complete. Note that we may complete the transmission of
     * multiple entries at the same time, if they are all processed by one call to {@link AvailableChunksZipResponseBodyPart#encodeChunk}
     * and transmitted together.
     */
    @Nullable // if not currently sending an entry
    private Releasable currentEntryReleasable;

    /**
     * @param filename     The name of the zip file, which appears in the {@code Content-Disposition} HTTP header of the response, and also
     *                     is used as a directory prefix for all entries.
     * @param restChannel  The {@link RestChannel} on which to send the response.
     * @param onCompletion A resource which is released when the transmission is complete.
     */
    public ChunkedZipResponse(String filename, RestChannel restChannel, Releasable onCompletion) {
        this.filename = filename;
        this.restChannel = restChannel;
        this.listenersRefs = AbstractRefCounted.of(() -> enqueueEntry(null, NO_MORE_ENTRIES, onCompletion));
        this.rootListenerRef = Releasables.releaseOnce(listenersRefs::decRef);
    }

    private final RefCounted listenersRefs;
    private final Releasable rootListenerRef;

    /**
     * Close this {@link ChunkedZipResponse}. Once closed, when there are no more pending listeners the zip file footer is sent.
     */
    @Override
    public void close() {
        rootListenerRef.close();
    }

    /**
     * Create a listener which, when completed, will write the result {@link ChunkedRestResponseBodyPart}, and any following parts, as an
     * entry in the response stream with the given name. If the listener is completed successfully with {@code null}, or exceptionally, then
     * no entry is sent. When all listeners created by this method have been completed, the zip file footer is sent.
     * <p>
     * This method may be called as long as this {@link ChunkedZipResponse} is not closed, or there is at least one other incomplete entry
     * listener.
     *
     * @param entryName  The name of the entry in the response zip file.
     * @param releasable A resource which is released when the entry has been completely processed, i.e. when
     *                   <ul>
     *                   <li>the sequence of {@link ChunkedRestResponseBodyPart} instances have been fully sent, or</li>
     *                   <li>the listener was completed with {@code null}, or an exception, indicating that no entry is to be sent, or</li>
     *                   <li>the overall response was cancelled before completion and all resources related to the partial transmission of
     *                   this entry have been released.</li>
     *                   </ul>
     */
    public ActionListener<ChunkedRestResponseBodyPart> newEntryListener(String entryName, Releasable releasable) {
        if (listenersRefs.tryIncRef()) {
            final var zipEntry = new ZipEntry(filename + "/" + entryName);
            return ActionListener.assertOnce(ActionListener.releaseAfter(new ActionListener<>() {
                @Override
                public void onResponse(ChunkedRestResponseBodyPart chunkedRestResponseBodyPart) {
                    if (chunkedRestResponseBodyPart == null) {
                        Releasables.closeExpectNoException(releasable);
                    } else {
                        enqueueEntry(zipEntry, chunkedRestResponseBodyPart, releasable);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    Releasables.closeExpectNoException(releasable);
                }

                @Override
                public String toString() {
                    return "ZipEntry[" + zipEntry.getName() + "]";
                }
            }, listenersRefs::decRef));
        } else {
            assert false : "already closed";
            throw new AlreadyClosedException("response already closed");
        }
    }

    /**
     * A zip file entry which is ready for transmission, to be stored in {@link #entryQueue}.
     *
     * @param zipEntry      The entry metadata, to be written in its header.
     * @param firstBodyPart The first part of the entry body. Subsequent parts, if present, come from
     *                      {@link ChunkedRestResponseBodyPart#getNextPart}.
     * @param releasable    A resource to release when this entry has been fully transmitted, or is no longer required because the
     *                      transmission was cancelled.
     */
    private record ChunkedZipEntry(ZipEntry zipEntry, ChunkedRestResponseBodyPart firstBodyPart, Releasable releasable) {}

    /**
     * Queue of entries that are ready for transmission.
     */
    private final Queue<ChunkedZipEntry> entryQueue = new LinkedBlockingQueue<>();

    /**
     * Upper bound on the number of entries in the queue, atomically modified to ensure there's only one thread processing queue entries at
     * once.
     */
    private final AtomicInteger queueLength = new AtomicInteger();

    /**
     * Ref-counting for access to the queue, to avoid clearing the queue on abort concurrently with an entry being sent.
     */
    private final RefCounted queueRefs = AbstractRefCounted.of(this::drainQueue);

    /**
     * Flag to indicate if the request has been aborted, at which point we should stop enqueueing more entries and promptly clean up the
     * ones being sent. It's safe to ignore this, but without it in theory a constant stream of calls to {@link #enqueueEntry} could prevent
     * {@link #drainQueue} from running for arbitrarily long.
     */
    private final AtomicBoolean isRestResponseFinished = new AtomicBoolean();

    private boolean tryAcquireQueueRef() {
        return isRestResponseFinished.get() == false && queueRefs.tryIncRef();
    }

    /**
     * Called when an entry is ready for its transmission to start. Adds the entry to {@link #entryQueue} and spawns a new
     * {@link AvailableChunksZipResponseBodyPart} if none is currently active.
     *
     * @param zipEntry      The entry metadata.
     * @param firstBodyPart The first part of the entry. Entries may comprise multiple parts, with transmission pauses in between.
     * @param releasable    Released when the entry has been fully transmitted.
     */
    private void enqueueEntry(ZipEntry zipEntry, ChunkedRestResponseBodyPart firstBodyPart, Releasable releasable) {
        if (tryAcquireQueueRef()) {
            try {
                entryQueue.add(new ChunkedZipEntry(zipEntry, firstBodyPart, releasable));
                if (queueLength.getAndIncrement() == 0) {
                    // There is no active AvailableChunksZipResponseBodyPart, but there is now an entry in the queue, so we must create a
                    // AvailableChunksZipResponseBodyPart to process it (along with any other entries that are concurrently added to the
                    // queue). It's safe to mutate releasable and continuationListener here because they are only otherwise accessed by an
                    // active AvailableChunksZipResponseBodyPart (which does not exist) or when all queueRefs have been released (which they
                    // have not here).
                    final var nextEntry = entryQueue.poll();
                    assert nextEntry != null;
                    final var availableChunks = new AvailableChunksZipResponseBodyPart(nextEntry.zipEntry(), nextEntry.firstBodyPart());
                    assert currentEntryReleasable == null;
                    currentEntryReleasable = nextEntry.releasable();
                    final var currentAvailableChunksListener = nextAvailableChunksListener;
                    nextAvailableChunksListener = new SubscribableListener<>();
                    if (currentAvailableChunksListener == null) {
                        // We are not resuming after a pause, this is the first entry to be sent, so we start the response transmission.
                        final var restResponse = RestResponse.chunked(RestStatus.OK, availableChunks, this::restResponseFinished);
                        restResponse.addHeader("content-disposition", Strings.format("attachment; filename=\"%s.zip\"", filename));
                        restChannel.sendResponse(restResponse);
                    } else {
                        // We are resuming transmission after a pause, so just carry on sending the response body.
                        currentAvailableChunksListener.onResponse(availableChunks);
                    }
                }
            } finally {
                queueRefs.decRef();
            }
        } else {
            Releasables.closeExpectNoException(releasable);
        }
    }

    private void restResponseFinished() {
        assert Transports.assertTransportThread();
        if (isRestResponseFinished.compareAndSet(false, true)) {
            queueRefs.decRef();
        }
    }

    private void drainQueue() {
        assert isRestResponseFinished.get();
        assert queueRefs.hasReferences() == false;
        final var taskCount = queueLength.get() + 1;
        final var releasables = new ArrayList<Releasable>(taskCount);
        try {
            releasables.add(currentEntryReleasable);
            currentEntryReleasable = null;
            ChunkedZipEntry entry;
            while ((entry = entryQueue.poll()) != null) {
                releasables.add(entry.releasable());
            }
            assert entryQueue.isEmpty() : entryQueue.size(); // no concurrent adds
            assert releasables.size() == taskCount || releasables.size() == taskCount - 1 : taskCount + " vs " + releasables.size();
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(releasables));
        }
    }

    /**
     * A {@link ChunkedRestResponseBodyPart} which will yield all currently-available chunks by consuming entries from {@link #entryQueue}.
     * There is only ever at most one active instance of this class at any time, in the sense that one such instance becoming inactive
     * <i>happens-before</i> the creation of the next instance. One of these parts may send chunks for more than one entry.
     */
    private final class AvailableChunksZipResponseBodyPart implements ChunkedRestResponseBodyPart {

        /**
         * The next {@link ZipEntry} header whose transmission to start.
         */
        @Nullable // if no entry is available, or we've already sent the header for the current entry and are now sending its body.
        private ZipEntry zipEntry;

        /**
         * The body part which is currently being transmitted, or {@link #NO_MORE_ENTRIES} if we're transmitting the zip file footer.
         */
        private ChunkedRestResponseBodyPart bodyPart;

        /**
         * True when we have run out of compressed chunks ready for immediate transmission, so the response is paused, but we expect to send
         * more data later.
         */
        private boolean isResponsePaused;

        /**
         * True when we have sent the zip file footer, or the response was cancelled.
         */
        private boolean isResponseComplete;

        /**
         * A listener which is created when there are no more available chunks, so transmission is paused, subscribed to in
         * {@link #getNextPart}, and then completed with the next body part (sequence of zipped chunks, i.e. a new (unique) active
         * {@link AvailableChunksZipResponseBodyPart}).
         */
        private SubscribableListener<ChunkedRestResponseBodyPart> getNextPartListener;

        /**
         * A cache for an empty list to be used to collect the {@code Releasable} instances to be released when the next chunk has been
         * fully transmitted. It's a list because a call to {@link #encodeChunk} may yield a chunk that completes several entries, each of
         * which has its own resources to release. We cache this value across chunks because most chunks won't release anything, so we can
         * keep the empty list around for later to save on allocations.
         */
        private ArrayList<Releasable> nextReleasablesCache = new ArrayList<>();

        AvailableChunksZipResponseBodyPart(ZipEntry zipEntry, ChunkedRestResponseBodyPart bodyPart) {
            this.zipEntry = zipEntry;
            this.bodyPart = bodyPart;
        }

        /**
         * @return whether this part of the compressed response is complete
         */
        @Override
        public boolean isPartComplete() {
            return isResponsePaused || isResponseComplete;
        }

        @Override
        public boolean isLastPart() {
            return isResponseComplete;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            assert getNextPartListener != null;
            getNextPartListener.addListener(listener);
        }

        /**
         * Transfer {@link #currentEntryReleasable} into the supplied collection (i.e. add it to {@code releasables} and then clear
         * {@link #currentEntryReleasable}). Called when the last chunk of the last part of the current entry is serialized, so that we can
         * start serializing chunks of the next entry straight away whilst delaying the release of the current entry's resources until the
         * transmission of the chunk that is currently under construction.
         */
        private void transferCurrentEntryReleasable(ArrayList<Releasable> releasables) {
            assert queueRefs.hasReferences();

            if (currentEntryReleasable == null) {
                return;
            }

            if (releasables == nextReleasablesCache) {
                // adding the first value, so we must line up a new cached value for the next caller
                nextReleasablesCache = new ArrayList<>();
            }

            releasables.add(currentEntryReleasable);
            currentEntryReleasable = null;
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            assert Transports.isTransportThread(Thread.currentThread());

            final ArrayList<Releasable> releasables = nextReleasablesCache;
            assert releasables.isEmpty();
            try {
                if (tryAcquireQueueRef()) {
                    try {
                        assert queueLength.get() > 0;
                        // This is the current unique active AvailableChunksZipResponseBodyPart (i.e. queueLength is strictly positive and
                        // we hold a queueRef), so any concurrent calls to enqueueEntry() at this point will just add to the queue and won't
                        // spawn a new AvailableChunksZipResponseBodyPart or mutate any fields.

                        final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
                        assert targetStream == null;
                        targetStream = chunkStream;

                        do {
                            writeNextBytes(sizeHint, recycler, releasables);
                        } while (isResponseComplete == false && isResponsePaused == false && chunkStream.size() < sizeHint);

                        assert (releasables == nextReleasablesCache) == releasables.isEmpty();
                        assert nextReleasablesCache.isEmpty();

                        final Releasable chunkStreamReleasable = () -> Releasables.closeExpectNoException(chunkStream);
                        final var result = new ReleasableBytesReference(
                            chunkStream.bytes(),
                            releasables.isEmpty()
                                ? chunkStreamReleasable
                                : Releasables.wrap(Iterators.concat(Iterators.single(chunkStreamReleasable), releasables.iterator()))
                        );

                        targetStream = null;
                        return result;
                    } finally {
                        queueRefs.decRef();
                    }
                } else {
                    // request aborted, nothing more to send (queue is being cleared by queueRefs#closeInternal)
                    isResponseComplete = true;
                    return new ReleasableBytesReference(BytesArray.EMPTY, () -> {});
                }
            } catch (Exception e) {
                logger.error("failure encoding chunk", e);
                throw e;
            } finally {
                if (targetStream != null) {
                    assert false : "failure encoding chunk";
                    IOUtils.closeWhileHandlingException(targetStream, Releasables.wrap(releasables));
                    targetStream = null;
                }
            }
        }

        private void writeNextBytes(int sizeHint, Recycler<BytesRef> recycler, ArrayList<Releasable> releasables) throws IOException {
            try {
                if (bodyPart == NO_MORE_ENTRIES) {
                    // When the last ref from listenersRefs is completed we enqueue a final sentinel entry to trigger the transmission of
                    // the zip file footer, which happens here:
                    finishResponse(releasables);
                    return;
                }

                if (zipEntry != null) {
                    // This is the start of a new entry, so write the entry header:
                    zipOutputStream.putNextEntry(zipEntry);
                    zipEntry = null;
                }

                // Write the next chunk of the current entry to the zip stream
                if (bodyPart.isPartComplete() == false) {
                    try (var innerChunk = bodyPart.encodeChunk(sizeHint, recycler)) {
                        final var iterator = innerChunk.iterator();
                        BytesRef bytesRef;
                        while ((bytesRef = iterator.next()) != null) {
                            zipOutputStream.write(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                        }
                    }
                }
                if (bodyPart.isPartComplete()) {
                    // Complete the current part: if the current entry is incomplete then set up a listener for its next part, otherwise
                    // move on to the next available entry and start sending its content.
                    finishCurrentPart(releasables);
                }
            } finally {
                // Flush any buffered data (but not the compressor) to chunkStream so that its size is accurate.
                zipOutputStream.flush();
            }
        }

        private void finishCurrentPart(ArrayList<Releasable> releasables) throws IOException {
            if (bodyPart.isLastPart()) {
                zipOutputStream.closeEntry();
                transferCurrentEntryReleasable(releasables);
                final var newQueueLength = queueLength.decrementAndGet();
                if (newQueueLength == 0) {
                    // The current entry is complete, but the next entry isn't available yet, so we pause transmission. This means we are no
                    // longer an active AvailableChunksZipResponseBodyPart, so any concurrent calls to enqueueEntry() at this point will now
                    // spawn a new AvailableChunksZipResponseBodyPart to take our place.
                    isResponsePaused = true;
                    assert getNextPartListener == null;
                    assert nextAvailableChunksListener != null;
                    // Calling our getNextPart() will eventually yield the next body part supplied to enqueueEntry():
                    getNextPartListener = nextAvailableChunksListener;
                } else {
                    // The current entry is complete, and the first part of the next entry is already available, so we start sending its
                    // chunks too. This means we're still the unique active AvailableChunksZipResponseBodyPart. We re-use this
                    // AvailableChunksZipResponseBodyPart instance rather than creating a new one to avoid unnecessary allocations.
                    final var nextEntry = entryQueue.poll();
                    assert nextEntry != null;
                    zipEntry = nextEntry.zipEntry();
                    bodyPart = nextEntry.firstBodyPart();
                    currentEntryReleasable = nextEntry.releasable();
                }
            } else {
                // The current entry has more parts to come, but we have reached the end of the current part, so we assume that the next
                // part is not yet available and therefore must pause transmission. This means we are no longer an active
                // AvailableChunksZipResponseBodyPart, but also another call to enqueueEntry() won't create a new
                // AvailableChunksZipResponseBodyPart because the current entry is still counted in queueLength:
                assert queueLength.get() > 0;
                // Instead, we create a new active AvailableChunksZipResponseBodyPart when the next part of the current entry becomes
                // available. It doesn't affect correctness if the next part is already available, it's just a little less efficient to make
                // a new AvailableChunksZipResponseBodyPart in that case. That's ok, entries can coalesce all the available parts together
                // themselves if efficiency really matters.
                isResponsePaused = true;
                assert getNextPartListener == null;
                // Calling our getNextPart() will eventually yield the next body part from the current entry:
                getNextPartListener = SubscribableListener.newForked(
                    l -> bodyPart.getNextPart(l.map(p -> new AvailableChunksZipResponseBodyPart(null, p)))
                );
            }
        }

        private void finishResponse(ArrayList<Releasable> releasables) throws IOException {
            assert zipEntry == null;
            assert entryQueue.isEmpty() : entryQueue.size();
            zipOutputStream.finish();
            isResponseComplete = true;
            transferCurrentEntryReleasable(releasables);
            assert getNextPartListener == null;
        }

        @Override
        public String getResponseContentTypeString() {
            return ZIP_CONTENT_TYPE;
        }
    }

    /**
     * Sentinel body part indicating the end of the zip file.
     */
    private static final ChunkedRestResponseBodyPart NO_MORE_ENTRIES = new ChunkedRestResponseBodyPart() {
        @Override
        public boolean isPartComplete() {
            assert false : "never called";
            return true;
        }

        @Override
        public boolean isLastPart() {
            assert false : "never called";
            return true;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            assert false : "never called";
            listener.onFailure(new IllegalStateException("impossible"));
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) {
            assert false : "never called";
            return ReleasableBytesReference.empty();
        }

        @Override
        public String getResponseContentTypeString() {
            assert false : "never called";
            return ZIP_CONTENT_TYPE;
        }
    };
}
