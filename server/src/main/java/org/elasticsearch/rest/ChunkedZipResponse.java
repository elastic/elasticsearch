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
import java.util.function.Consumer;
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

    private record ChunkedZipEntry(ZipEntry zipEntry, ChunkedRestResponseBodyPart firstBodyPart, Releasable releasable) {}

    private BytesStream target;

    private final OutputStream out = new OutputStream() {
        @Override
        public void write(int b) throws IOException {
            assert target != null;
            target.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            assert target != null;
            target.write(b, off, len);
        }
    };

    private final ZipOutputStream zipOutputStream = new ZipOutputStream(out, StandardCharsets.UTF_8);

    private final String filename;
    private final RestChannel restChannel;

    @Nullable // if the first part hasn't been sent yet
    private SubscribableListener<ChunkedRestResponseBodyPart> continuationListener;

    @Nullable // if not currently sending an entry
    private Releasable releasable;

    /**
     * @param filename     The name of the zip file, which appears in the {@code Content-Disposition} HTTP header of the response, and also
     *                     is used as a directory prefix for all entries.
     * @param restChannel  The {@link RestChannel} on which to send the response.
     * @param onCompletion A resource which is released when the transmission is complete.
     */
    public ChunkedZipResponse(String filename, RestChannel restChannel, Releasable onCompletion) {
        this.filename = filename;
        this.restChannel = restChannel;
        this.listenersRefs = AbstractRefCounted.of(() -> enqueueEntry(null, null, ActionListener.releasing(onCompletion)));
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
     * Create a listener which, when completed, will write the result {@link ChunkedRestResponseBodyPart} as an entry in the response stream
     * with the given name. If the listener is completed successfully with {@code null}, or exceptionally, then no entry is sent. When all
     * listeners created by this method have been completed, the zip file footer is sent.
     * <p>
     * This method may be called as long as this {@link ChunkedZipResponse} is not closed, or there is at least one other incomplete entry
     * listener.
     *
     * @param entryName The name of the entry in the response zip file.
     * @param listener  A listener which is completed when the entry has been completely processed: either fully sent, or else the request
     *                  was cancelled and the response will not be used any further. If the returned entry listener is completed
     *                  exceptionally then the exception is passed to {@code listener}, otherwise this listener is completed successfully.
     */
    public ActionListener<ChunkedRestResponseBodyPart> newEntryListener(String entryName, ActionListener<Void> listener) {
        if (listenersRefs.tryIncRef()) {
            final var zipEntry = new ZipEntry(filename + "/" + entryName);
            return ActionListener.assertOnce(ActionListener.releaseAfter(listener.delegateFailureAndWrap((l, firstBodyPart) -> {
                if (firstBodyPart == null) {
                    l.onResponse(null);
                } else {
                    enqueueEntry(zipEntry, firstBodyPart, l);
                }
            }), listenersRefs::decRef));
        } else {
            assert false : "already closed";
            throw new AlreadyClosedException("response already closed");
        }
    }

    /**
     * Queue of entries that are ready for transmission.
     */
    private final Queue<ChunkedZipEntry> entryQueue = new LinkedBlockingQueue<>();

    /**
     * Upper bound on the number of entries in the queue, atomically modified to ensure there's only one thread processing queue entries
     * at once.
     */
    private final AtomicInteger queueLength = new AtomicInteger();

    /**
     * Ref-counting for access to the queue, to avoid clearing the queue on abort concurrently with an entry being sent.
     */
    private final RefCounted queueRefs = AbstractRefCounted.of(this::drainQueue);

    /**
     * Flag to indicate if the request has been aborted, at which point we should stop enqueueing more entries and promptly clean up the
     * ones being sent.
     */
    private final AtomicBoolean isRestResponseFinished = new AtomicBoolean();

    private boolean tryAcquireQueueRef() {
        return isRestResponseFinished.get() == false && queueRefs.tryIncRef();
    }

    private void enqueueEntry(ZipEntry zipEntry, ChunkedRestResponseBodyPart firstBodyPart, ActionListener<Void> listener) {
        if (tryAcquireQueueRef()) {
            try {
                entryQueue.add(new ChunkedZipEntry(zipEntry, firstBodyPart, () -> listener.onResponse(null)));
                if (queueLength.getAndIncrement() == 0) {
                    final var nextEntry = entryQueue.poll();
                    assert nextEntry != null;
                    final var continuation = new QueueConsumer(nextEntry.zipEntry(), nextEntry.firstBodyPart());
                    assert releasable == null;
                    releasable = nextEntry.releasable();
                    final var currentContinuationListener = continuationListener;
                    continuationListener = new SubscribableListener<>();
                    if (currentContinuationListener == null) {
                        final var restResponse = RestResponse.chunked(RestStatus.OK, continuation, this::restResponseFinished);
                        restResponse.addHeader("content-disposition", Strings.format("attachment; filename=\"%s.zip\"", filename));
                        restChannel.sendResponse(restResponse);
                    } else {
                        currentContinuationListener.onResponse(continuation);
                    }
                }
            } finally {
                queueRefs.decRef();
            }
        } else {
            listener.onFailure(new AlreadyClosedException("response already closed"));
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
            if (releasable != null) {
                releasables.add(releasable);
                releasable = null;
            }
            ChunkedZipEntry entry;
            while ((entry = entryQueue.poll()) != null) {
                releasables.add(entry.releasable());
            }
            assert entryQueue.isEmpty() : entryQueue.size(); // no concurrent adds
            assert releasables.size() <= taskCount : taskCount + " vs " + releasables.size();
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(releasables));
        }
    }

    /**
     * A {@link ChunkedRestResponseBodyPart} which will yield all currently-available chunks by consuming entries from the queue.
     */
    private final class QueueConsumer implements ChunkedRestResponseBodyPart {

        private ZipEntry zipEntry;
        private ChunkedRestResponseBodyPart bodyPart;
        private boolean isPartComplete;
        private boolean isLastPart;
        private Consumer<ActionListener<ChunkedRestResponseBodyPart>> getNextPart;
        private ArrayList<Releasable> nextReleasables = new ArrayList<>(); // preserved between chunks because will often be unused

        QueueConsumer(ZipEntry zipEntry, ChunkedRestResponseBodyPart bodyPart) {
            this.zipEntry = zipEntry;
            this.bodyPart = bodyPart;
        }

        @Override
        public boolean isPartComplete() {
            return isPartComplete;
        }

        @Override
        public boolean isLastPart() {
            return isLastPart;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            assert getNextPart != null;
            getNextPart.accept(listener);
        }

        private void transferReleasable(ArrayList<Releasable> releasables) {
            assert queueRefs.hasReferences();

            if (releasable == null) {
                return;
            }

            if (releasables == nextReleasables) {
                nextReleasables = new ArrayList<>();
            }

            releasables.add(releasable);
            releasable = null;
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            final ArrayList<Releasable> releasables = nextReleasables;
            assert releasables.isEmpty();
            try {
                if (tryAcquireQueueRef()) {
                    try {
                        final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
                        assert target == null;
                        target = chunkStream;

                        do {
                            try {
                                if (bodyPart == null) {
                                    // no more entries
                                    assert zipEntry == null;
                                    assert entryQueue.isEmpty() : entryQueue.size();
                                    zipOutputStream.finish();
                                    isPartComplete = true;
                                    isLastPart = true;
                                    transferReleasable(releasables);
                                    assert getNextPart == null;
                                } else if (zipEntry != null) {
                                    // new entry, so write the entry header
                                    zipOutputStream.putNextEntry(zipEntry);
                                    zipEntry = null;
                                } else {
                                    // writing entry body
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
                                        if (bodyPart.isLastPart()) {
                                            zipOutputStream.closeEntry();
                                            transferReleasable(releasables);
                                            final var newQueueLength = queueLength.decrementAndGet();
                                            if (newQueueLength == 0) {
                                                // next entry isn't available yet, so we stop iterating
                                                isPartComplete = true;
                                                assert getNextPart == null;
                                                getNextPart = continuationListener::addListener;
                                            } else {
                                                // next entry is immediately available so start sending its chunks too
                                                final var nextEntry = entryQueue.poll();
                                                assert nextEntry != null;
                                                zipEntry = nextEntry.zipEntry();
                                                bodyPart = nextEntry.firstBodyPart();
                                                releasable = nextEntry.releasable();
                                            }
                                        } else {
                                            // this body part has a continuation, for which we must wait
                                            isPartComplete = true;
                                            assert getNextPart == null;
                                            getNextPart = l -> bodyPart.getNextPart(l.map(p -> new QueueConsumer(null, p)));
                                        }
                                    }
                                }
                            } finally {
                                zipOutputStream.flush();
                            }
                        } while (isPartComplete == false && chunkStream.size() < sizeHint);

                        assert (releasables == nextReleasables) == releasables.isEmpty();
                        assert nextReleasables.isEmpty();

                        final Releasable chunkStreamReleasable = () -> Releasables.closeExpectNoException(chunkStream);
                        final var result = new ReleasableBytesReference(
                            chunkStream.bytes(),
                            releasables.isEmpty()
                                ? chunkStreamReleasable
                                : Releasables.wrap(Iterators.concat(Iterators.single(chunkStreamReleasable), releasables.iterator()))
                        );

                        target = null;
                        return result;
                    } finally {
                        queueRefs.decRef();
                    }
                } else {
                    // request aborted, nothing more to send (queue is being cleared by queueRefs#closeInternal)
                    isPartComplete = true;
                    isLastPart = true;
                    return new ReleasableBytesReference(BytesArray.EMPTY, () -> {});
                }
            } catch (Exception e) {
                logger.error("failure encoding chunk", e);
                throw e;
            } finally {
                if (target != null) {
                    assert false : "failure encoding chunk";
                    IOUtils.closeWhileHandlingException(target, Releasables.wrap(releasables));
                    target = null;
                }
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return ZIP_CONTENT_TYPE;
        }
    }
}
