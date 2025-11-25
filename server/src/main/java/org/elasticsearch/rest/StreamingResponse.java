/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A REST response with any body to which the caller can write fragments in an asynchronous and streaming fashion.
 * <p>
 * Callers submit individual fragments of content using {@link #writeFragment}. Internally, the output entries are held in a queue.
 * If the queue becomes empty then the response transmission is paused until the next entry becomes available.
 * <p>
 * The internal queue is unbounded. It is the caller's responsibility to ensure that the response does not consume an excess of resources
 * while it's being sent.
 * <p>
 * The caller must eventually call {@link StreamingResponse#close} to finish the transmission of the response.
 */
public final class StreamingResponse<T> implements Releasable {

    private final RestChannel restChannel;
    private final String responseContentType;
    private final Releasable onCompletion;
    private final CheckedBiConsumer<T, RecyclerBytesStreamOutput, IOException> writer;

    /**
     * A listener for the next fragment to become available for transmission after a pause. Completed with the newly-created unique active
     * {@link AvailableFragmentsResponseBodyPart} within {@link #writeFragment}, and  subscribed to via
     * {@link AvailableFragmentsResponseBodyPart#getNextPart} when the current {@link AvailableFragmentsResponseBodyPart}
     * becomes inactive because of a transmission pause.
     */
    @Nullable // if the first fragment hasn't been sent yet
    private SubscribableListener<ChunkedRestResponseBodyPart> nextAvailableFragmentListener;

    /**
     * A resource to be released when the transmission of the current fragment is complete. Note that we may complete the transmission of
     * multiple fragments at the same time, if they are all processed by one call to {@link AvailableFragmentsResponseBodyPart#encodeChunk}
     * and transmitted together.
     */
    @Nullable // if not currently sending a fragment
    private Releasable currentFragmentReleasable;

    /**
     * @param restChannel         The {@link RestChannel} on which to send the response.
     * @param responseContentType The content type of the response.
     * @param onCompletion        A resource which is released when the transmission is complete.
     */
    public StreamingResponse(
        RestChannel restChannel,
        String responseContentType,
        Releasable onCompletion,
        CheckedBiConsumer<T, RecyclerBytesStreamOutput, IOException> writer
    ) {
        this.restChannel = restChannel;
        this.responseContentType = responseContentType;
        this.onCompletion = onCompletion;
        this.writer = writer;
    }

    /**
     * Close this {@link StreamingResponse}, indicating that there will be no more fragments to send.
     */
    @Override
    public void close() {
        writeFragment(NO_MORE_FRAGMENTS, () -> {
            if (isRestResponseFinished.compareAndSet(false, true)) {
                queueRefs.decRef();
            }
        });
    }

    /**
     * Enqueue the given fragment for transmission.
     * @param fragment   The fragment to send.
     * @param releasable A resource which is released when the fragment has been completely processed, i.e. when
     *                   <ul>
     *                   <li>it has been fully sent, or</li>
     *                   <li>the overall response was cancelled before completion and all resources related to the partial transmission of
     *                   this fragment have been released.</li>
     *                   </ul>
     */
    public void writeFragment(Iterator<T> fragment, Releasable releasable) {
        if (tryAcquireQueueRef()) {
            try {
                fragmentQueue.add(new StreamingFragment<>(fragment, releasable));
                if (queueLength.getAndIncrement() == 0) {
                    // There is no active AvailableChunksZipResponseBodyPart, but there is now an entry in the queue, so we must create a
                    // AvailableChunksZipResponseBodyPart to process it (along with any other entries that are concurrently added to the
                    // queue). It's safe to mutate releasable and continuationListener here because they are only otherwise accessed by an
                    // active AvailableChunksZipResponseBodyPart (which does not exist) or when all queueRefs have been released (which they
                    // have not here).
                    final var nextFragment = fragmentQueue.poll();
                    assert nextFragment != null;
                    final var availableFragments = new AvailableFragmentsResponseBodyPart(nextFragment.fragment());
                    assert currentFragmentReleasable == null;
                    currentFragmentReleasable = nextFragment.releasable();
                    final var currentAvailableFragmentListener = nextAvailableFragmentListener;
                    nextAvailableFragmentListener = new SubscribableListener<>();
                    if (currentAvailableFragmentListener == null) {
                        // We are not resuming after a pause, this is the first fragment to be sent, so we start the response transmission.
                        restChannel.sendResponse(RestResponse.chunked(RestStatus.OK, availableFragments, this::restResponseFinished));
                    } else {
                        // We are resuming transmission after a pause, so just carry on sending the response body.
                        assert currentAvailableFragmentListener.isDone() == false;
                        currentAvailableFragmentListener.onResponse(availableFragments);
                    }
                }
            } finally {
                queueRefs.decRef();
            }
        } else {
            Releasables.closeExpectNoException(releasable);
        }
    }

    /**
     * A fragment which is ready for transmission, to be stored in {@link #fragmentQueue}.
     *
     * @param fragment      The fragment to send.
     * @param releasable    A resource to release when this fragment has been fully transmitted, or is no longer required because the
     *                      transmission was cancelled.
     */
    private record StreamingFragment<T>(Iterator<T> fragment, Releasable releasable) {}

    /**
     * Queue of fragments that are ready for transmission.
     */
    private final Queue<StreamingFragment<T>> fragmentQueue = new LinkedBlockingQueue<>();

    /**
     * Upper bound on the number of fragments in the queue, atomically modified to ensure there's only one thread processing the queue
     * at once.
     */
    private final AtomicInteger queueLength = new AtomicInteger();

    /**
     * Ref-counting for access to the queue, to avoid clearing the queue on abort concurrently with a fragment being sent.
     */
    private final RefCounted queueRefs = AbstractRefCounted.of(this::drainQueue);

    /**
     * Flag to indicate if the request has been aborted, at which point we should stop enqueueing more fragments and promptly clean up the
     * ones being sent. It's safe to ignore this, but without it in theory a constant stream of calls to {@link #writeFragment} could
     * prevent {@link #drainQueue} from running for arbitrarily long.
     */
    private final AtomicBoolean isRestResponseFinished = new AtomicBoolean();

    private boolean tryAcquireQueueRef() {
        return isRestResponseFinished.get() == false && queueRefs.tryIncRef();
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
        final var taskCount = queueLength.get() + 2 /* currentFragmentReleasable and onCompletion */ ;
        final var releasables = new ArrayList<Releasable>(taskCount);
        try {
            releasables.add(currentFragmentReleasable);
            currentFragmentReleasable = null;
            StreamingFragment<T> fragment;
            while ((fragment = fragmentQueue.poll()) != null) {
                releasables.add(fragment.releasable());
            }
            assert fragmentQueue.isEmpty() : fragmentQueue.size(); // no concurrent adds
            assert releasables.size() == taskCount - 1 || releasables.size() == taskCount - 2 : taskCount + " vs " + releasables.size();
        } finally {
            releasables.add(onCompletion);
            Releasables.closeExpectNoException(Releasables.wrap(releasables));
        }
    }

    /**
     * A {@link ChunkedRestResponseBodyPart} which will yield all currently-available fragments by consuming from {@link #fragmentQueue}.
     * There is only ever at most one active instance of this class at any time, in the sense that one such instance becoming inactive
     * <i>happens-before</i> the creation of the next instance. One of these parts may send chunks for more than one fragment.
     */
    private final class AvailableFragmentsResponseBodyPart implements ChunkedRestResponseBodyPart {

        /**
         * An iterator over the chunks of the fragment currently being transmitted.
         */
        private Iterator<T> fragmentChunksIterator;

        /**
         * True when we have run out of chunks ready for immediate transmission, so the response is paused, but we expect to send more data
         * later.
         */
        private boolean isResponsePaused;

        /**
         * True when we have sent the last chunk of the last fragment, or the response was cancelled.
         */
        private boolean isResponseComplete;

        /**
         * A listener which is created when there are no more available fragments, so transmission is paused, subscribed to in
         * {@link #getNextPart}, and then completed with the next body part (sequence of fragments, i.e. a new (unique) active
         * {@link AvailableFragmentsResponseBodyPart}).
         */
        private SubscribableListener<ChunkedRestResponseBodyPart> getNextPartListener;

        /**
         * A cache for an empty list to be used to collect the {@code Releasable} instances to be released when the next chunk has been
         * fully transmitted. It's a list because a call to {@link #encodeChunk} may yield a chunk that completes several fragments, each of
         * which has its own resources to release. We cache this value across chunks because most chunks won't release anything, so we can
         * keep the empty list around for later to save on allocations.
         */
        private ArrayList<Releasable> nextReleasablesCache = new ArrayList<>();

        AvailableFragmentsResponseBodyPart(Iterator<T> fragmentChunksIterator) {
            this.fragmentChunksIterator = fragmentChunksIterator;
        }

        /**
         * @return whether this part of the response is complete
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
         * Transfer {@link #currentFragmentReleasable} into the supplied collection (i.e. add it to {@code releasables} and then clear
         * {@link #currentFragmentReleasable}). Called when the last chunk of the current fragment is serialized, so that we
         * can start serializing chunks of the next fragment straight away whilst delaying the release of the current fragment's resources
         * until the transmission of the chunk that is currently under construction.
         */
        private void transferCurrentFragmentReleasable(ArrayList<Releasable> releasables) {
            assert queueRefs.hasReferences();

            if (currentFragmentReleasable == null) {
                return;
            }

            if (releasables == nextReleasablesCache) {
                // adding the first value, so we must line up a new cached value for the next caller
                nextReleasablesCache = new ArrayList<>();
            }

            releasables.add(currentFragmentReleasable);
            currentFragmentReleasable = null;
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
                        // This is the current unique active AvailableFragmentsResponseBodyPart (i.e. queueLength is strictly positive and
                        // we hold a queueRef), so any concurrent calls to writeFragment() at this point will just add to the queue and
                        // won't spawn a new AvailableFragmentsResponseBodyPart or mutate any fields.

                        final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);

                        do {
                            if (fragmentChunksIterator.hasNext()) {
                                writer.accept(fragmentChunksIterator.next(), chunkStream);
                            } else {
                                completeCurrentFragment(releasables);
                            }
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
            }
        }

        private void completeCurrentFragment(ArrayList<Releasable> releasables) throws IOException {
            transferCurrentFragmentReleasable(releasables);
            final var localNextAvailableFragmentListener = nextAvailableFragmentListener; // read before queue len decr
            final var newQueueLength = queueLength.decrementAndGet();
            if (fragmentChunksIterator == NO_MORE_FRAGMENTS) {
                isResponseComplete = true;
            } else if (newQueueLength == 0) {
                isResponsePaused = true;
                assert getNextPartListener == null;
                assert localNextAvailableFragmentListener != null;
                // Calling our getNextPart() will eventually yield the next fragment supplied to writeFragment():
                getNextPartListener = localNextAvailableFragmentListener;
            } else {
                // The current fragment is complete, and the next fragment is already available, so we start sending its
                // chunks too. This means we're still the unique active AvailableFragmentsResponseBodyPart. We re-use
                // this AvailableFragmentsResponseBodyPart instance rather than creating a new one to avoid unnecessary
                // allocations.

                final var nextFragment = fragmentQueue.poll();
                assert nextFragment != null;
                currentFragmentReleasable = nextFragment.releasable();
                fragmentChunksIterator = nextFragment.fragment();
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return responseContentType;
        }
    }

    /**
     * Sentinel fragment indicating the end of the response.
     */
    private final Iterator<T> NO_MORE_FRAGMENTS = new Iterator<>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            assert false : "not called";
            return null;
        }
    };
}
