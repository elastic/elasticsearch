/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.common.blobstore.transfer.stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.util.concurrent.RunOnce;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * OffsetRangeIndexInputStream extends InputStream to read from a specified offset using IndexInput
 *
 * @opensearch.internal
 */
public class OffsetRangeIndexInputStream extends OffsetRangeInputStream {
    private static final Logger logger = LogManager.getLogger(OffsetRangeIndexInputStream.class);
    private final InputStreamIndexInput inputStreamIndexInput;
    private final IndexInput indexInput;
    private AtomicBoolean readBlock;
    private final OffsetRangeRefCount offsetRangeRefCount;
    private final RunOnce closeOnce;

    /**
     * Construct a new OffsetRangeIndexInputStream object
     *
     * @param indexInput IndexInput opened on the file to read from
     * @param size The maximum length to read from specified <code>position</code>
     * @param position Position from where read needs to start
     * @throws IOException When <code>IndexInput#seek</code> operation fails
     */
    public OffsetRangeIndexInputStream(IndexInput indexInput, long size, long position) throws IOException {
        indexInput.seek(position);
        this.indexInput = indexInput;
        this.inputStreamIndexInput = new InputStreamIndexInput(indexInput, size);
        ClosingStreams closingStreams = new ClosingStreams(inputStreamIndexInput, indexInput);
        offsetRangeRefCount = new OffsetRangeRefCount(closingStreams);
        closeOnce = new RunOnce(offsetRangeRefCount::decRef);
    }

    public OffsetRangeIndexInputStream(IndexInput input, Object size, Object position,
                                       InputStreamIndexInput inputStreamIndexInput, IndexInput indexInput,
                                       OffsetRangeRefCount offsetRangeRefCount, RunOnce closeOnce) {
        super();
        this.inputStreamIndexInput = inputStreamIndexInput;
        this.indexInput = indexInput;
        this.offsetRangeRefCount = offsetRangeRefCount;
        this.closeOnce = closeOnce;
    }



    @Override
    public void setReadBlock(AtomicBoolean readBlock) {
        this.readBlock = readBlock;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        // There are two levels of check to ensure that we don't read an already closed stream and
        // to not close the stream if it is already being read.
        // 1. First check is a coarse-grained check outside reference check which allows us to fail fast if read
        // was invoked after the stream was closed. We need a separate atomic boolean closed because we don't want a
        // future read to succeed when #close has been invoked even if there are on-going reads. On-going reads would
        // hold reference and since ref count will not be 0 even after close was invoked, future reads will go through
        // without a check on closed. Also, we do need to set closed externally. It is shared across all streams of the
        // file. Check on closed in this class makes sure that no other stream allows subsequent reads. closed is
        // being set to true in RemoteTransferContainer#close which is invoked when we are done processing all
        // parts/file. Processing completes when either all parts are completed successfully or if either of the parts
        // failed. In successful case, subsequent read will anyway not go through since all streams would have been
        // consumed fully but in case of failure, SDK can continue to invoke read and this would be a wasted compute
        // and IO.
        // 2. In second check, a tryIncRef is invoked which tries to increment reference under lock and fails if ref
        // is already closed. If reference is successfully obtained by the stream then stream will not be closed.
        // Ref counting ensures that stream isn't closed in between reads.
        //
        // All these protection mechanisms are required in order to prevent invalid access to streams happening
        // from the new S3 async SDK.
        ensureReadable();
        OffsetRangeRefCount ignored = getStreamReference();
        return inputStreamIndexInput.read(b, off, len);

    }

    private OffsetRangeRefCount getStreamReference() {
        boolean successIncrement = offsetRangeRefCount.tryIncRef();
        if (successIncrement == false) {
            throw alreadyClosed("OffsetRangeIndexInputStream is already unreferenced.");
        }
        return offsetRangeRefCount;
    }

    @SuppressWarnings("checkstyle:DescendantToken")
    private void ensureReadable() {
        if (readBlock != null && readBlock.get() == true) {
            logger.debug("Read attempted on a stream which was read blocked!");
            throw alreadyClosed("Read blocked stream.");
        }
    }

    AlreadyClosedException alreadyClosed(String msg) {
        return new AlreadyClosedException(msg + this);
    }

    @Override
    public int read() throws IOException {
        ensureReadable();
        OffsetRangeRefCount ignored = getStreamReference();
        return inputStreamIndexInput.read();
    }

    @Override
    public boolean markSupported() {
        return inputStreamIndexInput.markSupported();
    }

    @Override
    public synchronized void mark(int readlimit) {
        inputStreamIndexInput.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        inputStreamIndexInput.reset();
    }

    @Override
    public long getFilePointer() throws IOException {
        return indexInput.getFilePointer();
    }

    @Override
    public String toString() {
        return "OffsetRangeIndexInputStream{" + "indexInput=" + indexInput + ", readBlock=" + readBlock + '}';
    }

    private static class ClosingStreams {
        private final InputStreamIndexInput inputStreamIndexInput;
        private final IndexInput indexInput;

        @SuppressWarnings("checkstyle:RedundantModifier")
        public ClosingStreams(InputStreamIndexInput inputStreamIndexInput, IndexInput indexInput) {
            this.inputStreamIndexInput = inputStreamIndexInput;
            this.indexInput = indexInput;
        }
    }

    private static class OffsetRangeRefCount extends ReleasableBytesReference.RefCountedReleasable<ClosingStreams> {
        private static final Logger logger = LogManager.getLogger(OffsetRangeRefCount.class);

        @SuppressWarnings("checkstyle:RedundantModifier")
        public OffsetRangeRefCount(ClosingStreams ref) {
            super("OffsetRangeRefCount", ref, () -> {
                try {
                    ref.inputStreamIndexInput.close();
                } catch (IOException ex) {
                    logger.error("Failed to close indexStreamIndexInput", ex);
                }
                try {
                    ref.indexInput.close();
                } catch (IOException ex) {
                    logger.error("Failed to close indexInput", ex);
                }
            });
        }
    }

    @Override
    public void close() throws IOException {
        closeOnce.run();
    }
}
