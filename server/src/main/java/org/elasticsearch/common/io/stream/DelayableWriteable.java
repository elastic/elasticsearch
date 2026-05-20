/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A holder for {@link Writeable}s that delays reading the underlying object
 * on the receiving end. To be used for objects whose deserialized
 * representation is inefficient to keep in memory compared to their
 * corresponding serialized representation.
 * The node that produces the {@link Writeable} calls {@link #referencing(Writeable)}
 * to create a {@link DelayableWriteable} that serializes the inner object
 * first to a buffer and writes the content of the buffer to the {@link StreamOutput}.
 * The receiver node calls {@link #delayed(Reader, StreamInput)} to create a
 * {@link DelayableWriteable} that reads the buffer from the @link {@link StreamInput}
 * but delays creating the actual object by calling {@link #expand()} when needed.
 * Multiple {@link DelayableWriteable}s coming from different nodes may be buffered
 * on the receiver end, which may hold a mix of {@link DelayableWriteable}s that were
 * produced locally (hence expanded) as well as received form another node (hence subject
 * to delayed expansion). When such objects are buffered for some time it may be desirable
 * to force their buffering in serialized format by calling
 * {@link #asSerialized(Reader, NamedWriteableRegistry)}.
 */
public abstract class DelayableWriteable<T extends Writeable> implements Writeable, Releasable {

    private static final TransportVersion COMPRESS_DELAYABLE_WRITEABLE = TransportVersion.fromName("compress_delayable_writeable");
    private static final TransportVersion DELAYABLE_WRITEABLE_UNCOMPRESSED_SIZE = TransportVersion.fromName(
        "delayable_writeable_uncompressed_size"
    );

    /**
     * Sentinel for {@link Serialized#uncompressedSize} indicating that the uncompressed length was not transmitted by the peer
     * (i.e. {@link #COMPRESS_DELAYABLE_WRITEABLE} was supported but {@link #DELAYABLE_WRITEABLE_UNCOMPRESSED_SIZE} was not).
     * This value is never written to the wire.
     */
    private static final long UNKNOWN_UNCOMPRESSED_SIZE = -1L;

    /**
     * Build a {@linkplain DelayableWriteable} that wraps an existing object
     * but is serialized so that deserializing it can be delayed.
     */
    public static <T extends Writeable> DelayableWriteable<T> referencing(T reference) {
        return new Referencing<>(reference);
    }

    /**
     * Build a {@linkplain DelayableWriteable} that copies a buffer from
     * the provided {@linkplain StreamInput} and deserializes the buffer
     * when {@link #expand()} is called.
     */
    public static <T extends Writeable> DelayableWriteable<T> delayed(Writeable.Reader<T> reader, StreamInput in) throws IOException {
        final TransportVersion version = in.getTransportVersion();
        final ReleasableBytesReference serialized;
        final long uncompressedSize;
        if (version.supports(DELAYABLE_WRITEABLE_UNCOMPRESSED_SIZE)) {
            int compressedLen = in.readInt();
            uncompressedSize = in.readVLong();
            serialized = in.readReleasableBytesReference(compressedLen);
        } else if (version.supports(COMPRESS_DELAYABLE_WRITEABLE)) {
            int compressedLen = in.readInt();
            serialized = in.readReleasableBytesReference(compressedLen);
            uncompressedSize = UNKNOWN_UNCOMPRESSED_SIZE;
        } else {
            // legacy format: bytes are uncompressed, so the stored length is the uncompressed size.
            serialized = in.readReleasableBytesReference();
            uncompressedSize = serialized.length();
        }
        return new Serialized<>(reader, version, in.namedWriteableRegistry(), serialized, uncompressedSize);
    }

    public static <T extends Writeable> DelayableWriteable<T> referencing(Writeable.Reader<T> reader, StreamInput in) throws IOException {
        final TransportVersion version = in.getTransportVersion();
        final ReleasableBytesReference serialized;
        if (version.supports(DELAYABLE_WRITEABLE_UNCOMPRESSED_SIZE)) {
            int compressedLen = in.readInt();
            // Uncompressed length is discarded: the Referencing wrapper recomputes it on demand from the live object.
            in.readVLong();
            serialized = in.readReleasableBytesReference(compressedLen);
        } else if (version.supports(COMPRESS_DELAYABLE_WRITEABLE)) {
            serialized = in.readReleasableBytesReference(in.readInt());
        } else {
            serialized = in.readReleasableBytesReference();
        }
        try (serialized) {
            return new Referencing<>(deserialize(reader, version, in.namedWriteableRegistry(), serialized));
        }
    }

    protected DelayableWriteable() {}

    /**
     * Returns a {@linkplain DelayableWriteable} that stores its contents
     * in serialized form.
     */
    public abstract Serialized<T> asSerialized(Writeable.Reader<T> reader, NamedWriteableRegistry registry);

    /**
     * Expands the inner {@link Writeable} to its original representation and returns it
     */
    public abstract T expand();

    /**
     * {@code true} if the {@linkplain Writeable} is being stored in
     * serialized form, {@code false} otherwise.
     */
    public abstract boolean isSerialized();

    /**
     * Returns the uncompressed serialized size of the inner {@link Writeable}:
     * the byte count it would have if written to a plain {@link StreamOutput} without compression.
     */
    public abstract long getUncompressedSerializedSize();

    private static class Referencing<T extends Writeable> extends DelayableWriteable<T> {
        private final T reference;

        private Referencing(T reference) {
            this.reference = reference;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final TransportVersion version = out.getTransportVersion();
            if (version.supports(DELAYABLE_WRITEABLE_UNCOMPRESSED_SIZE)) {
                final long uncompressedSize = DelayableWriteable.getUncompressedSerializedSize(reference);
                final BytesStreamOutput tmp = new BytesStreamOutput();
                try (var compressor = CompressorFactory.COMPRESSOR.threadLocalStreamOutput(tmp)) {
                    compressor.setTransportVersion(version);
                    reference.writeTo(compressor);
                }
                final var bytes = tmp.bytes();
                out.writeInt(bytes.length());
                out.writeVLong(uncompressedSize);
                bytes.writeTo(out);
            } else if (version.supports(COMPRESS_DELAYABLE_WRITEABLE)) {
                out.writeWithSizePrefix(reference);
            } else {
                out.legacyWriteWithSizePrefix(reference);
            }
        }

        @Override
        public T expand() {
            return reference;
        }

        @Override
        public Serialized<T> asSerialized(Reader<T> reader, NamedWriteableRegistry registry) {
            // TODO: this path is currently not used in production code, if it ever is this should start using pooled buffers
            final long uncompressedSize = DelayableWriteable.getUncompressedSerializedSize(reference);
            BytesStreamOutput buffer = new BytesStreamOutput();
            try (var out = CompressorFactory.COMPRESSOR.threadLocalStreamOutput(buffer)) {
                out.setTransportVersion(TransportVersion.current());
                reference.writeTo(out);
            } catch (IOException e) {
                throw new RuntimeException("unexpected error writing writeable to buffer", e);
            }
            return new Serialized<>(
                reader,
                TransportVersion.current(),
                registry,
                ReleasableBytesReference.wrap(buffer.bytes()),
                uncompressedSize
            );
        }

        @Override
        public boolean isSerialized() {
            return false;
        }

        @Override
        public long getUncompressedSerializedSize() {
            return DelayableWriteable.getUncompressedSerializedSize(reference);
        }

        @Override
        public void close() {
            // noop
        }
    }

    /**
     * A {@link Writeable} stored in serialized form backed by a {@link ReleasableBytesReference}. Once an instance is no longer used its
     * backing memory must be manually released by invoking {@link #close()} on it.
     */
    public static class Serialized<T extends Writeable> extends DelayableWriteable<T> {
        private final Writeable.Reader<T> reader;
        private final TransportVersion serializedAtVersion;
        private final NamedWriteableRegistry registry;
        private final ReleasableBytesReference serialized;
        // Set when known: size the bytes would occupy if written without compression. Negative when the
        // bytes were received from a peer that did not transmit the uncompressed size on the wire.
        // TODO: can be final once UNKNOWN_UNCOMPRESSED_SIZE is no longer needed (i.e. all supported versions transmit the size).
        private long uncompressedSize;

        private Serialized(
            Writeable.Reader<T> reader,
            TransportVersion serializedAtVersion,
            NamedWriteableRegistry registry,
            ReleasableBytesReference serialized,
            long uncompressedSize
        ) {
            this.reader = reader;
            this.serializedAtVersion = serializedAtVersion;
            this.registry = registry;
            this.serialized = serialized;
            this.uncompressedSize = uncompressedSize;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion() == serializedAtVersion) {
                /*
                 * If the version *does* line up we can just copy the bytes
                 * which is good because this is how shard request caching
                 * works.
                 */
                if (out.getTransportVersion().supports(DELAYABLE_WRITEABLE_UNCOMPRESSED_SIZE)) {
                    out.writeInt(serialized.length());
                    // Lazy accessor: covers the case where the source peer did not transmit the uncompressed size.
                    out.writeVLong(getUncompressedSerializedSize());
                    serialized.writeTo(out);
                } else if (out.getTransportVersion().supports(COMPRESS_DELAYABLE_WRITEABLE)) {
                    out.writeInt(serialized.length());
                    serialized.writeTo(out);
                } else {
                    out.writeBytesReference(serialized);
                }
            } else {
                /*
                 * If the version doesn't line up then we have to deserialize
                 * into the Writeable and re-serialize it against the new
                 * output stream so it can apply any backwards compatibility
                 * differences in the wire protocol. This ain't efficient but
                 * it should be quite rare.
                 */
                referencing(expand()).writeTo(out);
            }
        }

        @Override
        public T expand() {
            try {
                return deserialize(reader, serializedAtVersion, registry, serialized);
            } catch (IOException e) {
                throw new RuntimeException("unexpected error expanding serialized delayed writeable", e);
            }
        }

        @Override
        public Serialized<T> asSerialized(Reader<T> reader, NamedWriteableRegistry registry) {
            return this; // We're already serialized
        }

        /**
         * Returns the {@link TransportVersion} at which the serialized bytes were written.
         * Used by callers that need to detect a version mismatch before delegating to
         * {@link #writeTo} so they can handle lifecycle (e.g. release pooled resources) after
         * expanding.
         */
        public TransportVersion getSerializedAtVersion() {
            return serializedAtVersion;
        }

        @Override
        public boolean isSerialized() {
            return true;
        }

        @Override
        public long getUncompressedSerializedSize() {
            if (uncompressedSize < 0) {
                // Peer did not transmit the uncompressed length (mid-era between COMPRESS_DELAYABLE_WRITEABLE
                // and DELAYABLE_WRITEABLE_UNCOMPRESSED_SIZE). Compute it once by streaming through the
                // decompressor and caching the result.
                uncompressedSize = computeUncompressedSize();
            }
            return uncompressedSize;
        }

        /**
         * Used only during cluster upgrades, when the old cluster doesn't send the original size back.
         * Decompresses the serialized bytes into a null sink to count the raw uncompressed byte count.
         */
        private long computeUncompressedSize() {
            if (serializedAtVersion.supports(COMPRESS_DELAYABLE_WRITEABLE) == false) {
                return serialized.length();
            }
            try (
                InputStream in = CompressorFactory.COMPRESSOR.threadLocalStreamInput(serialized.streamInput())
            ) {
                return in.transferTo(OutputStream.nullOutputStream());
            } catch (IOException e) {
                throw new UncheckedIOException("failed to compute uncompressed size of serialized DelayableWriteable", e);
            }
        }

        @Override
        public void close() {
            serialized.close();
        }

    }

    /**
     * Returns the uncompressed serialized size in bytes of the provided {@link Writeable}, i.e. the byte count it
     * would have if written to a plain {@link StreamOutput} without compression. Use this for memory accounting
     * around aggregation reduction; see {@link #getUncompressedSerializedSize()} for the instance-level method.
     */
    public static long getUncompressedSerializedSize(Writeable ref) {
        try (CountingStreamOutput out = new CountingStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            ref.writeTo(out);
            return out.position();
        } catch (IOException exc) {
            throw new UncheckedIOException(exc);
        }
    }

    private static <T> T deserialize(
        Reader<T> reader,
        TransportVersion serializedAtVersion,
        NamedWriteableRegistry registry,
        BytesReference serialized
    ) throws IOException {
        try (
            StreamInput in = serializedAtVersion.supports(COMPRESS_DELAYABLE_WRITEABLE)
                ? CompressorFactory.COMPRESSOR.threadLocalStreamInput(serialized.streamInput())
                : serialized.streamInput()
        ) {
            return reader.read(wrapWithDeduplicatorStreamInput(in, serializedAtVersion, registry));
        }
    }

    /** Wraps the provided {@link StreamInput} with another stream that extends {@link Deduplicator} */
    public static StreamInput wrapWithDeduplicatorStreamInput(
        StreamInput in,
        TransportVersion serializedAtVersion,
        @Nullable NamedWriteableRegistry registry
    ) {
        StreamInput out = registry == null
            ? new DeduplicateStreamInput(in, new DeduplicatorCache())
            : new DeduplicateNamedWriteableAwareStreamInput(in, registry, new DeduplicatorCache());
        out.setTransportVersion(serializedAtVersion);
        return out;
    }

    /** An object implementing this interface can deduplicate instance of the provided objects.*/
    public interface Deduplicator {
        <T> T deduplicate(T object);
    }

    private static class DeduplicateStreamInput extends FilterStreamInput implements Deduplicator {

        private final Deduplicator deduplicator;

        private DeduplicateStreamInput(StreamInput delegate, Deduplicator deduplicator) {
            super(delegate);
            this.deduplicator = deduplicator;
        }

        @Override
        public <T> T deduplicate(T object) {
            return deduplicator.deduplicate(object);
        }
    }

    private static class DeduplicateNamedWriteableAwareStreamInput extends NamedWriteableAwareStreamInput implements Deduplicator {

        private final Deduplicator deduplicator;

        private DeduplicateNamedWriteableAwareStreamInput(
            StreamInput delegate,
            NamedWriteableRegistry registry,
            Deduplicator deduplicator
        ) {
            super(delegate, registry);
            this.deduplicator = deduplicator;
        }

        @Override
        public <T> T deduplicate(T object) {
            return deduplicator.deduplicate(object);
        }
    }

    /**
     * Implementation of a {@link Deduplicator} cache. It can hold up to 1024 instances.
     */
    private static class DeduplicatorCache implements Deduplicator {

        private static final int MAX_SIZE = 1024;
        // lazily init
        private Map<Object, Object> cache = null;

        @SuppressWarnings("unchecked")
        @Override
        public <T> T deduplicate(T object) {
            if (cache == null) {
                cache = new HashMap<>();
                cache.put(object, object);
            } else if (cache.size() < MAX_SIZE) {
                object = (T) cache.computeIfAbsent(object, o -> o);
            }
            return object;
        }
    }
}
