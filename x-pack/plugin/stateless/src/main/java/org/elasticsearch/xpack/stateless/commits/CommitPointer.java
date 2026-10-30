/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Prototype ("cheap shards"): a small blob that the indexing shard (over)writes after each batched compound commit (BCC) upload and
 * that names the latest uploaded commit of the shard, so that a search shard can find and open that commit with a single object store
 * GET, without listing the shard's blobs, without reading BCC headers and without asking a resident indexing shard.
 * <p>
 * Location: {@code <shard path>/}{@link #blobName(long) cheap_shard_commit_pointer.&lt;primary term&gt;}. There is one pointer per primary term
 * so that only the primary of that term ever writes it: a stale primary of an older term cannot overwrite the pointer of the current term,
 * and a search shard reads the pointer of the primary term it knows from the cluster state (never a newer one).
 * <p>
 * Content: the last {@link StatelessCompoundCommit} of the uploaded BCC (with the blob locations of all its files, possibly in older BCCs),
 * the BCC's term/generation, the term of the writer, the writer node id and the {@link BlobFileRanges} of the commit files whose header and
 * footer are replicated next to their compound commit header (so that a reader can warm them without reading any CC header). The
 * serialization reuses the transport serialization of these classes, prefixed with the transport version used, and is protected by a CRC32.
 * <p>
 * The pointer is written after the BCC it names is uploaded, so it never names a blob that was not uploaded.
 */
public record CommitPointer(
    long writerPrimaryTerm,
    String writerNodeId,
    PrimaryTermAndGeneration batchedCompoundCommitTermAndGeneration,
    StatelessCompoundCommit compoundCommit,
    Map<String, BlobFileRanges> replicatedFileRanges
) {

    public static final String BLOB_NAME_PREFIX = "cheap_shard_commit_pointer.";
    private static final int MAGIC = 0x43505452; // "CPTR"
    private static final int FORMAT_VERSION = 1;
    private static final int MAX_POINTER_SIZE_IN_BYTES = 64 * 1024 * 1024;

    public CommitPointer {
        Objects.requireNonNull(writerNodeId);
        Objects.requireNonNull(batchedCompoundCommitTermAndGeneration);
        Objects.requireNonNull(compoundCommit);
        Objects.requireNonNull(replicatedFileRanges);
    }

    public static String blobName(long primaryTerm) {
        return BLOB_NAME_PREFIX + primaryTerm;
    }

    /**
     * Builds the pointer of the last compound commit of an uploaded BCC. {@code knownFileRanges} are the file ranges known by the writer
     * (with replicated header/footer ranges); only those with replicated ranges and the same blob location as the commit file are kept.
     */
    public static CommitPointer create(
        long writerPrimaryTerm,
        String writerNodeId,
        BatchedCompoundCommit uploadedBcc,
        Map<String, BlobFileRanges> knownFileRanges
    ) {
        final StatelessCompoundCommit commit = uploadedBcc.lastCompoundCommit();
        final Map<String, BlobFileRanges> ranges = new HashMap<>();
        for (var entry : commit.commitFiles().entrySet()) {
            var known = knownFileRanges.get(entry.getKey());
            if (known != null && known.hasReplicatedRanges() && known.blobLocation().equals(entry.getValue())) {
                ranges.put(entry.getKey(), known);
            }
        }
        return new CommitPointer(writerPrimaryTerm, writerNodeId, uploadedBcc.primaryTermAndGeneration(), commit, Map.copyOf(ranges));
    }

    /**
     * Computes the {@link BlobFileRanges} (with replicated header/footer ranges when present) of the internal files of every compound
     * commit of the given BCC, i.e. what a reader would otherwise compute after reading the CC headers.
     */
    public static Map<String, BlobFileRanges> computeReplicatedFileRanges(BatchedCompoundCommit bcc) {
        final Map<String, BlobFileRanges> result = new HashMap<>();
        long headerOffset = 0L;
        final var commits = bcc.compoundCommits();
        for (int i = 0; i < commits.size(); i++) {
            final var cc = commits.get(i);
            result.putAll(BlobFileRanges.computeBlobFileRanges(true, cc, headerOffset, cc.internalFiles()));
            headerOffset += org.elasticsearch.blobcache.BlobCacheUtils.toPageAlignedSize(cc.sizeInBytes());
        }
        return result;
    }

    public BytesReference toBytes() throws IOException {
        try (BytesStreamOutput payload = new BytesStreamOutput()) {
            final TransportVersion version = TransportVersion.current();
            payload.writeInt(MAGIC);
            payload.writeVInt(FORMAT_VERSION);
            TransportVersion.writeVersion(version, payload);
            payload.setTransportVersion(version);
            payload.writeVLong(writerPrimaryTerm);
            payload.writeString(writerNodeId);
            batchedCompoundCommitTermAndGeneration.writeTo(payload);
            compoundCommit.writeTo(payload);
            payload.writeMap(replicatedFileRanges, (o, v) -> v.writeTo(o));
            final BytesReference bytes = payload.bytes();
            final CRC32 crc = new CRC32();
            final byte[] array = BytesReference.toBytes(bytes);
            crc.update(array);
            try (BytesStreamOutput out = new BytesStreamOutput(array.length + Long.BYTES)) {
                out.writeBytes(array);
                out.writeLong(crc.getValue());
                return out.copyBytes();
            }
        }
    }

    public static CommitPointer fromBytes(BytesReference bytes) throws IOException {
        if (bytes.length() < Long.BYTES + Integer.BYTES) {
            throw new IOException("commit pointer too short: " + bytes.length());
        }
        final byte[] array = BytesReference.toBytes(bytes);
        final int payloadLength = array.length - Long.BYTES;
        final CRC32 crc = new CRC32();
        crc.update(array, 0, payloadLength);
        final long expected = new BytesArray(array, payloadLength, Long.BYTES).streamInput().readLong();
        if (crc.getValue() != expected) {
            throw new IOException("commit pointer checksum mismatch");
        }
        try (StreamInput in = new BytesArray(array, 0, payloadLength).streamInput()) {
            if (in.readInt() != MAGIC) {
                throw new IOException("not a commit pointer");
            }
            final int format = in.readVInt();
            if (format != FORMAT_VERSION) {
                throw new IOException("unsupported commit pointer format [" + format + "]");
            }
            final TransportVersion version = TransportVersion.readVersion(in);
            if (version.isKnown() == false) {
                throw new IOException("commit pointer written with an unknown transport version [" + version + "]");
            }
            in.setTransportVersion(version);
            final long writerTerm = in.readVLong();
            final String nodeId = in.readString();
            final PrimaryTermAndGeneration bcc = new PrimaryTermAndGeneration(in);
            final StatelessCompoundCommit commit = StatelessCompoundCommit.readFromTransport(in);
            final Map<String, BlobFileRanges> ranges = in.readImmutableMap(BlobFileRanges::new);
            if (in.available() > 0) {
                throw new IOException("unexpected trailing bytes in commit pointer");
            }
            return new CommitPointer(writerTerm, nodeId, bcc, commit, ranges);
        }
    }

    /**
     * Reads the pointer of the given primary term with one GET. Returns {@code null} if there is none.
     */
    @Nullable
    public static CommitPointer read(BlobContainer shardContainer, long primaryTerm) throws IOException {
        final byte[] bytes;
        try (InputStream in = shardContainer.readBlob(OperationPurpose.INDICES, blobName(primaryTerm))) {
            bytes = BytesReference.toBytes(Streams.readFully(Streams.limitStream(in, MAX_POINTER_SIZE_IN_BYTES + 1L)));
        } catch (NoSuchFileException e) {
            return null;
        }
        if (bytes.length > MAX_POINTER_SIZE_IN_BYTES) {
            throw new IOException("commit pointer too large");
        }
        return fromBytes(new BytesArray(bytes));
    }

    public void write(BlobContainer shardContainer) throws IOException {
        assert writerPrimaryTerm > 0 : writerPrimaryTerm;
        shardContainer.writeBlobAtomic(OperationPurpose.INDICES, blobName(writerPrimaryTerm), toBytes(), false);
    }

    /**
     * Validates the pointer against what the reader knows from the cluster state. Returns {@code null} if the pointer can be used,
     * otherwise the reason why it cannot.
     */
    @Nullable
    public String validate(ShardId shardId, long knownPrimaryTerm) {
        if (writerPrimaryTerm != knownPrimaryTerm) {
            return "pointer written in term [" + writerPrimaryTerm + "] but the shard's primary term is [" + knownPrimaryTerm + "]";
        }
        if (compoundCommit.shardId().equals(shardId) == false
            || compoundCommit.shardId().getIndex().getUUID().equals(shardId.getIndex().getUUID()) == false) {
            return "pointer of shard " + compoundCommit.shardId() + " read for shard " + shardId;
        }
        final var cc = compoundCommit.primaryTermAndGeneration();
        if (cc.primaryTerm() > knownPrimaryTerm || batchedCompoundCommitTermAndGeneration.primaryTerm() > knownPrimaryTerm) {
            return "pointer names commit "
                + cc
                + " in BCC "
                + batchedCompoundCommitTermAndGeneration
                + " newer than term "
                + knownPrimaryTerm;
        }
        if (compoundCommit.internalFiles().isEmpty() == false
            && compoundCommit.getContainingBccBlobFile().termAndGeneration().equals(batchedCompoundCommitTermAndGeneration) == false) {
            return "commit " + cc + " is not in BCC " + batchedCompoundCommitTermAndGeneration;
        }
        for (var location : compoundCommit.commitFiles().values()) {
            if (location.getBatchedCompoundCommitTermAndGeneration().after(batchedCompoundCommitTermAndGeneration)) {
                return "commit file in " + location.getBatchedCompoundCommitTermAndGeneration() + " after the pointer's BCC";
            }
        }
        for (var entry : replicatedFileRanges.entrySet()) {
            if (entry.getValue().blobLocation().equals(compoundCommit.commitFiles().get(entry.getKey())) == false) {
                return "file ranges of [" + entry.getKey() + "] do not match the commit";
            }
        }
        return null;
    }
}
