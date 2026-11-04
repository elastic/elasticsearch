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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * Prototype (fast activation): a small blob that the indexing shard (over)writes after each batched compound commit (BCC) upload and
 * that names the latest uploaded commit of the shard, so that a search shard can find and open that commit with a single object store
 * GET, without listing the shard's blobs, without reading BCC headers and without asking a resident indexing shard.
 * <p>
 * Location: {@code <shard path>/}{@link #blobName(long) commit_pointer.&lt;primary term&gt;}. There is one pointer per primary term
 * so that only the primary of that term ever writes it: a stale primary of an older term cannot overwrite the pointer of the current term,
 * and a search shard reads the pointer of the primary term it knows from the cluster state (never a newer one).
 * <p>
 * Content: the last {@link StatelessCompoundCommit} of the uploaded BCC (with the blob locations of all its files, possibly in older BCCs),
 * the BCC's term/generation, the term of the writer, the writer node id and the {@link BlobFileRanges} of the commit files whose header and
 * footer are replicated next to their compound commit header (so that a reader can warm them without reading any CC header). The
 * serialization reuses the transport serialization of these classes, prefixed with the transport version used, and is protected by a CRC32.
 * <p>
 * The pointer is written after the BCC it names is uploaded, so it never names a blob that was not uploaded.
 * <p>
 * Phase 4 (default off, see {@link CommitPointerOpenBytes}): the pointer can also carry the bytes that opening a reader on the commit
 * reads ({@link #openRanges}), so that the search shard opens its reader without object store reads. A pointer with open ranges is
 * written with format version 2; without, the format is unchanged (version 1).
 */
public record CommitPointer(
    long writerPrimaryTerm,
    String writerNodeId,
    PrimaryTermAndGeneration batchedCompoundCommitTermAndGeneration,
    StatelessCompoundCommit compoundCommit,
    Map<String, BlobFileRanges> replicatedFileRanges,
    List<CommitPointerOpenBytes.OpenRange> openRanges
) {

    public static final String BLOB_NAME_PREFIX = "commit_pointer.";
    private static final int MAGIC = 0x43505452; // "CPTR"
    private static final int FORMAT_VERSION = 1;
    /** Format version of a pointer that carries open ranges (phase 4). */
    private static final int FORMAT_VERSION_OPEN_RANGES = 2;
    private static final int MAX_POINTER_SIZE_IN_BYTES = 64 * 1024 * 1024;

    public CommitPointer {
        Objects.requireNonNull(writerNodeId);
        Objects.requireNonNull(batchedCompoundCommitTermAndGeneration);
        Objects.requireNonNull(compoundCommit);
        Objects.requireNonNull(replicatedFileRanges);
        Objects.requireNonNull(openRanges);
    }

    public CommitPointer(
        long writerPrimaryTerm,
        String writerNodeId,
        PrimaryTermAndGeneration batchedCompoundCommitTermAndGeneration,
        StatelessCompoundCommit compoundCommit,
        Map<String, BlobFileRanges> replicatedFileRanges
    ) {
        this(writerPrimaryTerm, writerNodeId, batchedCompoundCommitTermAndGeneration, compoundCommit, replicatedFileRanges, List.of());
    }

    /**
     * The same pointer with the given open-time bytes (see {@link CommitPointerOpenBytes}).
     */
    public CommitPointer withOpenRanges(List<CommitPointerOpenBytes.OpenRange> ranges) {
        return new CommitPointer(
            writerPrimaryTerm,
            writerNodeId,
            batchedCompoundCommitTermAndGeneration,
            compoundCommit,
            replicatedFileRanges,
            List.copyOf(ranges)
        );
    }

    /**
     * The file ranges of the commit as a search shard installs them from this pointer: the replicated ranges where the pointer has them,
     * the plain blob locations otherwise.
     */
    public Map<String, BlobFileRanges> installedFileRanges() {
        final Map<String, BlobFileRanges> result = new HashMap<>();
        for (var entry : compoundCommit.commitFiles().entrySet()) {
            final var replicated = replicatedFileRanges.get(entry.getKey());
            result.put(entry.getKey(), replicated != null ? replicated : new BlobFileRanges(entry.getValue()));
        }
        return result;
    }

    /** Total size of the open-time bytes carried by this pointer. */
    public long openRangesSizeInBytes() {
        long total = 0L;
        for (var range : openRanges) {
            total += range.bytes().length();
        }
        return total;
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
            payload.writeVInt(openRanges.isEmpty() ? FORMAT_VERSION : FORMAT_VERSION_OPEN_RANGES);
            TransportVersion.writeVersion(version, payload);
            payload.setTransportVersion(version);
            payload.writeVLong(writerPrimaryTerm);
            payload.writeString(writerNodeId);
            batchedCompoundCommitTermAndGeneration.writeTo(payload);
            compoundCommit.writeTo(payload);
            payload.writeMap(replicatedFileRanges, (o, v) -> v.writeTo(o));
            if (openRanges.isEmpty() == false) {
                payload.writeCollection(openRanges);
            }
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
            if (format != FORMAT_VERSION && format != FORMAT_VERSION_OPEN_RANGES) {
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
            final List<CommitPointerOpenBytes.OpenRange> openRanges = format == FORMAT_VERSION_OPEN_RANGES
                ? in.readCollectionAsImmutableList(CommitPointerOpenBytes.OpenRange::new)
                : List.of();
            if (in.available() > 0) {
                throw new IOException("unexpected trailing bytes in commit pointer");
            }
            return new CommitPointer(writerTerm, nodeId, bcc, commit, ranges, openRanges);
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

    /**
     * Maximum number of pointers of older primary terms that {@link #checkPredecessorPointers} reads. Beyond it the check gives up and the
     * caller falls back to object store discovery.
     */
    public static final int MAX_PREDECESSOR_POINTERS = 4;

    /**
     * Stale isolated primary check of pointer-based activation. The pointer of {@code knownPrimaryTerm} names {@code commit}. If that
     * commit is of an older term, a primary of a term in {@code [commit term, knownPrimaryTerm - 1]} may still be alive on an isolated
     * node and have uploaded a newer commit of its own term after the current primary recovered (hollow shards do not flush on recovery),
     * which the current primary does not know about. Such a primary also writes the pointer of its term after each upload, so reading
     * those pointers (usually 1 GET) finds the newer commit just like the object store LIST of the default recovery would (and with the
     * same race: a commit uploaded after the read is not seen).
     * <p>
     * A commit of {@code knownPrimaryTerm} needs no check: it is newer than any commit of an older term.
     *
     * @return {@code null} if no pointer of an older term names a newer commit, otherwise the reason not to use {@code commit} (a newer
     *         commit was found, a pointer of an older term is unreadable or invalid, or there are more than
     *         {@link #MAX_PREDECESSOR_POINTERS} terms to check)
     */
    @Nullable
    public static String checkPredecessorPointers(
        BlobContainer shardContainer,
        ShardId shardId,
        long knownPrimaryTerm,
        StatelessCompoundCommit commit
    ) {
        final PrimaryTermAndGeneration commitTermAndGen = commit.primaryTermAndGeneration();
        final long commitTerm = commitTermAndGen.primaryTerm();
        if (commitTerm >= knownPrimaryTerm) {
            return null;
        }
        if (knownPrimaryTerm - commitTerm > MAX_PREDECESSOR_POINTERS) {
            return "commit "
                + commitTermAndGen
                + " is more than "
                + MAX_PREDECESSOR_POINTERS
                + " terms older than term "
                + knownPrimaryTerm;
        }
        for (long term = knownPrimaryTerm - 1; term >= commitTerm; term--) {
            final CommitPointer predecessor;
            try {
                predecessor = read(shardContainer, term);
            } catch (Exception e) {
                return "commit pointer of older term [" + term + "] unreadable: " + e;
            }
            if (predecessor == null) {
                continue; // no primary of that term published a pointer
            }
            final String invalid = predecessor.validate(shardId, term);
            if (invalid != null) {
                return "commit pointer of older term [" + term + "] invalid: " + invalid;
            }
            final PrimaryTermAndGeneration predecessorCommit = predecessor.compoundCommit().primaryTermAndGeneration();
            if (predecessorCommit.after(commitTermAndGen)) {
                return "commit pointer of older term ["
                    + term
                    + "] names commit "
                    + predecessorCommit
                    + " newer than "
                    + commitTermAndGen
                    + " (stale primary?)";
            }
        }
        return null;
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
        if (openRanges.isEmpty() == false) {
            final Set<BlobFile> blobFiles = new HashSet<>();
            for (var location : compoundCommit.commitFiles().values()) {
                blobFiles.add(location.blobFile());
            }
            for (var range : openRanges) {
                if (blobFiles.contains(range.blobFile()) == false) {
                    return "open range of blob [" + range.blobFile() + "] not referenced by the commit";
                }
            }
        }
        return null;
    }
}
