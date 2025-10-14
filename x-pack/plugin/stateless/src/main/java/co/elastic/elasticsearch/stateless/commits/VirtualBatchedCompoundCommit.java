/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.InternalFile;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.stateless.commits.ReplicatedContent.ALWAYS_REPLICATE;
import static co.elastic.elasticsearch.stateless.commits.StatelessCommitService.isGenerationalFile;
import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.CURRENT_VERSION;
import static java.util.stream.Collectors.groupingBy;
import static org.elasticsearch.common.io.Streams.limitStream;

/**
 * Represents a collection of non-uploaded compound commits, where multiple commits can be added and read,
 * ensuring they will all be uploaded as a single blob with fixed offsets within the final batched compound commit.
 *
 * <p>
 *     This class uses ref-counting to ensure that readers can have access to the underlying Lucene segments once
 *     they've acquired a reference through {@link #incRef()}. The acquired reference remains valid until it is
 *     released using the {@link #decRef()} method.
 * </p>
 *
 * <p>
 *     It is expected that after the batched compound commit is written to the store using the
 *     {@link #getFrozenInputStreamForUpload()} method, the caller should promptly invoke {@link #close()} on the input stream and the VBCC
 *     instance. This action releases the acquired Lucene commit reference and facilitates the proper release of associated resources.
 * </p>
 *
 * <p>
 *     This class facilitates the appending of multiple compound commits via {@link #appendCommit(StatelessCommitRef, boolean)}.
 *     When the caller intends to write these commits to the blob store it should use {@link #getFrozenInputStreamForUpload()}.
 * </p>
 *
 * */
public class VirtualBatchedCompoundCommit extends AbstractRefCounted implements Closeable, AbstractBatchedCompoundCommit {

    private static final Logger logger = LogManager.getLogger(VirtualBatchedCompoundCommit.class);

    private static final Logger LOG_TIME_SPENT_READING_DURING_UPLOAD = LogManager.getLogger(
        VirtualBatchedCompoundCommit.class.getCanonicalName() + ".time_spent_reading_during_upload"
    );

    private final ShardId shardId;
    private final String nodeEphemeralId;
    private final Function<String, BlobLocation> uploadedBlobLocationsSupplier;
    private final NavigableSet<PendingCompoundCommit> pendingCompoundCommits;
    // TODO: the internal files should be added to the corresponding BlobReferences
    private final Map<String, BlobLocation> internalLocations = new ConcurrentHashMap<>();
    // Maps internal data (pending compound commits' headers, files, padding) to their offset in the virtual batched compound commit
    private final NavigableMap<Long, InternalDataReader> internalDataReadersByOffset = new ConcurrentSkipListMap<>();
    private final AtomicLong currentOffset = new AtomicLong();
    private final AtomicReference<Thread> appendingCommitThread = new AtomicReference<>();
    private final BlobFile blobFile;
    private final PrimaryTermAndGeneration primaryTermAndGeneration;
    private final long creationTimeInMillis;
    // VBCC can no longer be appended to once it is frozen
    private volatile boolean frozen = false;

    // Tracks search nodes notified that the non-uploaded VBCC's commits are available from the index node.
    // Search shards may move to new search nodes before the commits are uploaded and tracking in the BlobReference begins.
    // So tracking begins here before a BlobReference is created.
    private final Set<String> notifiedSearchNodeIds;

    // Size of a region in cache
    private final int cacheRegionSizeInBytes;

    // An estimate of the maximum size of a header in a cache region.
    // This is used to avoid adding replicated content for files that are already included in the first region.
    private final int estimatedMaxHeaderSizeInBytes;

    public VirtualBatchedCompoundCommit(
        ShardId shardId,
        String nodeEphemeralId,
        long primaryTerm,
        long generation,
        Function<String, BlobLocation> uploadedBlobLocationsSupplier,
        LongSupplier timeInMillisSupplier,
        int cacheRegionSize,
        int estimatedMaxHeaderSizeInBytes

    ) {
        this.shardId = shardId;
        this.nodeEphemeralId = nodeEphemeralId;
        this.uploadedBlobLocationsSupplier = uploadedBlobLocationsSupplier;
        this.pendingCompoundCommits = new ConcurrentSkipListSet<>();
        this.primaryTermAndGeneration = new PrimaryTermAndGeneration(primaryTerm, generation);
        this.blobFile = new BlobFile(StatelessCompoundCommit.blobNameFromGeneration(generation), primaryTermAndGeneration);
        this.creationTimeInMillis = timeInMillisSupplier.getAsLong();
        this.notifiedSearchNodeIds = ConcurrentCollections.newConcurrentSet();
        this.cacheRegionSizeInBytes = cacheRegionSize;
        if (estimatedMaxHeaderSizeInBytes < 0 || cacheRegionSizeInBytes < estimatedMaxHeaderSizeInBytes) {
            throw new IllegalArgumentException(
                "Must be 0.0 to " + cacheRegionSizeInBytes + " inclusive but got " + estimatedMaxHeaderSizeInBytes
            );
        }
        this.estimatedMaxHeaderSizeInBytes = estimatedMaxHeaderSizeInBytes;
    }

    public void addNotifiedSearchNodeIds(Collection<String> nodeIds) {
        assert frozen == false : "Unable to add notified search nodes ids after the VBCC is finalized";
        notifiedSearchNodeIds.addAll(nodeIds);
    }

    public Set<String> getNotifiedSearchNodeIds() {
        assert frozen : "Accessing the notified search node id list before the VBCC is finalized";
        return Collections.unmodifiableSet(notifiedSearchNodeIds);
    }

    /**
     * Freeze the VBCC so that no more CC can be appended. The VBCC is guaranteed to be frozen afterwards.
     * No synchronization is needed for this method because its sole caller is itself synchronized
     * @return {@code true} if the VBCC is frozen by this thread or
     * {@code false} if it is already frozen or concurrently frozen by other threads.
     */
    public boolean freeze() {
        assert assertCompareAndSetFreezeOrAppendingCommitThread(null, Thread.currentThread());
        try {
            assert pendingCompoundCommits.isEmpty() == false : "Cannot freeze an empty virtual batch compound commit";
            if (isFrozen()) {
                return false;
            }
            frozen = true;
            logger.debug("VBCC is successfully frozen");
            return true;
        } finally {
            assert assertCompareAndSetFreezeOrAppendingCommitThread(Thread.currentThread(), null);
        }
    }

    /**
     * Add the specified {@link StatelessCommitRef} as {@link PendingCompoundCommit}
     * No synchronization is needed for this method because its sole caller is itself synchronized
     * @return {@code true} if the append is successful or {@code false} if the VBCC is frozen and cannot be appended to
     */
    public boolean appendCommit(StatelessCommitRef reference, boolean useInternalFilesReplicatedContent) {
        assert assertCompareAndSetFreezeOrAppendingCommitThread(null, Thread.currentThread());
        try {
            return doAppendCommit(reference, useInternalFilesReplicatedContent);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Unable to append commit [" + reference.getPrimaryTerm() + ", " + reference.getGeneration() + "]",
                e
            );
        } catch (Throwable e) {
            // This on purpose catches Throwables, to log potential assertions that may be otherwise masked by test suite timeouts.
            logger.warn(shardId + " throwable while appending [" + reference.getPrimaryTerm() + ", " + reference.getGeneration() + "]", e);
            throw e;
        } finally {
            assert assertCompareAndSetFreezeOrAppendingCommitThread(Thread.currentThread(), null);
        }
    }

    public boolean isFrozen() {
        return frozen;
    }

    private boolean doAppendCommit(StatelessCommitRef reference, boolean useInternalFilesReplicatedContent) throws IOException {
        assert primaryTermAndGeneration.primaryTerm() == reference.getPrimaryTerm();
        assert (pendingCompoundCommits.isEmpty() && primaryTermAndGeneration.generation() == reference.getGeneration())
            || (pendingCompoundCommits.isEmpty() == false && primaryTermAndGeneration.generation() < reference.getGeneration());
        assert pendingCompoundCommits.isEmpty() || pendingCompoundCommits.last().getGeneration() < reference.getGeneration();

        // bail early if VBCC is already frozen to avoid doing any work
        if (isFrozen()) {
            return false;
        }

        final var ccTermAndGen = new PrimaryTermAndGeneration(reference.getPrimaryTerm(), reference.getGeneration());
        final boolean isFirstCommit = ccTermAndGen.equals(primaryTermAndGeneration);

        // Ordered set of compound commit (CC) internal files
        var internalFiles = new TreeSet<InternalFile>();

        // Map of compound commit (CC) referenced files
        var referencedFiles = new HashMap<String, BlobLocation>();

        var internalFilesSize = 0L;
        for (String commitFile : reference.getCommitFiles()) {
            boolean isAdditionalFile = reference.getAdditionalFiles().contains(commitFile);
            if (isAdditionalFile || (isFirstCommit && isGenerationalFile(commitFile))) {
                assert internalLocations.containsKey(commitFile) == false : commitFile;
                var fileLength = reference.getDirectory().fileLength(commitFile);
                internalFiles.add(new InternalFile(commitFile, fileLength));
                internalFilesSize += fileLength;
            } else {
                var blobLocation = internalLocations.get(commitFile);
                assert blobLocation != null || isGenerationalFile(commitFile) == false : commitFile;
                if (blobLocation == null) {
                    blobLocation = uploadedBlobLocationsSupplier.apply(commitFile);
                    assert blobLocation != null : commitFile;
                    assert blobLocation.getBatchedCompoundCommitTermAndGeneration().before(primaryTermAndGeneration);
                }
                referencedFiles.put(commitFile, blobLocation);
            }
        }

        var replicatedContent = useInternalFilesReplicatedContent
            ? getReplicatedContent(ccTermAndGen, currentOffset.get(), internalFiles, internalFilesSize, reference.getDirectory())
            : ReplicatedContent.EMPTY;
        var replicatedContentHeader = replicatedContent.header();

        // We replicate referenced .si files into hollow commits as extra content appended after the internal files in the compound commit.
        final List<InternalFile> extraContentFiles = reference.isHollow() ? new ArrayList<>() : List.of();
        var extraContentSize = 0L;
        if (reference.isHollow()) {
            for (var entry : referencedFiles.entrySet()) {
                if (Objects.equals(IndexFileNames.getExtension(entry.getKey()), LuceneFilesExtensions.SI.getExtension())) {
                    extraContentFiles.add(new InternalFile(entry.getKey(), entry.getValue().fileLength()));
                    extraContentSize += entry.getValue().fileLength();
                }
            }
        }

        var header = materializeCompoundCommitHeader(
            reference,
            internalFiles,
            replicatedContentHeader,
            referencedFiles,
            useInternalFilesReplicatedContent,
            extraContentFiles
        );

        final long sizeInBytes = header.length + replicatedContentHeader.dataSizeInBytes() + internalFilesSize + extraContentSize;
        if (logger.isDebugEnabled()) {
            var referencedBlobs = referencedFiles.values().stream().map(location -> location.blobFile().blobName()).distinct().count();
            logger.debug(
                """
                    {}{} appending commit ({} bytes). References external {} files in {} other CCs and adds
                    {} internal: {}, and
                    {} extra content files: {}.""",
                shardId,
                primaryTermAndGeneration,
                sizeInBytes,
                referencedFiles.size(),
                referencedBlobs,
                internalFiles.size(),
                internalFiles,
                extraContentFiles.size(),
                extraContentFiles
            );
        }

        // Add padding to the previous CC if it exists
        if (pendingCompoundCommits.isEmpty() == false) {
            var lastCompoundCommit = pendingCompoundCommits.last();
            long lastCompoundCommitSize = lastCompoundCommit.getSizeInBytes();
            long lastCompoundCommitSizePageAligned = BlobCacheUtils.toPageAlignedSize(lastCompoundCommitSize);
            int padding = Math.toIntExact(lastCompoundCommitSizePageAligned - lastCompoundCommitSize);
            if (padding > 0) {
                lastCompoundCommit.setPadding(padding);
                long paddingOffset = currentOffset.get();
                var previousPaddingOffset = internalDataReadersByOffset.put(paddingOffset, new InternalPaddingReader(padding));
                assert previousPaddingOffset == null;
                currentOffset.set(paddingOffset + padding);
            }
        }

        final long headerOffset = currentOffset.get();
        assert headerOffset == BlobCacheUtils.toPageAlignedSize(headerOffset) : "header offset is not page-aligned: " + headerOffset;
        var previousHeaderOffset = internalDataReadersByOffset.put(headerOffset, new InternalHeaderReader(header));
        assert previousHeaderOffset == null;

        long replicatedContentOffset = headerOffset + header.length;
        for (var replicatedRangeReader : replicatedContent.readers()) {
            var previousReplicatedContent = internalDataReadersByOffset.put(replicatedContentOffset, replicatedRangeReader);
            assert previousReplicatedContent == null;
            replicatedContentOffset += replicatedRangeReader.rangeLength();
        }
        assert replicatedContentOffset == headerOffset + header.length + replicatedContentHeader.dataSizeInBytes();

        long fileOffset = headerOffset + header.length + replicatedContentHeader.dataSizeInBytes();

        // Map of all compound commit (CC) files with their internal or referenced blob location
        final var commitFiles = new HashMap<>(referencedFiles);

        for (var internalFile : internalFiles) {
            var fileLength = internalFile.length();
            var blobLocation = new BlobLocation(blobFile, fileOffset, fileLength);

            var previousFile = commitFiles.put(internalFile.name(), blobLocation);
            assert previousFile == null : internalFile.name();

            var previousLocation = internalLocations.put(internalFile.name(), blobLocation);
            assert previousLocation == null : internalFile.name();

            var previousOffset = internalDataReadersByOffset.put(
                fileOffset,
                new InternalFileReader(internalFile.name(), reference.getDirectory())
            );
            assert previousOffset == null : internalFile.name();
            fileOffset += fileLength;
        }
        currentOffset.set(fileOffset);

        // Extra content files
        final Map<String, BlobLocation> extraContent = extraContentFiles.isEmpty()
            ? Map.of()
            : Maps.newHashMapWithExpectedSize(extraContentFiles.size());
        for (var extraFile : extraContentFiles) {
            var fileLength = extraFile.length();
            var blobLocation = new BlobLocation(blobFile, fileOffset, fileLength);

            var previousFile = extraContent.put(extraFile.name(), blobLocation);
            assert previousFile == null : extraFile.name();

            var previousOffset = internalDataReadersByOffset.put(
                fileOffset,
                new InternalFileReader(extraFile.name(), reference.getDirectory())
            );
            assert previousOffset == null : extraFile.name();
            fileOffset += fileLength;
        }
        currentOffset.set(fileOffset);

        var pendingCompoundCommit = new PendingCompoundCommit(
            header.length,
            reference,
            reference.isHollow()
                ? StatelessCompoundCommit.newHollowStatelessCompoundCommit(
                    shardId,
                    ccTermAndGen,
                    Collections.unmodifiableMap(commitFiles),
                    sizeInBytes,
                    internalFiles.stream().map(InternalFile::name).collect(Collectors.toUnmodifiableSet()),
                    header.length,
                    replicatedContent.header(),
                    Collections.unmodifiableMap(extraContent)
                )
                : new StatelessCompoundCommit(
                    shardId,
                    ccTermAndGen,
                    reference.getTranslogRecoveryStartFile(),
                    nodeEphemeralId,
                    Collections.unmodifiableMap(commitFiles),
                    sizeInBytes,
                    internalFiles.stream().map(InternalFile::name).collect(Collectors.toUnmodifiableSet()),
                    header.length,
                    replicatedContent.header(),
                    Collections.unmodifiableMap(extraContent)
                ),
            Long.parseLong(reference.getIndexCommit().getUserData().get(SequenceNumbers.MAX_SEQ_NO))
        );
        pendingCompoundCommits.add(pendingCompoundCommit);
        assert currentOffset.get() == headerOffset + pendingCompoundCommit.getSizeInBytes()
            : "current offset "
                + currentOffset.get()
                + " should be equal to header offset "
                + headerOffset
                + " plus size of pending compound commit "
                + pendingCompoundCommit.getSizeInBytes();
        assert assertInternalConsistency();
        return true;
    }

    private boolean assertInternalConsistency() {
        final Set<String> allInternalFiles = pendingCompoundCommits.stream()
            .flatMap(pc -> pc.getStatelessCompoundCommit().internalFiles().stream())
            .collect(Collectors.toUnmodifiableSet());
        assert allInternalFiles.equals(internalLocations.keySet()) : "all internal files must have internal blobLocations";

        final var sizeInBytes = pendingCompoundCommits.stream().mapToLong(PendingCompoundCommit::getSizeInBytes).sum();
        assert sizeInBytes == currentOffset.get() : "current offset must be at the end of the VBCC";

        var it = pendingCompoundCommits.iterator();
        while (it.hasNext()) {
            var pendingCompoundCommit = it.next();
            final var cc = pendingCompoundCommit.getStatelessCompoundCommit();
            // Assert that compound commits have padding to be page-aligned, except for the last compound commit
            assert it.hasNext() == false || pendingCompoundCommit.getSizeInBytes() == BlobCacheUtils.toPageAlignedSize(cc.sizeInBytes())
                : "intermediate statelessCompoundCommit size in bytes "
                    + cc.sizeInBytes()
                    + " plus padding length "
                    + pendingCompoundCommit.padding
                    + " should be equal to page-aligned size in bytes "
                    + BlobCacheUtils.toPageAlignedSize(cc.sizeInBytes());
            assert it.hasNext() || pendingCompoundCommit.padding == 0 : "last pending compound commit should not have padding";

            // Assert that all generational files are contained in the same VBCC (no reference to a previous VBCC or BCC)
            for (var commitFile : cc.commitFiles().entrySet()) {
                assert isGenerationalFile(commitFile.getKey()) == false
                    || commitFile.getValue().getBatchedCompoundCommitTermAndGeneration().equals(primaryTermAndGeneration)
                    : "generational file "
                        + commitFile.getValue()
                        + " should be located in BCC "
                        + primaryTermAndGeneration
                        + " but got "
                        + commitFile.getValue().getBatchedCompoundCommitTermAndGeneration();
            }

            assert cc.commitFiles().keySet().containsAll(cc.internalFiles())
                : "internal files " + cc.internalFiles() + " must be part of commit files " + cc.commitFiles().keySet();

            assert cc.extraContent().isEmpty() || cc.hollow() : "currently only hollow commits can have extra files";
            assert cc.extraContent()
                .keySet()
                .stream()
                .allMatch(filename -> Objects.equals(IndexFileNames.getExtension(filename), LuceneFilesExtensions.SI.getExtension()))
                : "currently only segment info ."
                    + LuceneFilesExtensions.SI.getExtension()
                    + " files are expected to be in extra content files, and found "
                    + cc.extraContent().keySet();
            assert cc.commitFiles().keySet().containsAll(cc.extraContent().keySet())
                : "extra content files "
                    + cc.extraContent().keySet()
                    + " must be part of commit files "
                    + cc.commitFiles().keySet()
                    + " as they currently consist of replicated referenced files";
            assert cc.hollow() == false
                || cc.commitFiles()
                    .entrySet()
                    .stream()
                    .filter(
                        e -> cc.internalFiles().contains(e.getKey()) == false
                            && Objects.equals(IndexFileNames.getExtension(e.getKey()), LuceneFilesExtensions.SI.getExtension())
                    )
                    .allMatch(e -> cc.extraContent().containsKey(e.getKey()))
                : "hollow commit referenced .si files in "
                    + cc.commitFiles().keySet()
                    + " must be part of extra content files "
                    + cc.extraContent().keySet();

            // a blob location that refers to the whole CC, so that we can check that internal and extra content files are in these bounds
            final var maxBlobLocation = Stream.concat(cc.commitFiles().entrySet().stream(), cc.extraContent().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (value1, value2) -> value2))
                .values()
                .stream()
                .max((bl1, bl2) -> {
                    final var bcc = bl1.getBatchedCompoundCommitTermAndGeneration();
                    int cmp = bcc.compareTo(bl2.getBatchedCompoundCommitTermAndGeneration());
                    if (cmp != 0) {
                        return cmp;
                    }
                    return Long.compare(bl1.offset(), bl2.offset());
                })
                .get();
            final var ccLocation = new BlobLocation(
                maxBlobLocation.blobFile(),
                maxBlobLocation.offset() + maxBlobLocation.fileLength() - cc.sizeInBytes(),
                cc.sizeInBytes()
            );
            assert cc.extraContent().values().stream().allMatch(location -> ccLocation.contains(location))
                : "all extra content files "
                    + cc.extraContent()
                    + " must be contained within the compound commit file location "
                    + ccLocation;
            assert cc.internalFiles().stream().allMatch(file -> ccLocation.contains(cc.commitFiles().get(file)))
                : "all internal files "
                    + cc.internalFiles()
                    + " of commit files "
                    + cc.commitFiles()
                    + " must be contained within the compound commit file location "
                    + ccLocation;
            assert Sets.difference(cc.commitFiles().keySet(), cc.internalFiles())
                .stream()
                .allMatch(file -> ccLocation.contains(cc.commitFiles().get(file)) == false)
                : "all referenced files must be located outside the compound commit file location "
                    + ccLocation
                    + " but got "
                    + cc.commitFiles()
                    + " with "
                    + cc.internalFiles()
                    + " internal files";
        }

        // Group the internal data readers by class
        final Map<Class<?>, List<InternalDataReader>> internalDataReaderGroups = internalDataReadersByOffset.values()
            .stream()
            .collect(groupingBy(internalHeaderOrFile -> internalHeaderOrFile.getClass()));
        assert internalDataReaderGroups.get(InternalHeaderReader.class).size() == pendingCompoundCommits.size()
            : "all pending CCs must have header offsets";
        final Set<String> allExtraContentFiles = pendingCompoundCommits.stream()
            .flatMap(pc -> pc.getStatelessCompoundCommit().extraContent().keySet().stream())
            .collect(Collectors.toUnmodifiableSet());
        assert Sets.union(allInternalFiles, allExtraContentFiles)
            .equals(
                Set.copyOf(
                    internalDataReaderGroups.get(InternalFileReader.class).stream().map(r -> ((InternalFileReader) r).filename).toList()
                )
            ) : "all internal and extra content files must have a corresponding InternalFileReader";
        if (internalDataReaderGroups.containsKey(InternalPaddingReader.class)) {
            assert internalDataReaderGroups.get(InternalPaddingReader.class).size() < pendingCompoundCommits.size()
                : "paddings "
                    + internalDataReaderGroups.get(InternalPaddingReader.class).size()
                    + " are more than pending CCs (excluding the last one) "
                    + (pendingCompoundCommits.size() - 1);
            internalDataReaderGroups.get(InternalPaddingReader.class).forEach(reader -> {
                assert reader instanceof InternalPaddingReader;
                InternalPaddingReader paddingReader = (InternalPaddingReader) reader;
                assert paddingReader.padding < SharedBytes.PAGE_SIZE
                    : "padding " + paddingReader.padding + " is more than page size " + SharedBytes.PAGE_SIZE;
            });
        }
        return true;
    }

    private ReplicatedContent getReplicatedContent(
        PrimaryTermAndGeneration commitTermAndGen,
        long currentOffset,
        TreeSet<InternalFile> internalFiles,
        long internalFilesSize,
        Directory directory
    ) {
        // Current position of the start of the commit (relative to the start of the region)
        int currentPositionInRegion = (int) (currentOffset % cacheRegionSizeInBytes);

        // Approximate position of the end of the header + replicated content (using the estimated max. header size as a hint)
        int estimatedHeaderEnd = Math.addExact(currentPositionInRegion, estimatedMaxHeaderSizeInBytes);

        // Approximate position of the end of the commit
        var estimatedCommitEnd = Math.addExact(estimatedHeaderEnd, internalFilesSize);

        // Check if the header and internal files completely fit into the same region, in which case there is no need for replicated content
        if (estimatedCommitEnd < cacheRegionSizeInBytes) {
            logger.trace(
                "{} skipping content replication: commit {} at offset [{}][{}] with [{}] bytes of internal files "
                    + "would fit within the cache region of [{}] bytes assuming an approximate header size of [{}] bytes",
                shardId,
                commitTermAndGen,
                currentOffset,
                currentPositionInRegion,
                internalFilesSize,
                cacheRegionSizeInBytes,
                estimatedMaxHeaderSizeInBytes
            );
            return ReplicatedContent.EMPTY;
        }

        LongPredicate shouldReplicate;
        if (estimatedMaxHeaderSizeInBytes != 0) {
            // Replicate content for internal files that are not in the same region as the header
            shouldReplicate = (offset) -> cacheRegionSizeInBytes <= (estimatedHeaderEnd + offset);
        } else {
            shouldReplicate = ALWAYS_REPLICATE; // Keep previous behavior
        }
        return ReplicatedContent.create(true, internalFiles, directory, shouldReplicate);
    }

    public BatchedCompoundCommit getFrozenBatchedCompoundCommit() {
        assert isFrozen() : "Cannot serialize before freeze";
        assert assertInternalConsistency();

        List<StatelessCompoundCommit> compoundCommits = new ArrayList<>(pendingCompoundCommits.size());
        for (PendingCompoundCommit pendingCompoundCommit : pendingCompoundCommits) {
            compoundCommits.add(pendingCompoundCommit.getStatelessCompoundCommit());
        }
        return new BatchedCompoundCommit(primaryTermAndGeneration, Collections.unmodifiableList(compoundCommits));
    }

    /**
     * Generate an InputStream of the serialized VBCC suitable for upload to blob storage.
     * <p>
     * The InputStream is implemented as a {@link SlicedInputStream} that iterates over a set of InputStreams
     * representing VBCC metadata and each of the Lucene files included in the VBCC commits.
     * <p>
     * As each Lucene file InputStream is consumed, it maintains a running checksum of the bytes read, which it compares
     * to the checksum in the Lucene file footer when the file substream is closed. Each substream is closed before opening
     * the next, according to the contract of {@link SlicedInputStream}.
     * <p>
     * If Lucene file corruption is detected when the file is closed, it will throw a CorruptIndexException that propagates
     * up through the SlicedInputStream as it is read.
     *
     * @return an InputStream of the VBCC, which throws CorruptIndexException on Lucene checksum mismatch, in addition to
     *   general IOExceptions on IO error.
     */
    public InputStream getFrozenInputStreamForUpload() {
        assert isFrozen() : "Cannot stream before freeze";
        assert assertInternalConsistency();
        return getInputStreamForUpload();
    }

    InputStream getInputStreamForUpload() {
        mustIncRef();
        List<Long> offsets = internalDataReadersByOffset.navigableKeySet().stream().collect(Collectors.toUnmodifiableList());
        return wrapForLogging(new SlicedInputStream(offsets.size()) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                final var offset = offsets.get(slice);
                final var reader = internalDataReadersByOffset.get(offset);
                return reader.getInputStream();
            }

            @Override
            public void close() throws IOException {
                if (isClosed() == false) {
                    try {
                        super.close();
                    } finally {
                        decRef();
                    }
                }
            }
        });
    }

    public InputStream getFrozenInputStreamForUpload(final long offset, final long length) {
        assert isFrozen() : "Cannot stream before freeze";
        assert assertInternalConsistency();
        assert hasReferences();

        mustIncRef();
        final var slices = internalDataReadersByOffset.subMap(
            internalDataReadersByOffset.floorKey(offset),
            true,
            // could have been offset + length - 1, but we avoid an `if` that we'd
            // otherwise need to avoid a NPE for the case of getBytesByRange(0, 0).
            internalDataReadersByOffset.floorKey(offset + length),
            true
        ).entrySet().stream().toList();

        if (slices.isEmpty()) {
            return new ByteArrayInputStream(BytesRef.EMPTY_BYTES);
        }
        return limitStream(wrapForLogging(new SlicedInputStream(slices.size()) {

            final AtomicBoolean closed = new AtomicBoolean();

            @Override
            protected InputStream openSlice(int n) throws IOException {
                var slice = slices.get(n);
                long skipBytes = Math.max(0L, offset - slice.getKey());
                assert skipBytes == 0 || n == 0 : "can be non-zero only for the first entry, but got: " + skipBytes + " for slice " + n;
                if (skipBytes > 0) {
                    // make sure that we validate the checksum of any file we reach the end of. To do this we need to read from the
                    // beginning of this file even when we're starting at an offset. We do want to avoid reading skipped bytes if we are
                    // not reaching the end of the file in this slice to minimize overread, so we only read skipped bytes if the end of the
                    // first file in this slice is contained in the slice.
                    final long chunkEnd = offset + length;
                    final Long higherKey = internalDataReadersByOffset.higherKey(offset);
                    final long fileEnd = higherKey == null ? getTotalSizeInBytes() : higherKey;
                    final boolean overread = fileEnd <= chunkEnd;
                    var stream = overread ? slice.getValue().getInputStream() : slice.getValue().getInputStream(skipBytes, Long.MAX_VALUE);
                    if (overread) {
                        stream.skipNBytes(skipBytes);
                    }
                    assert stream.markSupported();
                    return stream;
                } else {
                    var stream = slice.getValue().getInputStream();
                    assert stream.markSupported();
                    return stream;
                }
            }

            @Override
            public void close() throws IOException {
                if (closed.compareAndSet(false, true)) {
                    try {
                        super.close();
                    } finally {
                        decRef();
                    }
                }
            }
        }), length);
    }

    public String getBlobName() {
        return blobFile.blobName();
    }

    public ShardId getShardId() {
        return shardId;
    }

    public PrimaryTermAndGeneration getPrimaryTermAndGeneration() {
        return primaryTermAndGeneration;
    }

    @Override
    public PrimaryTermAndGeneration primaryTermAndGeneration() {
        return getPrimaryTermAndGeneration();
    }

    public long getTotalSizeInBytes() {
        return currentOffset.get();
    }

    public Map<String, BlobLocation> getInternalLocations() {
        return internalLocations;
    }

    public long getCreationTimeInMillis() {
        return creationTimeInMillis;
    }

    public StatelessCompoundCommit lastCompoundCommit() {
        assert pendingCompoundCommits.isEmpty() == false;
        return pendingCompoundCommits.last().getStatelessCompoundCommit();
    }

    public long getMaxGeneration() {
        assert pendingCompoundCommits.isEmpty() == false;
        return pendingCompoundCommits.last().getGeneration();
    }

    public PendingCompoundCommit getLastPendingCompoundCommit() {
        return pendingCompoundCommits.last();
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        IOUtils.closeWhileHandlingException(pendingCompoundCommits);
    }

    public List<PendingCompoundCommit> getPendingCompoundCommits() {
        return List.copyOf(pendingCompoundCommits);
    }

    int size() {
        return pendingCompoundCommits.size();
    }

    Set<PrimaryTermAndGeneration> getPendingCompoundCommitGenerations() {
        return pendingCompoundCommits.stream()
            .map(PendingCompoundCommit::getStatelessCompoundCommit)
            .map(StatelessCompoundCommit::primaryTermAndGeneration)
            .collect(Collectors.toSet());
    }

    // visible for testing
    public int getTotalPaddingInBytes() {
        return pendingCompoundCommits.stream().mapToInt(pendingCompoundCommit -> pendingCompoundCommit.padding).sum();
    }

    private byte[] materializeCompoundCommitHeader(
        StatelessCommitRef reference,
        Iterable<InternalFile> internalFiles,
        InternalFilesReplicatedRanges replicatedRanges,
        Map<String, BlobLocation> referencedFiles,
        boolean useInternalFilesReplicatedContent,
        Iterable<InternalFile> extraContent
    ) throws IOException {
        assert getBlobName() != null;
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            var positionTrackingOutputStreamStreamOutput = new PositionTrackingOutputStreamStreamOutput(os);
            StatelessCompoundCommit.writeXContentHeader(
                shardId,
                reference.getGeneration(),
                reference.getPrimaryTerm(),
                reference.isHollow() ? "" : nodeEphemeralId,
                reference.getTranslogRecoveryStartFile(),
                referencedFiles,
                internalFiles,
                replicatedRanges,
                CURRENT_VERSION,
                positionTrackingOutputStreamStreamOutput,
                useInternalFilesReplicatedContent,
                extraContent
            );
            return os.toByteArray();
        }
    }

    BlobLocation getBlobLocation(String fileName) {
        var internalLocation = internalLocations.get(fileName);
        return internalLocation == null ? uploadedBlobLocationsSupplier.apply(fileName) : internalLocation;
    }

    /**
     * Get the bytes of the virtual batched compound commit by reading the internal files (headers and internal files of pending
     * compound commits) in the given range.
     * @param offset the offset in the virtual batched compound commit to start reading internal files
     * @param length the length of the range to read
     * @param output the output to write the bytes to
     * @throws IOException
     */
    public void getBytesByRange(final long offset, final long length, final OutputStream output) throws IOException {
        assert offset >= 0;
        assert length >= 0 : "invalid length " + length;
        assert offset + length <= currentOffset.get() : "range [" + offset + ", " + length + "] more than " + currentOffset.get();
        assert ThreadPool.assertCurrentThreadPool(
            Stateless.GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL,
            Stateless.SHARD_WRITE_THREAD_POOL,
            Stateless.PREWARM_THREAD_POOL,
            Stateless.UPLOAD_PREWARM_THREAD_POOL
        );

        if (tryIncRef()) {
            try {
                NavigableMap<Long, InternalDataReader> subMap = internalDataReadersByOffset.subMap(
                    internalDataReadersByOffset.floorKey(offset),
                    true,
                    // could have been offset + length - 1, but we avoid an `if` that we'd
                    // otherwise need to avoid a NPE for the case of getBytesByRange(0, 0).
                    internalDataReadersByOffset.floorKey(offset + length),
                    true
                );
                long remainingBytesToRead = length;
                for (var entry : subMap.entrySet()) {
                    if (remainingBytesToRead <= 0) {
                        break;
                    }
                    InternalDataReader internalDataReader = entry.getValue();
                    long skipBytes = Math.max(0, offset - entry.getKey()); // can be non-zero only for the first entry
                    try (var inputStream = internalDataReader.getInputStream(skipBytes, remainingBytesToRead)) {
                        long bytesRead = Streams.copy(inputStream, output, false);
                        remainingBytesToRead -= bytesRead;
                    }
                }
                assert remainingBytesToRead == 0 : "remaining bytes to read " + remainingBytesToRead;
            } finally {
                decRef();
            }
        } else {
            throw buildResourceNotFoundException(shardId, primaryTermAndGeneration);
        }
    }

    boolean assertSameNodeEphemeralId(String id) {
        assert id.equals(nodeEphemeralId) : id + " != " + nodeEphemeralId;
        return true;
    }

    @Override
    public String toString() {
        return "VirtualBatchedCompoundCommit{"
            + "shardId="
            + shardId
            + ", primaryTermAndGeneration="
            + primaryTermAndGeneration
            + ", size="
            + size()
            + ", nodeEphemeralId='"
            + nodeEphemeralId
            + '\''
            + ", creationTimeInMillis="
            + creationTimeInMillis
            + ", frozen="
            + frozen
            + '}';
    }

    public static ResourceNotFoundException buildResourceNotFoundException(
        ShardId shardId,
        PrimaryTermAndGeneration primaryTermAndGeneration
    ) {
        return new ResourceNotFoundException("BCC for shard " + shardId + " and " + primaryTermAndGeneration + " is already uploaded");
    }

    private boolean assertCompareAndSetFreezeOrAppendingCommitThread(Thread current, Thread updated) {
        final Thread witness = appendingCommitThread.compareAndExchange(current, updated);
        assert witness == current
            : "Unable to set appending commit thread to ["
                + updated
                + "]: expected thread ["
                + current
                + "] to be the appending commit thread, but thread "
                + witness
                + " is already appending a commit to "
                + getBlobName();
        return true;
    }

    static class PendingCompoundCommit implements Closeable, Comparable<PendingCompoundCommit> {
        private final int headerSize;
        private final StatelessCommitRef reference;
        private final StatelessCompoundCommit statelessCompoundCommit;
        private final long maxSeqNo;
        // No need to be volatile because writing is synchronized at higher level in StatelessCommitService
        // and reading is dispatched to another thread after a second synchronization
        private int padding = 0;

        /**
         * Creates a new pending to upload compound commit. Note that the last pending compound commit should not have padding. The
         * padding is added to the previous pending compound commit when appending a new pending compound commit.
         * @param headerSize the size of materialized compound commit header
         * @param reference the lucene commit reference
         * @param statelessCompoundCommit the associated compound commit that will be uploaded
         */
        PendingCompoundCommit(
            int headerSize,
            StatelessCommitRef reference,
            StatelessCompoundCommit statelessCompoundCommit,
            long maxSeqNo
        ) {
            this.headerSize = headerSize;
            this.reference = reference;
            this.statelessCompoundCommit = statelessCompoundCommit;
            this.maxSeqNo = maxSeqNo;
            assert statelessCompoundCommit.hollow() == reference.isHollow()
                : "stateless compound commit hollow flag ["
                    + statelessCompoundCommit.hollow()
                    + "] does not match a hollow reference ["
                    + reference.isHollow()
                    + "]";
        }

        void setPadding(int padding) {
            this.padding = padding;
            assert padding >= 0 : "padding " + padding + " is negative";
        }

        public long getGeneration() {
            return reference.getGeneration();
        }

        long getMaxSeqNo() {
            return maxSeqNo;
        }

        StatelessCommitRef getCommitReference() {
            return reference;
        }

        /**
         * the size of the compound commit including codec, header, checksums, all files, and padding
         * Note that the last pending compound commit should not have padding. The padding is added to the previous pending compound commit
         * when appending a new pending compound commit.
         */
        public long getSizeInBytes() {
            return statelessCompoundCommit.sizeInBytes() + padding;
        }

        public StatelessCompoundCommit getStatelessCompoundCommit() {
            return statelessCompoundCommit;
        }

        // package-private for testing
        long getHeaderSize() {
            return headerSize;
        }

        @Override
        public int compareTo(PendingCompoundCommit o) {
            return Long.compare(getGeneration(), o.getGeneration());
        }

        @Override
        public void close() throws IOException {
            logger.debug(
                "{} releasing Lucene commit [term={}, gen={}]",
                statelessCompoundCommit.shardId(),
                reference.getPrimaryTerm(),
                reference.getGeneration()
            );
            reference.close();
        }
    }

    /**
     * Interface for reading internal data from a batched compound commit
     */
    interface InternalDataReader {
        /**
         * Get the {@link InputStream} for reading the internal data.
         * @param offset the number of bytes to skip in the internal data before starting to read the internal data.
         * @param length the max number of bytes to read. ineffective if larger than the remaining available size of the internal data.
         */
        InputStream getInputStream(long offset, long length) throws IOException;

        /**
         * Get the {@link InputStream} for reading the entire contents of the contained file.
         * @return An input stream that will read the entire contents of the file.
         * @throws IOException
         */
        InputStream getInputStream() throws IOException;
    }

    /**
     * Internal data reader for header bytes
     */
    private record InternalHeaderReader(byte[] header) implements InternalDataReader {
        @Override
        public InputStream getInputStream(long offset, long length) throws IOException {
            var stream = new ByteArrayInputStream(header);
            stream.skipNBytes(offset);
            return limitStream(stream, length);
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream(header);
        }
    }

    /**
     * Internal data reader for an internal file
     */
    private record InternalFileReader(String filename, Directory directory) implements InternalDataReader {
        @Override
        public InputStream getInputStream(long offset, long length) throws IOException {
            long fileLength = directory.fileLength(filename);
            assert offset < fileLength : "offset [" + offset + "] more than file length [" + fileLength + "]";
            long fileBytesToRead = Math.min(length, fileLength - offset);
            var ioContext = filename.startsWith(IndexFileNames.SEGMENTS) ? IOContext.READONCE : IOContext.DEFAULT;
            IndexInput input = directory.openInput(filename, ioContext);
            try {
                input.seek(offset);
                return new InputStreamIndexInput(input, fileBytesToRead) {
                    @Override
                    public void close() throws IOException {
                        IOUtils.close(super::close, input);
                    }
                };
            } catch (IOException e) {
                IOUtils.closeWhileHandlingException(input);
                throw e;
            }
        }

        /**
         * Produce an input stream for an index file that updates a running checksum as it is read, and validates it against the Lucene
         * footer when it is read to the end.
         * @return an input stream for this instance's filename
         * @throws IOException on an IO error opening the file (and the stream may throw CorruptIndexException on read)
         */
        @Override
        public InputStream getInputStream() throws IOException {
            Store.VerifyingIndexInput input = new Store.VerifyingIndexInput(directory.openInput(filename, IOContext.READONCE));
            logger.trace("opening validating input for {}", filename);

            return new InputStreamIndexInput(input, input.length()) {
                @Override
                public int read() throws IOException {
                    int ret = super.read();
                    verifyAtEnd();
                    return ret;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    int ret = super.read(b, off, len);
                    verifyAtEnd();
                    return ret;
                }

                @Override
                public void close() throws IOException {
                    IOUtils.close(super::close, input);
                }

                void verifyAtEnd() throws IOException {
                    if (input.getFilePointer() == input.length()) {
                        input.verify();
                    }
                }
            };
        }
    }

    /**
     * Internal data reader for padding bytes
     */
    private record InternalPaddingReader(int padding) implements InternalDataReader {

        public InternalPaddingReader {
            assert padding <= SharedBytes.PAGE_SIZE : "padding " + padding + " is more than page size " + SharedBytes.PAGE_SIZE;
        }

        private static final byte[] PADDING_BYTES;

        static {
            byte[] padding = new byte[SharedBytes.PAGE_SIZE];
            Arrays.fill(padding, (byte) 0);
            PADDING_BYTES = padding;
        }

        @Override
        public InputStream getInputStream(long offset, long length) {
            assert offset < padding : "offset [" + offset + "] more than padding length [" + padding + "]";
            int paddingBytesToRead = BlobCacheUtils.toIntBytes(Math.min(length, padding - offset));
            return limitStream(new ByteArrayInputStream(PADDING_BYTES), paddingBytesToRead);
        }

        @Override
        public InputStream getInputStream() {
            return getInputStream(0L, padding);
        }
    }

    private InputStream wrapForLogging(InputStream stream) {
        if (LOG_TIME_SPENT_READING_DURING_UPLOAD.isDebugEnabled()) {
            return new LogTimeSpentReadingInputStream(stream, shardId, primaryTermAndGeneration);
        } else {
            return stream;
        }
    }

    /**
     * {@link FilterInputStream} that tracks the time spent reading from the delegating input stream.
     */
    private static class LogTimeSpentReadingInputStream extends FilterInputStream {

        private final ShardId shardId;
        private final PrimaryTermAndGeneration primaryTermAndGeneration;
        private long elapsedNanos;
        private long bytes;

        LogTimeSpentReadingInputStream(InputStream in, ShardId shardId, PrimaryTermAndGeneration primaryTermAndGeneration) {
            super(in);
            this.shardId = shardId;
            this.primaryTermAndGeneration = primaryTermAndGeneration;
        }

        @Override
        public int read() throws IOException {
            long startTime = System.nanoTime();
            int result = super.read();
            elapsedNanos += (System.nanoTime() - startTime);
            if (result != -1) {
                bytes += 1L;
            }
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            long startTime = System.nanoTime();
            var result = super.read(b, off, len);
            elapsedNanos += (System.nanoTime() - startTime);
            if (result != -1) {
                bytes += result;
            }
            return result;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                LOG_TIME_SPENT_READING_DURING_UPLOAD.debug(
                    "{} spent [{}] ms reading [{}] bytes from VBCC {} during upload",
                    shardId,
                    TimeValue.nsecToMSec(elapsedNanos),
                    bytes,
                    primaryTermAndGeneration
                );
            }
        }
    }
}
