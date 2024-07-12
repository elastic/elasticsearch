/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.index.translog.transfer;


import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.InputStreamWithMetadata;
import org.elasticsearch.common.blobstore.stream.write.WritePriority;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.store.exception.ChecksumCombinationException;
import org.elasticsearch.index.translog.ChannelFactory;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.common.util.ByteUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * Service that handles remote transfer of translog and checkpoint files
 *
 * @opensearch.internal
 */
public class BlobStoreTransferService implements TransferService {

    private final BlobStore blobStore;
    private final ThreadPool threadPool;

    private static final int CHECKSUM_BYTES_LENGTH = 8;
    private static final Logger logger = LogManager.getLogger(BlobStoreTransferService.class);

    public BlobStoreTransferService(BlobStore blobStore, ThreadPool threadPool) {
        this.blobStore = blobStore;
        this.threadPool = threadPool;
    }

    @Override
    public void uploadBlob(
        String threadPoolName,
        final FileSnapshot.TransferFileSnapshot fileSnapshot,
        Iterable<String> remoteTransferPath,
        ActionListener<FileSnapshot.TransferFileSnapshot> listener,
        WritePriority writePriority
    ) {
        assert remoteTransferPath instanceof BlobPath;
        BlobPath blobPath = (BlobPath) remoteTransferPath;
        threadPool.executor(threadPoolName).execute(ActionRunnable.wrap(listener, l -> {
            try {
                uploadBlob(fileSnapshot, (Iterable<String>) blobPath, writePriority);
                l.onResponse(fileSnapshot);
            } catch (Exception e) {
                logger.error(() -> String.valueOf(new ParameterizedMessage("Failed to upload blob {}", fileSnapshot.getName())), e);
                l.onFailure(new FileTransferException(fileSnapshot, e));
            }
        }));
    }

    @SuppressWarnings("checkstyle:LineLength")
    @Override
    public void uploadBlob(final FileSnapshot.TransferFileSnapshot fileSnapshot, Iterable<String> remoteTransferPath, WritePriority writePriority)
        throws IOException {
        BlobPath blobPath = (BlobPath) remoteTransferPath;
        try (InputStream inputStream = fileSnapshot.inputStream()) {
            blobStore.blobContainer(blobPath).writeBlobAtomic(fileSnapshot.getName(), inputStream, fileSnapshot.getContentLength(), true);
        }
    }

    @SuppressWarnings("checkstyle:DescendantToken")
    @Override
    public void uploadBlobs(
        Set<FileSnapshot.TransferFileSnapshot> fileSnapshots,
        final Map<Long, BlobPath> blobPaths,
        ActionListener<FileSnapshot.TransferFileSnapshot> listener,
        WritePriority writePriority
    ) {
        fileSnapshots.forEach(fileSnapshot -> {
            BlobPath blobPath = blobPaths.get(fileSnapshot.getPrimaryTerm());
            if (!(blobStore.blobContainer(blobPath) instanceof AsyncMultiStreamBlobContainer)) {
                uploadBlob(ThreadPool.Names.TRANSLOG_TRANSFER, fileSnapshot, (Iterable<String>) blobPath, listener, writePriority);
            } else {
                uploadBlob(fileSnapshot, listener, blobPath, writePriority);
            }
        });

    }

    @Override
    public void uploadBlob(
        InputStream inputStream,
        Iterable<String> remotePath,
        String fileName,
        WritePriority writePriority,
        ActionListener<Void> listener
    ) throws IOException {
        assert remotePath instanceof BlobPath;
        BlobPath blobPath = (BlobPath) remotePath;
        final BlobContainer blobContainer = blobStore.blobContainer(blobPath);
        if (blobContainer instanceof AsyncMultiStreamBlobContainer == false) {
            blobContainer.writeBlob(fileName, inputStream, inputStream.available(), false);
            listener.onResponse(null);
            return;
        }
        final String resourceDescription = "BlobStoreTransferService.uploadBlob(blob=\"" + fileName + "\")";
        byte[] bytes = inputStream.readAllBytes();
        try (IndexInput input = new ByteArrayIndexInput(resourceDescription, bytes)) {
            long expectedChecksum = computeChecksum(input, resourceDescription);
            uploadBlobAsyncInternal(
                fileName,
                fileName,
                bytes.length,
                blobPath,
                writePriority,
                (size, position) -> new OffsetRangeIndexInputStream(input, size, position),
                expectedChecksum,
                listener,
                null
            );
        }
    }

    // Builds a metadata map containing the Base64-encoded checkpoint file data associated with a translog file.
    static Map<String, String> buildTransferFileMetadata(InputStream metadataInputStream) throws IOException {
        Map<String, String> metadata = new HashMap<>();
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[128];
            int bytesRead;
            int totalBytesRead = 0;

            while ((bytesRead = metadataInputStream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
                if (totalBytesRead > 1024) {
                    // We enforce a limit of 1KB on the size of the checkpoint file.
                    throw new IOException("Input stream exceeds 1KB limit");
                }
            }

            byte[] bytes = byteArrayOutputStream.toByteArray();
            String metadataString = Base64.getEncoder().encodeToString(bytes);
            metadata.put(CHECKPOINT_FILE_DATA_KEY, metadataString);
        }
        return metadata;
    }

    private void uploadBlob(
        FileSnapshot.TransferFileSnapshot fileSnapshot,
        ActionListener<FileSnapshot.TransferFileSnapshot> listener,
        BlobPath blobPath,
        WritePriority writePriority
    ) {

        try {
            ChannelFactory channelFactory = FileChannel::open;
            Map<String, String> metadata = null;
            if (fileSnapshot.getMetadataFileInputStream() != null) {
                metadata = buildTransferFileMetadata(fileSnapshot.getMetadataFileInputStream());
            }

            long contentLength;
            try (FileChannel channel = channelFactory.open(fileSnapshot.getPath(), StandardOpenOption.READ)) {
                contentLength = channel.size();
            }
            ActionListener<Void> completionListener = ActionListener.wrap(resp -> listener.onResponse(fileSnapshot), ex -> {
                logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", fileSnapshot.getName()), ex);
                listener.onFailure(new FileTransferException(fileSnapshot, ex));
            });

            Objects.requireNonNull(fileSnapshot.getChecksum());
            uploadBlobAsyncInternal(
                fileSnapshot.getName(),
                fileSnapshot.getName(),
                contentLength,
                blobPath,
                writePriority,
                (size, position) -> new OffsetRangeFileInputStream(fileSnapshot.getPath(), size, position),
                fileSnapshot.getChecksum(),
                completionListener,
                metadata
            );

        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed to upload blob {}", fileSnapshot.getName()), e);
            listener.onFailure(new FileTransferException(fileSnapshot, e));
        } finally {
            try {
                fileSnapshot.close();
            } catch (IOException e) {
                logger.warn("Error while closing TransferFileSnapshot", e);
            }
        }

    }

    private void uploadBlobAsyncInternal(
        String fileName,
        String remoteFileName,
        long contentLength,
        BlobPath blobPath,
        WritePriority writePriority,
        RemoteTransferContainer.OffsetRangeInputStreamSupplier inputStreamSupplier,
        long expectedChecksum,
        ActionListener<Void> completionListener,
        Map<String, String> metadata
    ) throws IOException {
        BlobContainer blobContainer = blobStore.blobContainer(blobPath);
        assert blobContainer instanceof AsyncMultiStreamBlobContainer;
        boolean remoteIntegrityEnabled = ((AsyncMultiStreamBlobContainer) blobContainer).remoteIntegrityCheckSupported();
        try (
            RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                fileName,
                remoteFileName,
                contentLength,
                true,
                writePriority,
                inputStreamSupplier,
                expectedChecksum,
                remoteIntegrityEnabled,
                metadata
            )
        ) {
            ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(
                remoteTransferContainer.createWriteContext(),
                completionListener
            );
        }
    }

    @Override
    public InputStream downloadBlob(Iterable<String> path, String fileName) throws IOException {
        return blobStore.blobContainer((BlobPath) path).readBlob(fileName);
    }

    @Override
    public InputStreamWithMetadata downloadBlobWithMetadata(Iterable<String> path, String fileName) throws IOException {
        assert blobStore.isBlobMetadataEnabled();
        return blobStore.blobContainer((BlobPath) path).readBlobWithMetadata(fileName);
    }

    @Override
    public void deleteBlobs(Iterable<String> path, List<String> fileNames) throws IOException {
        blobStore.blobContainer((BlobPath) path).deleteBlobsIgnoringIfNotExists(fileNames);
    }

    @Override
    public void deleteBlobsAsync(String threadpoolName, Iterable<String> path, List<String> fileNames, ActionListener<Void> listener) {
        threadPool.executor(threadpoolName).execute(() -> {
            try {
                deleteBlobs(path, fileNames);
                listener.onResponse(null);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void delete(Iterable<String> path) throws IOException {
        blobStore.blobContainer((BlobPath) path).delete();
    }

    @Override
    public void deleteAsync(String threadpoolName, Iterable<String> path, ActionListener<Void> listener) {
        threadPool.executor(threadpoolName).execute(() -> {
            try {
                delete(path);
                listener.onResponse(null);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public Set<String> listAll(Iterable<String> path) throws IOException {
        return blobStore.blobContainer((BlobPath) path).listBlobs().keySet();
    }

    @Override
    public Set<String> listFolders(Iterable<String> path) throws IOException {
        return blobStore.blobContainer((BlobPath) path).children().keySet();
    }

    @Override
    public void listFoldersAsync(String threadpoolName, Iterable<String> path, ActionListener<Set<String>> listener) {
        threadPool.executor(threadpoolName).execute(() -> {
            try {
                listener.onResponse(listFolders(path));
            } catch (IOException e) {
                listener.onFailure(e);
            }
        });
    }

    public void listAllInSortedOrder(Iterable<String> path, String filenamePrefix, int limit, ActionListener<List<BlobMetadata>> listener) {
        blobStore.blobContainer((BlobPath) path).listBlobsByPrefixInSortedOrder(limit, listener);
    }

    public void listAllInSortedOrderAsync(
        String threadpoolName,
        Iterable<String> path,
        String filenamePrefix,
        int limit,
        ActionListener<List<BlobMetadata>> listener
    ) {
        threadPool.executor(threadpoolName).execute(() -> { listAllInSortedOrder(path, filenamePrefix, limit, listener); });
    }

    private static long computeChecksum(IndexInput indexInput, String resourceDescription) throws ChecksumCombinationException {
        long expectedChecksum;
        try {
            expectedChecksum = checksumOfChecksum(indexInput.clone(), CHECKSUM_BYTES_LENGTH);
        } catch (Exception e) {
            throw new ChecksumCombinationException(
                "Potentially corrupted file: Checksum combination failed while combining stored checksum "
                    + "and calculated checksum of stored checksum",
                resourceDescription,
                e
            );
        }
        return expectedChecksum;
    }

    public static long checksumOfChecksum(IndexInput indexInput, int checksumBytesLength) throws IOException {
        long storedChecksum = CodecUtil.retrieveChecksum(indexInput);
        CRC32 checksumOfChecksum = new CRC32();
        checksumOfChecksum.update(ByteUtils.toByteArrayBE(storedChecksum));
        return crc32_combine(storedChecksum, checksumOfChecksum.getValue(), checksumBytesLength);
    }
    public static long crc32_combine(long crc1, long crc2, long len2){
        return combine(crc1, crc2, len2);
    }
    private static final int GF2_DIM = 32;

    static long combine(long crc1, long crc2, long len2){
        long row;
        long[] even = new long[GF2_DIM];
        long[] odd = new long[GF2_DIM];

        // degenerate case (also disallow negative lengths)
        if (len2 <= 0)
            return crc1;

        // put operator for one zero bit in odd
        odd[0] = 0xedb88320L;          // CRC-32 polynomial
        row = 1;
        for (int n = 1; n < GF2_DIM; n++) {
            odd[n] = row;
            row <<= 1;
        }

        // put operator for two zero bits in even
        gf2_matrix_square(even, odd);

        // put operator for four zero bits in odd
        gf2_matrix_square(odd, even);

        // apply len2 zeros to crc1 (first square will put the operator for one
        // zero byte, eight zero bits, in even)
        do {
            // apply zeros operator for this bit of len2
            gf2_matrix_square(even, odd);
            if ((len2 & 1)!=0)
                crc1 = gf2_matrix_times(even, crc1);
            len2 >>= 1;

            // if no more bits set, then done
            if (len2 == 0)
                break;

            // another iteration of the loop with odd and even swapped
            gf2_matrix_square(odd, even);
            if ((len2 & 1)!=0)
                crc1 = gf2_matrix_times(odd, crc1);
            len2 >>= 1;

            // if no more bits set, then done
        } while (len2 != 0);

        /* return combined crc */
        crc1 ^= crc2;
        return crc1;
    }
    @SuppressWarnings("checkstyle:NeedBraces")
    static final void gf2_matrix_square(long[] square, long[] mat) {
        for (int n = 0; n < GF2_DIM; n++)
            square[n] = gf2_matrix_times(mat, mat[n]);
    }
    private static long gf2_matrix_times(long[] mat, long vec){
        long sum = 0;
        int index = 0;
        while (vec!=0) {
            if ((vec & 1)!=0)
                sum ^= mat[index];
            vec >>= 1;
            index++;
        }
        return sum;
    }


}
