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
 */

package co.elastic.elasticsearch.stateless.lucene;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SearchDirectory extends BaseDirectory {

    /**
     * In-memory directory containing an empty commit only. This is used in search shards before they are initialized from the repository.
     * Note that since we create this instance statically and only once, all shards will initially have the same history uuid and Lucene
     * version until they are initialized from the repository.
     */
    private static final Directory EMPTY_COMMIT_DIRECTORY = new ByteBuffersDirectory();

    static {
        var emptySegmentInfos = new SegmentInfos(Version.CURRENT.luceneVersion.major);
        emptySegmentInfos.setUserData(
            Map.of(
                Engine.HISTORY_UUID_KEY,
                UUIDs.randomBase64UUID(),
                SequenceNumbers.LOCAL_CHECKPOINT_KEY,
                Long.toString(SequenceNumbers.NO_OPS_PERFORMED),
                SequenceNumbers.MAX_SEQ_NO,
                Long.toString(SequenceNumbers.NO_OPS_PERFORMED),
                Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID,
                "-1",
                Engine.ES_VERSION,
                Version.CURRENT.toString()
            ),
            false
        );
        try {
            emptySegmentInfos.commit(EMPTY_COMMIT_DIRECTORY);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private final ShardId shardId;

    private final SharedBlobCacheService<FileCacheKey> cacheService;

    private final SetOnce<BlobContainer> blobContainer = new SetOnce<>();

    private volatile Map<String, Long> currentMetadata = Map.of();

    public SearchDirectory(SharedBlobCacheService<FileCacheKey> cacheService, ShardId shardId) {
        super(new SingleInstanceLockFactory());
        this.cacheService = cacheService;
        this.shardId = shardId;
    }

    public void setBlobContainer(BlobContainer blobContainer) {
        this.blobContainer.set(blobContainer);
    }

    /**
     * Moves the directory to a new commit by setting the newly valid map of files and their metadata.
     *
     * @param newCommit map of file name to store metadata
     */
    public void updateCommit(Map<String, StoreFileMetadata> newCommit) {
        assert blobContainer.get() != null : shardId + " must have the blob container set before any commit update";
        // TODO: we only accumulate files as we see new commits, we need to start cleaning this map once we add deletes
        final Map<String, Long> updated = new HashMap<>(currentMetadata);
        newCommit.forEach((name, storeMetadata) -> updated.put(name, storeMetadata.length()));
        currentMetadata = Map.copyOf(updated);
    }

    @Override
    public String[] listAll() throws IOException {
        final var current = currentMetadata;
        if (current.isEmpty()) {
            return EMPTY_COMMIT_DIRECTORY.listAll();
        }
        return current.keySet().stream().sorted(String::compareTo).toArray(String[]::new);
    }

    @Override
    public void deleteFile(String name) {
        throw unsupportedException();
    }

    @Override
    public long fileLength(String name) throws IOException {
        final var current = currentMetadata;
        if (current.isEmpty()) {
            return EMPTY_COMMIT_DIRECTORY.fileLength(name);
        }
        return current.get(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public void sync(Collection<String> names) {
        throw unsupportedException();
    }

    @Override
    public void syncMetaData() {
        throw unsupportedException();
    }

    @Override
    public void rename(String source, String dest) {
        throw unsupportedException();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final var current = currentMetadata;
        if (current.isEmpty()) {
            return EMPTY_COMMIT_DIRECTORY.openInput(name, context);
        }
        final Long len = current.get(name);
        assert len != null : "unknown file [" + name + "] accessed";
        return new SearchIndexInput(
            name,
            cacheService.getCacheFile(new FileCacheKey(shardId, name), len),
            context,
            blobContainer.get(),
            cacheService,
            len,
            0
        );
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Set<String> getPendingDeletions() {
        throw unsupportedException();
    }

    private static UnsupportedOperationException unsupportedException() {
        assert false : "this operation is not supported and should have not be called";
        return new UnsupportedOperationException("stateless directory does not support this operation");
    }

    public static SearchDirectory unwrapDirectory(final Directory directory) {
        Directory dir = directory;
        while (dir != null) {
            if (dir instanceof SearchDirectory searchDirectory) {
                return searchDirectory;
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        var e = new IllegalStateException(directory.getClass() + " cannot be unwrapped as " + SearchDirectory.class);
        assert false : e;
        throw e;
    }
}
