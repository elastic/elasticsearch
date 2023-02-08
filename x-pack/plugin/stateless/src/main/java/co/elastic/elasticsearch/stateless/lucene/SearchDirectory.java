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
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SearchDirectory extends FilterDirectory {

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

    private volatile Map<String, Long> currentMetadata = Map.of();

    public SearchDirectory(Directory in) {
        super(in);
    }

    /**
     * Moves the directory to a new commit by setting the newly valid map of files and their metadata.
     *
     * @param newCommit map of file name to store metadata
     */
    public void updateCommit(Map<String, StoreFileMetadata> newCommit) {
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
    public long fileLength(String name) throws IOException {
        final var current = currentMetadata;
        if (current.isEmpty()) {
            return EMPTY_COMMIT_DIRECTORY.fileLength(name);
        }
        return current.get(name);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (currentMetadata.isEmpty()) {
            return EMPTY_COMMIT_DIRECTORY.openInput(name, context);
        }
        return super.openInput(name, context);
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
