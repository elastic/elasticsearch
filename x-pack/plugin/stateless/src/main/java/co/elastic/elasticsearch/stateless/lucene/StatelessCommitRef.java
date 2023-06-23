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

import org.apache.lucene.index.IndexFileNames;
import org.elasticsearch.common.lucene.FilterIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatelessCommitRef extends FilterIndexCommit implements Closeable {

    private final ShardId shardId;
    private final Engine.IndexCommitRef indexCommitRef;
    private final Collection<String> commitFiles;
    private final Set<String> additionalFiles;
    private final AtomicBoolean released;
    private final long primaryTerm;
    private final long translogRecoveryStartFile;

    public StatelessCommitRef(
        ShardId shardId,
        Engine.IndexCommitRef indexCommitRef,
        Collection<String> commitFiles,
        Set<String> additionalFiles,
        long primaryTerm,
        long translogRecoveryStartFile
    ) {
        super(indexCommitRef.getIndexCommit());
        this.shardId = Objects.requireNonNull(shardId);
        this.indexCommitRef = indexCommitRef;
        this.commitFiles = commitFiles;
        this.additionalFiles = Objects.requireNonNull(additionalFiles);
        this.primaryTerm = primaryTerm;
        this.translogRecoveryStartFile = translogRecoveryStartFile;
        this.released = new AtomicBoolean();
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public Collection<String> getCommitFiles() {
        return commitFiles;
    }

    public Set<String> getAdditionalFiles() {
        return additionalFiles;
    }

    @Override
    public void close() throws IOException {
        if (released.compareAndSet(false, true)) {
            indexCommitRef.close();
        }
    }

    public ShardId getShardId() {
        return shardId;
    }

    public long getTranslogRecoveryStartFile() {
        return translogRecoveryStartFile;
    }

    @Override
    public String toString() {
        return "StatelessCommitRef(" + shardId + ',' + primaryTerm + "," + in.toString() + ')';
    }

    public static boolean isGenerationalFile(String file) {
        return file.startsWith(IndexFileNames.SEGMENTS) == false && IndexFileNames.parseGeneration(file) > 0L;
    }
}
