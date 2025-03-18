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

import org.elasticsearch.common.lucene.FilterIndexCommit;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE;

public class StatelessCommitRef extends FilterIndexCommit implements Closeable {

    private final ShardId shardId;
    private final Engine.IndexCommitRef indexCommitRef;
    private final Collection<String> commitFiles;
    private final Set<String> additionalFiles;
    private final AtomicBoolean released;
    private final long primaryTerm;
    // The translog recovery start file is encoded in the commit user data and in the CC header, and is used to pinpoint the starting
    // translog compound file number to start scanning from for recovering operations indexed after the commit. It takes a special value
    // of {@link #HOLLOW_TRANSLOG_RECOVERY_START_FILE} to indicate that the commit is hollow and has no translog to recover from.
    private final long translogRecoveryStartFile;
    // The translog release end file is used so that the {@link TranslogReplicator} can release any translog files before this one.
    private final long translogReleaseEndFile;

    public StatelessCommitRef(
        ShardId shardId,
        Engine.IndexCommitRef indexCommitRef,
        Collection<String> commitFiles,
        Set<String> additionalFiles,
        long primaryTerm,
        long translogRecoveryStartFile,
        long translogReleaseEndFile
    ) {
        super(indexCommitRef.getIndexCommit());
        this.shardId = Objects.requireNonNull(shardId);
        this.indexCommitRef = indexCommitRef;
        this.commitFiles = commitFiles;
        this.additionalFiles = Objects.requireNonNull(additionalFiles);
        this.primaryTerm = primaryTerm;
        this.translogRecoveryStartFile = translogRecoveryStartFile;
        this.translogReleaseEndFile = translogReleaseEndFile;
        this.released = new AtomicBoolean();
        assert translogReleaseEndFile < 0 || translogRecoveryStartFile == translogReleaseEndFile || isHollow()
            : "translog start file for cleaning ("
                + translogReleaseEndFile
                + ") must be the same as translog recovery start file ("
                + translogRecoveryStartFile
                + ") for non-hollow commits or negative (ineffective)";
        assert translogReleaseEndFile != HOLLOW_TRANSLOG_RECOVERY_START_FILE
            : translogReleaseEndFile + " == " + HOLLOW_TRANSLOG_RECOVERY_START_FILE;
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

    public long getTranslogReleaseEndFile() {
        return translogReleaseEndFile;
    }

    public boolean isHollow() {
        return getTranslogRecoveryStartFile() == HOLLOW_TRANSLOG_RECOVERY_START_FILE;
    }

    @Override
    public String toString() {
        return "StatelessCommitRef(" + shardId + ',' + primaryTerm + "," + in.toString() + ')';
    }
}
