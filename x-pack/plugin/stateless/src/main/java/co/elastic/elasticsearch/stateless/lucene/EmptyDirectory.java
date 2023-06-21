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
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * In-memory directory containing an empty commit only. This is used in shards before they are initialized from the repository.
 * Note that since we create this instance statically and only once, all shards will initially have the same history uuid and Lucene
 * version until they are initialized from the repository.
 */
final class EmptyDirectory extends FilterDirectory {

    static final EmptyDirectory INSTANCE;
    static {
        final var directory = new ByteBuffersDirectory();
        var emptySegmentInfos = new SegmentInfos(IndexVersion.CURRENT.luceneVersion().major);
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
            emptySegmentInfos.commit(directory);
            INSTANCE = new EmptyDirectory(directory);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private final String segmentsFileName;

    private EmptyDirectory(Directory in) throws IOException {
        super(in);
        this.segmentsFileName = SegmentInfos.getLastCommitSegmentsFileName(in);
    }

    public String getSegmentsFileName() {
        return segmentsFileName;
    }

    @Override
    public void deleteFile(String name) {
        throw unsupportedException();
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
    public void rename(String source, String dest) {
        throw unsupportedException();
    }

    @Override
    public void syncMetaData() {
        throw unsupportedException();
    }

    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) {
        throw unsupportedException();
    }

    @Override
    public Set<String> getPendingDeletions() {
        throw unsupportedException();
    }

    private static UnsupportedOperationException unsupportedException() {
        assert false : "this operation is not supported and should have not be called";
        return new UnsupportedOperationException("EmptyDirectory does not support this operation");
    }
}
