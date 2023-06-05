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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.store.BytesReferenceIndexInput;
import org.elasticsearch.core.Streams;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A {@link Directory} from which you can only read segment info blobs, which is enough to get the latest {@link SegmentInfos}. The
 * downloaded blobs are assumed to be small, and are held in memory.
 */
public class SegmentInfoCachingDirectory extends BaseDirectory {

    private final BlobContainer blobContainer;
    private final StatelessCompoundCommit compoundCommit;
    private final Map<String, BytesReference> blobContentsByName = new HashMap<>();

    public SegmentInfoCachingDirectory(BlobContainer blobContainer, StatelessCompoundCommit compoundCommit) {
        super(NoLockFactory.INSTANCE);
        this.blobContainer = blobContainer;
        this.compoundCommit = compoundCommit;
    }

    @Override
    public String[] listAll() {
        return compoundCommit.commitFiles().keySet().toArray(new String[0]);
    }

    @Override
    public void deleteFile(String name) {
        assert false;
        throw new UnsupportedOperationException();
    }

    @Override
    public long fileLength(String name) throws IOException {
        var blobLocation = compoundCommit.commitFiles().get(name);
        if (blobLocation == null) {
            throw new FileNotFoundException(name);
        }
        return blobLocation.fileLength();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        assert false;
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        assert false;
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) {
        assert false;
        throw new UnsupportedOperationException();
    }

    @Override
    public void syncMetaData() {
        assert false;
        throw new UnsupportedOperationException();
    }

    @Override
    public void rename(String source, String dest) {
        assert false;
        throw new UnsupportedOperationException();
    }

    private static boolean isCached(String name) {
        return name.startsWith(IndexFileNames.SEGMENTS) || IndexFileNames.matchesExtension(name, "si");
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (isCached(name) == false) {
            assert false : name;
            throw new UnsupportedOperationException(name);
        }
        final var blobLocation = compoundCommit.commitFiles().get(name);
        if (blobLocation == null) {
            throw new FileNotFoundException(name);
        }
        try {
            return new BytesReferenceIndexInput(name, blobContentsByName.computeIfAbsent(name, n -> {
                try (
                    var bso = new BytesStreamOutput(Math.toIntExact(blobLocation.fileLength()));
                    var inputStream = blobContainer.readBlob(blobLocation.blobName(), blobLocation.offset(), blobLocation.fileLength())
                ) {
                    Streams.copy(inputStream, bso, false);
                    return bso.bytes();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }));
        } catch (UncheckedIOException e) {
            if (e.getCause() != null) {
                throw e.getCause();
            } else {
                throw e;
            }
        }
    }

    @Override
    public void close() {}

    @Override
    public Set<String> getPendingDeletions() {
        return Set.of();
    }
}
