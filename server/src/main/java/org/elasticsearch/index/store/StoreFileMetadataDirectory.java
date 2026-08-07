/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A read-only {@link Directory} backed by a map of {@link StoreFileMetadata}. Only files whose contents are inlined
 * in the metadata hash ({@link StoreFileMetadata#hashEqualsContents()} {@code == true}) can be opened; all others
 * throw on {@link #openInput}. Used to read Lucene {@code SegmentInfos} from snapshot manifests without downloading
 * any segment data files.
 */
public final class StoreFileMetadataDirectory extends Directory {
    private final Map<String, StoreFileMetadata> files;

    public StoreFileMetadataDirectory(Map<String, StoreFileMetadata> files) {
        this.files = files;
    }

    @Override
    public String[] listAll() {
        return files.keySet().toArray(new String[0]);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final StoreFileMetadata metadata = getMetadata(name);
        if (metadata.hashEqualsContents() == false) {
            throw new IOException("Unable to open " + name);
        }
        final BytesRef data = metadata.hash();
        return new ByteArrayIndexInput(name, data.bytes, data.offset, data.length);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return getMetadata(name).length();
    }

    @Override
    public void close() {}

    @Override
    public void deleteFile(String name) {
        throw new UnsupportedOperationException("this directory is read-only");
    }

    @Override
    public void rename(String source, String dest) {
        throw new UnsupportedOperationException("this directory is read-only");
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        throw new UnsupportedOperationException("this directory is read-only");
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw new UnsupportedOperationException("this directory is read-only");
    }

    @Override
    public void sync(Collection<String> names) {
        throw new UnsupportedOperationException("this directory is read-only");
    }

    @Override
    public void syncMetaData() {
        throw new UnsupportedOperationException("this directory is read-only");
    }

    @Override
    public Set<String> getPendingDeletions() {
        throw new UnsupportedOperationException("this directory is read-only");
    }

    @Override
    public Lock obtainLock(String name) {
        throw new UnsupportedOperationException("this directory is read-only");
    }

    private StoreFileMetadata getMetadata(String name) throws IOException {
        final StoreFileMetadata metadata = files.get(name);
        if (metadata == null) {
            throw new FileNotFoundException(name);
        }
        return metadata;
    }
}
