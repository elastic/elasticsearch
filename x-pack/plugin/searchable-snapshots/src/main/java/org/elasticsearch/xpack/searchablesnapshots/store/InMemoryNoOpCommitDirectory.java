/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.store;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.store.ImmutableDirectoryException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * A {@link Directory} which wraps a read-only "real" directory with a wrapper that allows no-op (in-memory) commits, and peer recoveries
 * of the same, so that we can start a shard on a completely readonly data set.
 */
class InMemoryNoOpCommitDirectory extends FilterDirectory {

    private final Directory realDirectory;
    private final Set<String> deletedFiles = new CopyOnWriteArraySet<>();

    InMemoryNoOpCommitDirectory(Directory realDirectory) {
        super(new ByteBuffersDirectory(NoLockFactory.INSTANCE));
        this.realDirectory = realDirectory;
    }

    public Directory getRealDirectory() {
        return realDirectory;
    }

    @Override
    public String[] listAll() throws IOException {
        final String[] ephemeralFiles = in.listAll();
        final String[] realFiles = Arrays.stream(realDirectory.listAll())
            .filter(f -> deletedFiles.contains(f) == false)
            .toArray(String[]::new);
        final String[] allFiles = new String[ephemeralFiles.length + realFiles.length];
        System.arraycopy(ephemeralFiles, 0, allFiles, 0, ephemeralFiles.length);
        System.arraycopy(realFiles, 0, allFiles, ephemeralFiles.length, realFiles.length);
        return allFiles;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureMutable(name);
        // remember that file got deleted, and blend it out when files are listed
        try {
            in.deleteFile(name);
        } catch (NoSuchFileException | FileNotFoundException e) {
            // cannot delete the segments_N file in the read-only directory, but that's ok, just ignore this
        }
        deletedFiles.add(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        try {
            return in.fileLength(name);
        } catch (NoSuchFileException | FileNotFoundException e) {
            return realDirectory.fileLength(name);
        }
    }

    @Override
    public void sync(Collection<String> names) {}

    @Override
    public void syncMetaData() {}

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureMutable(name);
        assert notOverwritingRealSegmentsFile(name) : name;
        deletedFiles.remove(name);
        return super.createOutput(name, context);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        ensureMutable(source);
        ensureMutable(dest);
        assert notOverwritingRealSegmentsFile(dest) : dest;
        super.rename(source, dest);
        deletedFiles.remove(dest);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        try {
            return in.openInput(name, context);
        } catch (NoSuchFileException | FileNotFoundException e) {
            return realDirectory.openInput(name, context);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(in, realDirectory);
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return super.getPendingDeletions(); // read-only realDirectory has no pending deletions
    }

    private static void ensureMutable(String name) {
        if ((name.startsWith("segments_")
            || name.startsWith("pending_segments_")
            || name.matches("^recovery\\..*\\.segments_.*$")) == false) {

            throw new ImmutableDirectoryException("file [" + name + "] is not mutable");
        }
    }

    private boolean notOverwritingRealSegmentsFile(String name) throws IOException {
        return name.startsWith("segments_") == false || Arrays.stream(realDirectory.listAll()).noneMatch(s -> s.equals(name));
    }

    @Override
    public String toString() {
        return "InMemoryNoOpCommitDirectory(" + "real=" + realDirectory + ", delegate=" + in + '}';
    }
}
