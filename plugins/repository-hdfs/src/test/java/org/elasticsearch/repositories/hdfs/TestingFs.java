/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.lucene.util.LuceneTestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;

/**
 * Extends LFS to improve some operations to keep the security permissions at
 * bay. In particular it never tries to execute!
 */
public class TestingFs extends DelegateToFileSystem {

    // wrap hadoop rawlocalfilesystem to behave less crazy
    static RawLocalFileSystem wrap(final Path base) {
        final FileSystemProvider baseProvider = base.getFileSystem().provider();
        return new RawLocalFileSystem() {

            private org.apache.hadoop.fs.Path box(Path path) {
                return new org.apache.hadoop.fs.Path(path.toUri());
            }

            private Path unbox(org.apache.hadoop.fs.Path path) {
                return baseProvider.getPath(path.toUri());
            }

            @Override
            protected org.apache.hadoop.fs.Path getInitialWorkingDirectory() {
                return box(base);
            }

            @Override
            public void setPermission(org.apache.hadoop.fs.Path path, FsPermission permission) {
               // no execution, thank you very much!
            }

            // pretend we don't support symlinks (which causes hadoop to want to do crazy things),
            // returning the boolean does not seem to really help, link-related operations are still called.

            @Override
            public boolean supportsSymlinks() {
                return false;
            }

            @Override
            public FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path path) throws IOException {
                return getFileStatus(path);
            }

            @Override
            public org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path path) throws IOException {
                return path;
            }

            @Override
            public FileStatus getFileStatus(org.apache.hadoop.fs.Path path) throws IOException {
                BasicFileAttributes attributes;
                try {
                    attributes = Files.readAttributes(unbox(path), BasicFileAttributes.class);
                } catch (NoSuchFileException e) {
                    // unfortunately, specific exceptions are not guaranteed. don't wrap hadoop over a zip filesystem or something.
                    FileNotFoundException fnfe = new FileNotFoundException("File " + path + " does not exist");
                    fnfe.initCause(e);
                    throw fnfe;
                }

                // we set similar values to raw local filesystem, except we are never a symlink
                long length = attributes.size();
                boolean isDir = attributes.isDirectory();
                int blockReplication = 1;
                long blockSize = getDefaultBlockSize(path);
                long modificationTime = attributes.creationTime().toMillis();
                return new FileStatus(length, isDir, blockReplication, blockSize, modificationTime, path);
            }
        };
    }

    public TestingFs(URI uri, Configuration configuration) throws URISyntaxException, IOException {
        super(URI.create("file:///"), wrap(LuceneTestCase.createTempDir()), configuration, "file", false);
    }

    @Override
    public void checkPath(org.apache.hadoop.fs.Path path) {
      // we do evil stuff, we admit it.
    }
}
