/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store.smb;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 * This class is used to wrap an existing {@link org.apache.lucene.store.FSDirectory} so that
 * the new shard segment files will be opened for Read and Write access.
 * <p>
 * When storing index files on an SMB share like Azure File Service, opening the file for Read
 * access can save a lot of roundtrips to the storage server and thus offering better performance.
 */
public final class SmbDirectoryWrapper extends FilterDirectory {

    private final FSDirectory fsDirectory;

    public SmbDirectoryWrapper(FSDirectory in) {
        super(in);
        fsDirectory = in;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        this.ensureOpen();
        return new SmbFSIndexOutput(name);
    }

    final class SmbFSIndexOutput extends OutputStreamIndexOutput {
        /**
         * The maximum chunk size is 8192 bytes, because {@link java.io.FileOutputStream} mallocs
         * a native buffer outside of stack if the write buffer size is larger.
         */
        static final int CHUNK_SIZE = 8192;

        SmbFSIndexOutput(String name) throws IOException {
            super("SmbFSIndexOutput(path=\"" + fsDirectory.getDirectory().resolve(name) + "\")", name,
                    new FilterOutputStream(Channels.newOutputStream(Files.newByteChannel(fsDirectory.getDirectory().resolve(name),
                            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.READ, StandardOpenOption.WRITE))) {
                        // This implementation ensures, that we never write more than CHUNK_SIZE bytes:
                        @Override
                        public void write(byte[] b, int offset, int length) throws IOException {
                            while (length > 0) {
                                final int chunk = Math.min(length, CHUNK_SIZE);
                                out.write(b, offset, chunk);
                                length -= chunk;
                                offset += chunk;
                            }
                        }
                    },
                    CHUNK_SIZE);
        }
    }
}
