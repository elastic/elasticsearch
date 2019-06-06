/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.store;

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
