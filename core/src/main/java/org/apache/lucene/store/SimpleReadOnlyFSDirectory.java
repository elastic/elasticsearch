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
package org.apache.lucene.store;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;

/**
 * Similar to {@link SimpleFSDirectory} but only supports reading files from a folder and does not create the directory if it does not
 * exist. This is useful if we don't want to resurrect a folder that was just deleted before creating the {@link Directory}.
 *
 * Only supports the {@link Directory#openInput(String,IOContext)} method.
 */
public class SimpleReadOnlyFSDirectory extends BaseDirectory {

    protected final Path directory; // The underlying filesystem directory

    public SimpleReadOnlyFSDirectory(Path path) throws IOException {
        super(FSLockFactory.getDefault());
        directory = path.toRealPath();
    }

    @Override
    public String[] listAll() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long fileLength(String name) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameFile(String source, String dest) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        Path path = directory.resolve(name);
        SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ);
        return new SimpleFSDirectory.SimpleFSIndexInput("SimpleFSIndexInput(path=\"" + path + "\")", channel, context);
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
    }
}
