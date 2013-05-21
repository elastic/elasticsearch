/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.lucene.util;

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/** A mock {@link Directory} impl that tracks open files. */
public class MockDirectoryWrapper extends Directory {

    private final Directory delegate;
    private final Multiset<String> openFiles;

    public MockDirectoryWrapper(Directory delegate) {
        this.delegate = delegate;
        this.openFiles = HashMultiset.create();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        if (!openFiles.isEmpty()) {
            throw new IllegalStateException("There are still open files!!! " + openFiles);
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        // TODO Auto-generated method stub
        return new MockIndexOutput(name, delegate.createOutput(name, context));
    }

    @Override
    public void deleteFile(String name) throws IOException {
        delegate.deleteFile(name);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return delegate.fileExists(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return delegate.fileLength(name);
    }

    @Override
    public String[] listAll() throws IOException {
        return delegate.listAll();
    }

    @Override
    public Lock makeLock(String name) {
        return delegate.makeLock(name);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return new MockIndexInput(name, delegate.openInput(name, context));
    }

    @Override
    public void sync(Collection<String> files) throws IOException {
        delegate.sync(files);
    }

    private class MockIndexInput extends IndexInput {

        private final String name;
        private final IndexInput delegate;

        protected MockIndexInput(String name, IndexInput delegate) {
            super(name);
            this.name = name;
            openFiles.add(name);
            this.delegate = delegate;
        }

        @Override
        public void close() throws IOException {
            try {
                delegate.close();
            } finally {
                openFiles.remove(name);
            }
        }

        @Override
        public long getFilePointer() {
            return delegate.getFilePointer();
        }

        @Override
        public long length() {
            return delegate.length();
        }

        @Override
        public void seek(long pointer) throws IOException {
            delegate.seek(pointer);
        }

        @Override
        public byte readByte() throws IOException {
            return delegate.readByte();
        }

        @Override
        public void readBytes(byte[] b, int o, int l) throws IOException {
            delegate.readBytes(b, o, l);
        }

        @Override
        public IndexInput clone() {
            // Clones don't need to be closed, nothing to track
            return delegate.clone();
        }

    }

    private class MockIndexOutput extends IndexOutput {

        private final String name;
        private final IndexOutput delegate;

        MockIndexOutput(String name, IndexOutput delegate) {
            super();
            this.name = name;
            openFiles.add(name);
            this.delegate = delegate;
        }

        @Override
        public void close() throws IOException {
            try {
                delegate.close();
            } finally {
                openFiles.remove(name);
            }
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public long getFilePointer() {
            return delegate.getFilePointer();
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        @Deprecated
        public void seek(long pointer) throws IOException {
            delegate.seek(pointer);
        }

        @Override
        public void writeByte(byte b) throws IOException {
            delegate.writeByte(b);
        }

        @Override
        public void writeBytes(byte[] b, int o, int l) throws IOException {
            delegate.writeBytes(b, o, l);
        }

    }

    @Override
    public String toString() {
        return "MockDirectoryWrapper[" + delegate + "]";
    }

}
