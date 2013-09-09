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

package org.apache.lucene.store;

import java.io.IOException;
import java.util.Collection;

public class FilterDirectory extends Directory {

    protected final Directory in;

    public FilterDirectory(Directory in) {
        this.in = in;
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return in.listAll();
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        ensureOpen();
        return in.fileExists(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        in.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        return in.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        return in.createOutput(name, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        ensureOpen();
        in.sync(names);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        return in.openInput(name, context);
    }

    @Override
    public Lock makeLock(String name) {
        ensureOpen();
        return in.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
        ensureOpen();
        in.clearLock(name);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        ensureOpen();
        in.setLockFactory(lockFactory);
    }

    @Override
    public LockFactory getLockFactory() {
        ensureOpen();
        return in.getLockFactory();
    }

    @Override
    public String getLockID() {
        ensureOpen();
        return in.getLockID();
    }

    @Override
    public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
        ensureOpen();
        in.copy(to, src, dest, context);
    }

    @Override
    public Directory.IndexInputSlicer createSlicer(final String name, final IOContext context) throws IOException {
        ensureOpen();
        return in.createSlicer(name, context);
    }

    public Directory getDelegate() {
        ensureOpen();
       return in;
    }
    
    @SuppressWarnings("resource")
    final Directory getLeafDirectory() {
        Directory current = getDelegate();
        while ((current instanceof FilterDirectory)) {
            current = ((FilterDirectory) current).getDelegate();
        }
        return current;
    }
    public static <T extends Directory> T getLeaf(Directory dir, Class<T> targetClass) {
        return getLeaf(dir, targetClass, null);
    }
    
    public static <T extends Directory> T getLeaf(Directory dir, Class<T> targetClass, T defaultValue) {
        Directory d = dir;
        if (dir instanceof FilterDirectory) {
            d = ((FilterDirectory) dir).getLeafDirectory();
        }
        if (targetClass.isAssignableFrom(d.getClass())) {
            return targetClass.cast(d);
        } else {
            return defaultValue;
        }
    }
    
    @Override
    public String toString() {
        return in.toString();
    }
}
