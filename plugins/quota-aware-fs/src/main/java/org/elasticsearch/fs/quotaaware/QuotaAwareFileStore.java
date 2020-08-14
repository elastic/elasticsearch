/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.fs.quotaaware;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;

/**
 * An implementation of {@link FileStore} that relies on
 * {@link QuotaAwareFileSystemProvider} for usage reporting. Other methods are
 * delegated to the backing instance of {@link FileStore}
 */
public final class QuotaAwareFileStore extends FileStore {

    private final FileStore backingFS;
    private final QuotaAwareFileSystemProvider provider;

    QuotaAwareFileStore(QuotaAwareFileSystemProvider provider, FileStore backingFS) {
        this.provider = provider;
        this.backingFS = backingFS;
    }

    @Override
    public String name() {
        return backingFS.name();
    }

    @Override
    public String type() {
        return backingFS.type();
    }

    @Override
    public boolean isReadOnly() {
        return backingFS.isReadOnly();
    }

    @Override
    public long getTotalSpace() throws IOException {
        return Math.min(provider.getTotal(), backingFS.getTotalSpace());
    }

    @Override
    public long getUsableSpace() throws IOException {
        return Math.min(provider.getRemaining(), backingFS.getUsableSpace());
    }

    @Override
    public long getUnallocatedSpace() throws IOException {
        // There is no point in telling users that the underlying
        // host has more capacity than what they're allowed to use so we limit
        // this one with remaining as well.
        return Math.min(provider.getRemaining(), backingFS.getUnallocatedSpace());
    }

    @Override
    public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
        return backingFS.supportsFileAttributeView(type);
    }

    @Override
    public boolean supportsFileAttributeView(String name) {
        return backingFS.supportsFileAttributeView(name);
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
        return backingFS.getFileStoreAttributeView(type);
    }

    @Override
    public Object getAttribute(String attribute) throws IOException {
        return backingFS.getAttribute(attribute);
    }

    @Override
    public String toString() {
        return "QuotaAwareFileStore(" + backingFS.toString() + ")";
    }
}
