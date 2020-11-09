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

import org.elasticsearch.common.SuppressForbidden;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * An implementation of {@link FileSystem} that returns the given
 * {@link QuotaAwareFileSystemProvider} provider {@link #provider()}.
 *
 * Other methods are delegated to given instance of {@link FileSystem} and
 * wrapped where result types are either @link {@link QuotaAwareFileSystem}
 * or @link {@link QuotaAwareFileStore}.
 *
 */
public final class QuotaAwareFileSystem extends FileSystem {
    private final FileSystem delegate;
    private final QuotaAwareFileSystemProvider provider;

    QuotaAwareFileSystem(QuotaAwareFileSystemProvider provider, FileSystem delegate) {
        this.provider = Objects.requireNonNull(provider, "Provider is required");
        this.delegate = Objects.requireNonNull(delegate, "FileSystem is required");
    }

    @Override
    public QuotaAwareFileSystemProvider provider() {
        return provider;
    }

    @Override
    @SuppressForbidden(reason = "accesses the default filesystem by design")
    public void close() throws IOException {
        if (this == FileSystems.getDefault()) {
            throw new UnsupportedOperationException("The default file system cannot be closed");
        } else if (delegate != FileSystems.getDefault()) {
            delegate.close();
        }
        provider.purge(delegate);
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public boolean isReadOnly() {
        return delegate.isReadOnly();
    }

    @Override
    public String getSeparator() {
        return delegate.getSeparator();
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return StreamSupport.stream(delegate.getRootDirectories().spliterator(), false).map((Function<Path, Path>) this::wrap)::iterator;
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return StreamSupport.stream(delegate.getFileStores().spliterator(), false)
            .map((Function<FileStore, FileStore>) provider::getFileStore)::iterator;
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return delegate.supportedFileAttributeViews();
    }

    @Override
    public Path getPath(String first, String... more) {
        return wrap(delegate.getPath(first, more));
    }

    private QuotaAwarePath wrap(Path delegatePath) {
        if (delegatePath == null) return null;
        else return new QuotaAwarePath(this, delegatePath);
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        PathMatcher matcher = delegate.getPathMatcher(syntaxAndPattern);
        return (path) -> matcher.matches(QuotaAwarePath.unwrap(path));
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        return delegate.getUserPrincipalLookupService();
    }

    @Override
    public WatchService newWatchService() throws IOException {
        return delegate.newWatchService();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        QuotaAwareFileSystem other = (QuotaAwareFileSystem) obj;
        if (!delegate.equals(other.delegate)) return false;
        if (!provider.equals(other.provider)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

}
