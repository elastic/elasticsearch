/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.fs.quotaaware;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * A wrapper implementation of {@link Path} that ensures the given
 * {@link QuotaAwareFileSystem} is returned in {@link #getFileSystem()} from
 * this instance and any other {@link Path} returned from this.
 *
 */
public final class QuotaAwarePath implements Path {

    private final QuotaAwareFileSystem fileSystem;
    private final Path delegate;

    QuotaAwarePath(QuotaAwareFileSystem fileSystem, Path delegate) {
        if (delegate instanceof QuotaAwarePath) {
            throw new IllegalArgumentException("Nested quota wrappers are not supported.");
        }
        this.fileSystem = Objects.requireNonNull(fileSystem, "A filesystem is required");
        this.delegate = Objects.requireNonNull(delegate, "A delegate path is required");
    }

    @Override
    public FileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public boolean isAbsolute() {
        return delegate.isAbsolute();
    }

    @Override
    public Path getRoot() {
        return wrap(delegate.getRoot());
    }

    @Override
    public Path getFileName() {
        return wrap(delegate.getFileName());
    }

    private Path wrap(Path delegate) {
        if (delegate == null) {
            return null;
        } else {
            return new QuotaAwarePath(fileSystem, delegate);
        }
    }

    static Path unwrap(Path path) {
        if (path instanceof QuotaAwarePath) {
            return ((QuotaAwarePath) path).delegate;
        } else {
            return path;
        }
    }

    @Override
    public Path getParent() {
        return wrap(delegate.getParent());
    }

    @Override
    public int getNameCount() {
        return delegate.getNameCount();
    }

    @Override
    public Path getName(int index) {
        return wrap(delegate.getName(index));
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        return wrap(delegate.subpath(beginIndex, endIndex));
    }

    @Override
    public boolean startsWith(Path other) {
        return delegate.startsWith(unwrap(other));
    }

    @Override
    public boolean startsWith(String other) {
        return delegate.startsWith(other);
    }

    @Override
    public boolean endsWith(Path other) {
        return delegate.endsWith(unwrap(other));
    }

    @Override
    public boolean endsWith(String other) {
        return delegate.endsWith(other);
    }

    @Override
    public Path normalize() {
        return wrap(delegate.normalize());
    }

    @Override
    public Path resolve(Path other) {
        return wrap(delegate.resolve(unwrap(other)));
    }

    @Override
    public Path resolve(String other) {
        return wrap(delegate.resolve(other));
    }

    @Override
    public Path resolveSibling(Path other) {
        return wrap(delegate.resolveSibling(unwrap(other)));
    }

    @Override
    public Path resolveSibling(String other) {
        return wrap(delegate.resolveSibling(other));
    }

    @Override
    public Path relativize(Path other) {
        return wrap(delegate.relativize(unwrap(other)));
    }

    @Override
    public URI toUri() {
        return delegate.toUri();
    }

    @Override
    public Path toAbsolutePath() {
        return wrap(delegate.toAbsolutePath());
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        return wrap(delegate.toRealPath(options));
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>[] events, Modifier... modifiers) throws IOException {
        return delegate.register(watcher, events, modifiers);
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>... events) throws IOException {
        return delegate.register(watcher, events);
    }

    @Override
    public Iterator<Path> iterator() {
        return StreamSupport.stream(delegate.spliterator(), false).map(this::wrap).iterator();
    }

    @Override
    public int compareTo(Path other) {
        return delegate.compareTo(toDelegate(other));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        QuotaAwarePath other = (QuotaAwarePath) obj;
        if (delegate.equals(other.delegate) == false) return false;
        if (fileSystem.equals(other.fileSystem) == false) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    private Path toDelegate(Path path) {
        if (path instanceof QuotaAwarePath) {
            QuotaAwarePath qaPath = (QuotaAwarePath) path;
            if (qaPath.fileSystem != fileSystem) {
                throw new ProviderMismatchException(
                    "mismatch, expected: " + fileSystem.provider().getClass() + ", got: " + qaPath.fileSystem.provider().getClass()
                );
            }
            return qaPath.delegate;
        } else {
            throw new ProviderMismatchException("mismatch, expected: QuotaAwarePath, got: " + path.getClass());
        }
    }
}
