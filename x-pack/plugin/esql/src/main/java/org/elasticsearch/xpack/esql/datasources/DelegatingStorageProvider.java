/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * A {@link StorageProvider} that forwards every call to another one, so a decorator overrides only what it
 * decorates.
 * <p>
 * Forwarding by hand is what makes a decorator silently stop decorating. {@code StorageProvider} has defaulted
 * methods, and a default resolves through {@code this}, so a decorator that does not mention a method does not
 * reach the delegate's implementation of it — it runs the default against itself. Adding a method to the
 * interface therefore opts every decorator out of its own behaviour for that method, with nothing to read in the
 * decorator that says so.
 * <p>
 * Extending this does not make a missing decoration a compile error: an un-overridden method forwards, which is
 * correct but undecorated. What it changes is which mistake is silent, and it puts the overrides that do decorate
 * together in one class where their absence is visible.
 * <p>
 * {@link #delegate()} is a method rather than a field so a decorator can resolve its delegate lazily.
 */
public abstract class DelegatingStorageProvider implements StorageProvider {

    /** The provider every call not overridden below is forwarded to. */
    protected abstract StorageProvider delegate();

    @Override
    public StorageObject newObject(StoragePath path) {
        return delegate().newObject(path);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        return delegate().newObject(path, length);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        return delegate().newObject(path, length, lastModified);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified, String version) {
        return delegate().newObject(path, length, lastModified, version);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        return delegate().listObjects(prefix, recursive);
    }

    /**
     * Forwarded, because {@link StorageProvider}'s default drops the name prefix and lists the whole directory. A
     * wrapper that inherited that default would silently widen every narrowed listing passing through it, which is
     * a slowdown rather than a wrong answer and so would not show up as a failure.
     */
    @Override
    public StorageIterator listObjects(StoragePath prefix, String namePrefix, boolean recursive) throws IOException {
        return delegate().listObjects(prefix, namePrefix, recursive);
    }

    @Override
    public StorageChildren listChildren(StoragePath prefix, int limit) throws IOException {
        return delegate().listChildren(prefix, limit);
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        return delegate().exists(path);
    }

    @Override
    public StorageEntry objectMetadata(StoragePath path) throws IOException {
        return delegate().objectMetadata(path);
    }

    @Override
    public List<String> supportedSchemes() {
        return delegate().supportedSchemes();
    }

    @Override
    public boolean supportsStableMetadata() {
        return delegate().supportsStableMetadata();
    }

    @Override
    public boolean listsInKeyOrder() {
        return delegate().listsInKeyOrder();
    }

    /**
     * Deliberately not forwarded, and left abstract so the compiler asks. Every other call here has one safe
     * default — reach the delegate — but closing does not: a decorator that holds a pooled or leased provider has
     * to return it rather than close it, and a provider inherited by accident would be closed under the other
     * queries still holding it. Resource ownership is the one thing a decorator must state.
     */
    @Override
    public abstract void close() throws IOException;
}
