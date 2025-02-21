/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.support.CacheIteratorHelper;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.ToLongBiFunction;

/**
 * Wrapper around a {@link Cache} instance where a composite key of the original key and the current project id, resolved through a
 * {@link ProjectResolver}, is used to write to and read from the cache.
 * <p>
 * During invalidation the cache is protected through locking in the {@link CacheIteratorHelper} because the result of iteration under any
 * mutation other than Cache.CacheIterator#remove() is undefined. Concurrent writes are allowed as long as there is no active
 * invalidation, the cache is protected against that by acquiring a read lock (blocking if invalidation in progress) before writing.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ProjectScopedCache<K, V> {
    private final Cache<ProjectScoped<K>, V> cache;
    private final ProjectResolver projectResolver;
    private final CacheIteratorHelper<ProjectScoped<K>, V> cacheIteratorHelper;

    // Visible for testing
    ProjectScopedCache(ProjectResolver projectResolver, Cache<ProjectScoped<K>, V> cache) {
        this.projectResolver = projectResolver;
        this.cache = cache;
        cacheIteratorHelper = new CacheIteratorHelper<>(cache);
    }

    public void invalidateProject() {
        if (projectResolver.supportsMultipleProjects()) {
            invalidateProject(projectResolver.getProjectId());
        } else {
            cache.invalidateAll();
        }
    }

    public void invalidateProject(ProjectId projectId) {
        cacheIteratorHelper.removeKeysIf(key -> key.projectId().equals(projectId));
    }

    public void invalidate(K key) {
        invalidate(projectResolver.getProjectId(), key);
    }

    public void invalidate(ProjectId projectId, K key) {
        try (ReleasableLock ignored = cacheIteratorHelper.acquireUpdateLock()) {
            cache.invalidate(new ProjectScoped<>(projectId, key));
        }
    }

    public void invalidate(K key, V value) {
        try (ReleasableLock ignored = cacheIteratorHelper.acquireUpdateLock()) {
            cache.invalidate(new ProjectScoped<>(projectResolver.getProjectId(), key), value);
        }
    }

    public void invalidateAll() {
        try (ReleasableLock ignored = cacheIteratorHelper.acquireUpdateLock()) {
            cache.invalidateAll();
        }
    }

    public V computeIfAbsent(K key, CacheLoader<K, V> loader) throws ExecutionException {
        try (var ignored = cacheIteratorHelper.acquireUpdateLock()) {
            return cache.computeIfAbsent(new ProjectScoped<>(projectResolver.getProjectId(), key), k -> loader.load(k.value));
        }
    }

    public long weight() {
        return cache.weight();
    }

    public int count() {
        return cache.count();
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    public static class Builder<K, V> {
        private long maximumWeight;
        private TimeValue expireAfterAccessNanos;
        private TimeValue expireAfterWrite;
        private ToLongBiFunction<K, V> weigher;
        private RemovalListener<K, V> removalListener;

        private Builder() {}

        public Builder<K, V> setMaximumWeight(long maximumWeight) {
            if (maximumWeight < 0) {
                throw new IllegalArgumentException("maximumWeight < 0");
            }
            this.maximumWeight = maximumWeight;
            return this;
        }

        public Builder<K, V> setExpireAfterAccess(TimeValue expireAfterAccess) {
            Objects.requireNonNull(expireAfterAccess);
            final long expireAfterAccessNanos = expireAfterAccess.getNanos();
            if (expireAfterAccessNanos <= 0) {
                throw new IllegalArgumentException("expireAfterAccess <= 0");
            }
            this.expireAfterAccessNanos = expireAfterAccess;
            return this;
        }

        public Builder<K, V> setExpireAfterWrite(TimeValue expireAfterWrite) {
            Objects.requireNonNull(expireAfterWrite);
            final long expireAfterWriteNanos = expireAfterWrite.getNanos();
            if (expireAfterWriteNanos <= 0) {
                throw new IllegalArgumentException("expireAfterWrite <= 0");
            }
            this.expireAfterWrite = expireAfterWrite;
            return this;
        }

        public Builder<K, V> weigher(ToLongBiFunction<K, V> weigher) {
            Objects.requireNonNull(weigher);
            this.weigher = weigher;
            return this;
        }

        public Builder<K, V> removalListener(RemovalListener<K, V> removalListener) {
            Objects.requireNonNull(removalListener);
            this.removalListener = removalListener;
            return this;
        }

        public ProjectScopedCache<K, V> build(ProjectResolver projectResolver) {
            CacheBuilder<ProjectScoped<K>, V> cacheBuilder = CacheBuilder.builder();

            if (maximumWeight != -1) {
                cacheBuilder.setMaximumWeight(maximumWeight);
            }
            if (expireAfterAccessNanos != null) {
                cacheBuilder.setExpireAfterAccess(expireAfterAccessNanos);
            }
            if (expireAfterWrite != null) {
                cacheBuilder.setExpireAfterWrite(expireAfterWrite);
            }
            if (weigher != null) {
                cacheBuilder.weigher((key, value) -> weigher.applyAsLong(key.value, value));
            }
            if (removalListener != null) {
                cacheBuilder.removalListener((notification) -> {
                    removalListener.onRemoval(
                        new RemovalNotification<>(notification.getKey().value, notification.getValue(), notification.getRemovalReason())
                    );
                });
            }
            return new ProjectScopedCache<>(projectResolver, cacheBuilder.build());
        }
    }

    private record ProjectScoped<T>(ProjectId projectId, T value) {
        private ProjectScoped(ProjectId projectId, T value) {
            this.projectId = Objects.requireNonNull(projectId);
            this.value = value;
        }
    }
}
