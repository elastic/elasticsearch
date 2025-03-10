/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.ProjectId.fromId;
import static org.hamcrest.Matchers.equalTo;

public class ProjectScopedCacheTests extends ESTestCase {
    private final AtomicReference<ProjectId> activeProjectId = new AtomicReference<>();
    private ProjectScopedCache<String, String> projectScopedCache;
    private CacheLoader<String, String> loader;
    private final ProjectResolver projectResolver = new ProjectResolver() {
        @Override
        public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProjectId getProjectId() {
            return activeProjectId.get();
        }

        @Override
        public boolean supportsMultipleProjects() {
            return true;
        }
    };

    @Before
    public void initCache() {
        activeProjectId.set(fromId("test-project"));
        projectScopedCache = ProjectScopedCache.<String, String>builder().setMaximumWeight(100).build(projectResolver);
        loader = key -> "value-" + key;
    }

    public void testComputeIfAbsent() throws ExecutionException {
        activeProjectId.set(fromId("test-project"));
        String value = projectScopedCache.computeIfAbsent("key1", loader);
        assertThat(value, equalTo("value-key1"));
        assertThat(projectScopedCache.count(), equalTo(1));
    }

    public void testInvalidateKey() throws ExecutionException {
        activeProjectId.set(fromId("test-project"));
        String value = projectScopedCache.computeIfAbsent("key1", loader);
        assertThat(value, equalTo("value-key1"));
        assertThat(projectScopedCache.count(), equalTo(1));

        projectScopedCache.invalidate("key1");
        assertThat(projectScopedCache.count(), equalTo(0));
    }

    public void testInvalidateProject() throws ExecutionException {
        activeProjectId.set(fromId("test-project"));
        projectScopedCache.computeIfAbsent("key1", loader);
        activeProjectId.set(fromId("other-test-project"));
        projectScopedCache.computeIfAbsent("key1", loader);

        assertThat(projectScopedCache.count(), equalTo(2));
        projectScopedCache.invalidateProject();
        assertThat(projectScopedCache.count(), equalTo(1));

        activeProjectId.set(fromId("test-project"));
        projectScopedCache.invalidateProject();
        assertThat(projectScopedCache.count(), equalTo(0));
    }

    public void testInvalidateAll() throws ExecutionException {
        activeProjectId.set(fromId("test-project"));
        projectScopedCache.computeIfAbsent("key1", loader);
        activeProjectId.set(fromId("other-test-project"));
        projectScopedCache.computeIfAbsent("key1", loader);
        projectScopedCache.invalidateAll();
        assertThat(projectScopedCache.count(), equalTo(0));
    }

    public void testRemovalListener() throws ExecutionException {
        final AtomicReference<String> removedKey = new AtomicReference<>();

        projectScopedCache = ProjectScopedCache.<String, String>builder()
            .setMaximumWeight(100)
            .removalListener((notification) -> removedKey.set(notification.getKey()))
            .build(projectResolver);

        activeProjectId.set(fromId("test-project"));
        projectScopedCache.computeIfAbsent("key1", loader);
        projectScopedCache.invalidate("key1");
        assertThat(removedKey.get(), equalTo("key1"));
    }

    public void testWeigher() throws ExecutionException {
        int numberOfEntries = randomIntBetween(2, 10);
        int maximumWeight = 2 * numberOfEntries;
        int weight = randomIntBetween(2, 10);
        AtomicLong evictions = new AtomicLong();

        projectScopedCache = ProjectScopedCache.<String, String>builder()
            .setMaximumWeight(maximumWeight)
            .weigher((k, v) -> weight)
            .removalListener(notification -> evictions.incrementAndGet())
            .build(projectResolver);

        for (int i = 0; i < numberOfEntries; i++) {
            projectScopedCache.computeIfAbsent(Integer.toString(i), loader);
        }
        // cache weight should be the largest multiple of weight less than maximumWeight
        assertEquals(weight * (maximumWeight / weight), projectScopedCache.weight());

        // the number of evicted entries should be the number of entries that fit in the excess weight
        assertEquals((int) Math.ceil((weight - 2) * numberOfEntries / (1.0 * weight)), evictions.get());
    }
}
