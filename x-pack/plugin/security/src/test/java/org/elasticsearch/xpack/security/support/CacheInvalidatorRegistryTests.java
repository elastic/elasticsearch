/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry.CacheInvalidator;
import org.junit.Before;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CacheInvalidatorRegistryTests extends ESTestCase {

    private CacheInvalidatorRegistry cacheInvalidatorRegistry;

    @Before
    public void setup() {
        cacheInvalidatorRegistry = new CacheInvalidatorRegistry();
    }

    public void testRegistryWillNotAllowInvalidatorsWithDuplicatedName() {
        cacheInvalidatorRegistry.registerCacheInvalidator("service1", mock(CacheInvalidator.class));
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> cacheInvalidatorRegistry.registerCacheInvalidator("service1", mock(CacheInvalidator.class)));
        assertThat(e.getMessage(), containsString("already has an entry with name: [service1]"));
    }

    public void testSecurityIndexStateChangeWillInvalidateAllRegisteredInvalidators() {
        final CacheInvalidator invalidator1 = mock(CacheInvalidator.class);
        when(invalidator1.shouldClearOnSecurityIndexStateChange()).thenReturn(true);
        cacheInvalidatorRegistry.registerCacheInvalidator("service1", invalidator1);
        final CacheInvalidator invalidator2 = mock(CacheInvalidator.class);
        when(invalidator2.shouldClearOnSecurityIndexStateChange()).thenReturn(true);
        cacheInvalidatorRegistry.registerCacheInvalidator("service2", invalidator2);
        final CacheInvalidator invalidator3 = mock(CacheInvalidator.class);
        cacheInvalidatorRegistry.registerCacheInvalidator("service3", invalidator3);

        final SecurityIndexManager.State previousState = SecurityIndexManager.State.UNRECOVERED_STATE;
        final SecurityIndexManager.State currentState = new SecurityIndexManager.State(
            Instant.now(), true, true, true, Version.CURRENT,
            ".security", ClusterHealthStatus.GREEN, IndexMetadata.State.OPEN, null, "my_uuid");

        cacheInvalidatorRegistry.onSecurityIndexStateChange(previousState, currentState);
        verify(invalidator1).invalidateAll();
        verify(invalidator2).invalidateAll();
        verify(invalidator3, never()).invalidateAll();
    }

    public void testInvalidateByKeyCallsCorrectInvalidatorObject() {
        final CacheInvalidator invalidator1 = mock(CacheInvalidator.class);
        cacheInvalidatorRegistry.registerCacheInvalidator("service1", invalidator1);
        final CacheInvalidator invalidator2 = mock(CacheInvalidator.class);
        cacheInvalidatorRegistry.registerCacheInvalidator("service2", invalidator2);

        cacheInvalidatorRegistry.invalidateByKey("service2", List.of("k1", "k2"));
        verify(invalidator1, never()).invalidate(any());
        verify(invalidator2).invalidate(List.of("k1", "k2"));

        // Trying to invalidate entries from a non-existing cache will throw error
        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class,
                () -> cacheInvalidatorRegistry.invalidateByKey("non-exist", List.of("k1", "k2")));
        assertThat(e.getMessage(), containsString("No cache named [non-exist] is found"));
    }

    public void testInvalidateCache() {
        final CacheInvalidator invalidator1 = mock(CacheInvalidator.class);
        cacheInvalidatorRegistry.registerCacheInvalidator("service1", invalidator1);
        final CacheInvalidator invalidator2 = mock(CacheInvalidator.class);
        cacheInvalidatorRegistry.registerCacheInvalidator("service2", invalidator2);

        cacheInvalidatorRegistry.invalidateCache("service1");
        verify(invalidator1).invalidateAll();
        verify(invalidator2, never()).invalidateAll();

        // Trying to invalidate entries from a non-existing cache will throw error
        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class,
                () -> cacheInvalidatorRegistry.invalidateCache("non-exist"));
        assertThat(e.getMessage(), containsString("No cache named [non-exist] is found"));
    }

    public void testRegisterAlias() {
        final CacheInvalidator invalidator1 = mock(CacheInvalidator.class);
        cacheInvalidatorRegistry.registerCacheInvalidator("cache1", invalidator1);
        final CacheInvalidator invalidator2 = mock(CacheInvalidator.class);
        cacheInvalidatorRegistry.registerCacheInvalidator("cache2", invalidator2);

        final NullPointerException e1 =
            expectThrows(NullPointerException.class, () -> cacheInvalidatorRegistry.registerAlias(null, Set.of()));
        assertThat(e1.getMessage(), containsString("cache alias cannot be null"));

        final IllegalArgumentException e2 =
            expectThrows(IllegalArgumentException.class, () -> cacheInvalidatorRegistry.registerAlias("alias1", Set.of()));
        assertThat(e2.getMessage(), containsString("cache names cannot be empty for aliasing"));

        cacheInvalidatorRegistry.registerAlias("alias1", randomFrom(Set.of("cache1"), Set.of("cache1", "cache2")));

        final IllegalArgumentException e3 =
            expectThrows(IllegalArgumentException.class, () -> cacheInvalidatorRegistry.registerAlias("alias1", Set.of("cache1")));
        assertThat(e3.getMessage(), containsString("cache alias already exists"));

        // validation should pass
        cacheInvalidatorRegistry.validate();
    }

    public void testValidateWillThrowForClashingAliasAndCacheNames() {
        final CacheInvalidator invalidator1 = mock(CacheInvalidator.class);
        cacheInvalidatorRegistry.registerCacheInvalidator("cache1", invalidator1);
        cacheInvalidatorRegistry.registerAlias("cache1", Set.of("cache1"));
        final IllegalStateException e =
            expectThrows(IllegalStateException.class, () -> cacheInvalidatorRegistry.validate());
        assertThat(e.getMessage(), containsString("cache alias cannot clash with cache name"));
    }

    public void testValidateWillThrowForNotFoundCacheNames() {
        final CacheInvalidator invalidator1 = mock(CacheInvalidator.class);
        cacheInvalidatorRegistry.registerCacheInvalidator("cache1", invalidator1);
        cacheInvalidatorRegistry.registerAlias("alias1", Set.of("cache1", "cache2"));
        final IllegalStateException e =
            expectThrows(IllegalStateException.class, () -> cacheInvalidatorRegistry.validate());
        assertThat(e.getMessage(), containsString("cache names not found: [cache2]"));
    }
}
