/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.security;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.store.RoleKey;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Compares the cache-hit path of the role cache behind {@code CompositeRolesStore#getRole} before and after the cache moved
 * from {@link Cache} to an access-ordered {@link LinkedHashMap} guarded by a single monitor.
 * <p>
 * Every authorization resolves the subject's role through this path, so it is the one place where the choice of data
 * structure and locking is on the hot path. Both implementations pay one global lock per hit: {@link Cache} takes the segment
 * read lock for the lookup and then its global LRU lock to promote the entry, while the monitor variant takes the monitor for
 * both. The benchmark measures how that difference behaves as threads are added. A {@link ConcurrentHashMap} without any LRU
 * bookkeeping is included as a floor; it is not a candidate because it cannot evict.
 * <p>
 * The {@code prehashed} variant computes the scoped key's hash before taking the monitor, while equality and LRU promotion
 * still run under it. This measures moving hashing out of the critical section, including the hash computation in each
 * measured lookup; it does not assume a hash cached during setup or change the production {@link RoleKey}.
 * <p>
 * Each thread cycles through the cached roles from its own starting offset, so with more than one role the threads promote
 * different entries rather than hammering one. With a single role the LRU promotion is a no-op in both implementations and
 * hashing and equality are still measured. Lookup keys and their name sets are equal to, but distinct from, the cached keys,
 * so hits cannot bypass set equality through object identity. All lookups are hits; the caches hold every role.
 * <p>
 * {@code roles} counts cache entries; {@code namesPerKey} counts role assignments combined in each entry. Names have fixed-seed
 * random suffixes: sequential numeric suffixes can cause substantial clustering in the JDK's immutable sets and confound key
 * size with collision cost. Role-key construction is outside the measurement. This benchmark does not measure cache misses
 * or invalidation scans.
 * <p>
 * Run, for example, with
 * {@code ../gradlew run --args "org.elasticsearch.benchmark.security.RoleCacheHitBenchmark.getRole_08 -p roles=1024 -p namesPerKey=100 -bm thrpt -tu s"}.
 * Larger keys can be selected with {@code -p namesPerKey=1000}.
 */
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class RoleCacheHitBenchmark {

    static {
        // Cache and the reserved roles create Elasticsearch loggers in static initializers, which need the logging provider
        BenchmarkLogging.configure();
    }

    /** Distinct cached roles that the threads cycle through. */
    @Param({ "1", "1000", "10000" })
    private int roles;

    /** Distinct role names combined into each cached role. */
    @Param({ "1", "50", "100" })
    private int namesPerKey;

    /**
     * {@code cache}: the previous implementation, {@link Cache}. {@code monitor}: the current implementation, a synchronized
     * access-ordered {@link LinkedHashMap}. {@code prehashed}: the same map with hashing before the monitor.
     * {@code concurrent}: a {@link ConcurrentHashMap} with no eviction, as a floor.
     */
    @Param({ "cache", "monitor", "prehashed", "concurrent" })
    private String impl;

    private RoleCache roleCache;
    private RoleKey[] keys;
    private ProjectId projectId;

    /** Per-thread cursor so that threads walk different roles rather than the same one. */
    @State(Scope.Thread)
    public static class Cursor {
        int index;

        @Setup
        public void setup(RoleCacheHitBenchmark benchmark, ThreadParams threadParams) {
            // spread the starting points over the key space so that concurrent threads touch different entries
            index = (int) (((long) threadParams.getThreadIndex() * benchmark.roles) / Math.max(1, threadParams.getThreadCount()));
        }
    }

    @Setup
    public void setup() {
        if (roles < 1 || namesPerKey < 1) {
            throw new IllegalArgumentException("roles and namesPerKey must be positive");
        }
        projectId = ProjectId.fromId("benchmark");
        keys = new RoleKey[roles];
        roleCache = switch (impl) {
            case "cache" -> new CacheRoleCache(roles);
            case "monitor" -> new MonitorRoleCache(roles);
            case "prehashed" -> new PrehashedRoleCache(roles);
            case "concurrent" -> new ConcurrentRoleCache();
            default -> throw new IllegalArgumentException("unknown impl [" + impl + "]");
        };
        final Random random = new Random(163644);
        for (int i = 0; i < roles; i++) {
            final String[] names = new String[namesPerKey];
            for (int j = 0; j < names.length; j++) {
                names[j] = "role-" + i + "-" + j + "-" + Long.toUnsignedString(random.nextLong(), 16);
            }
            final RoleKey cachedKey = new RoleKey(Set.copyOf(List.of(names)), RoleKey.ROLES_STORE_SOURCE);
            keys[i] = new RoleKey(Set.copyOf(List.of(names)), RoleKey.ROLES_STORE_SOURCE);
            if (keys[i].getNames() == cachedKey.getNames()) {
                throw new IllegalStateException("lookup and cached keys must have distinct name sets");
            }
            roleCache.put(projectId, cachedKey, Role.EMPTY);
        }
        selfTest();
    }

    /** Verifies that the configured implementation serves equal keys and isolates projects and sources before measuring it. */
    private void selfTest() {
        if (new PrehashedKey(projectId, keys[0]).hashCode() != new ProjectScoped<>(projectId, keys[0]).hashCode()) {
            throw new IllegalStateException("precomputed hashes must match the original scoped key hashes");
        }
        for (RoleKey key : keys) {
            if (roleCache.get(projectId, key) != Role.EMPTY) {
                throw new IllegalStateException("expected a cache hit for an equal lookup key");
            }
        }
        if (roleCache.get(ProjectId.fromId("other-project"), keys[0]) != null
            || roleCache.get(projectId, new RoleKey(keys[0].getNames(), "other-source")) != null) {
            throw new IllegalStateException("cache keys must distinguish projects and sources");
        }
    }

    private Role lookup(Cursor cursor) {
        final int i = cursor.index;
        cursor.index = i + 1 == roles ? 0 : i + 1;
        return roleCache.get(projectId, keys[i]);
    }

    @Benchmark
    @Threads(1)
    public Role getRole_01(Cursor cursor) {
        return lookup(cursor);
    }

    @Benchmark
    @Threads(2)
    public Role getRole_02(Cursor cursor) {
        return lookup(cursor);
    }

    @Benchmark
    @Threads(4)
    public Role getRole_04(Cursor cursor) {
        return lookup(cursor);
    }

    @Benchmark
    @Threads(8)
    public Role getRole_08(Cursor cursor) {
        return lookup(cursor);
    }

    @Benchmark
    @Threads(16)
    public Role getRole_16(Cursor cursor) {
        return lookup(cursor);
    }

    private interface RoleCache {
        Role get(ProjectId projectId, RoleKey roleKey);

        void put(ProjectId projectId, RoleKey roleKey, Role role);
    }

    /** The key type the role cache uses, reproduced here because the real one is private to its owner. */
    private record ProjectScoped<T>(ProjectId projectId, T value) {}

    /**
     * The previous implementation. A hit takes the segment's read lock for the lookup and then the cache's global LRU lock
     * to promote the entry.
     */
    private static final class CacheRoleCache implements RoleCache {
        private final Cache<ProjectScoped<RoleKey>, Role> cache;

        CacheRoleCache(int maximumSize) {
            final CacheBuilder<ProjectScoped<RoleKey>, Role> builder = CacheBuilder.builder();
            builder.setMaximumWeight(maximumSize);
            cache = builder.build();
        }

        @Override
        public Role get(ProjectId projectId, RoleKey roleKey) {
            return cache.get(new ProjectScoped<>(projectId, roleKey));
        }

        @Override
        public void put(ProjectId projectId, RoleKey roleKey, Role role) {
            cache.put(new ProjectScoped<>(projectId, roleKey), role);
        }
    }

    /** The current implementation. Every operation, including a hit, runs under the monitor. */
    private static final class MonitorRoleCache implements RoleCache {
        private final Map<ProjectScoped<RoleKey>, Role> map;

        MonitorRoleCache(int maximumSize) {
            map = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<ProjectScoped<RoleKey>, Role> eldest) {
                    return size() > maximumSize;
                }
            };
        }

        @Override
        public synchronized Role get(ProjectId projectId, RoleKey roleKey) {
            return map.get(new ProjectScoped<>(projectId, roleKey));
        }

        @Override
        public synchronized void put(ProjectId projectId, RoleKey roleKey, Role role) {
            map.put(new ProjectScoped<>(projectId, roleKey), role);
        }
    }

    /** Keeps the same key semantics while moving hash computation before the monitor. */
    private record PrehashedKey(ProjectId projectId, RoleKey roleKey, int hash) {
        private PrehashedKey(ProjectId projectId, RoleKey roleKey) {
            // Match the hash of ProjectScoped so the two map variants have the same bucket distribution.
            this(projectId, roleKey, 31 * projectId.hashCode() + roleKey.hashCode());
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /** An experimental monitor-backed cache with hash computation outside the critical section. */
    private static final class PrehashedRoleCache implements RoleCache {
        private final Map<PrehashedKey, Role> map;

        PrehashedRoleCache(int maximumSize) {
            map = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<PrehashedKey, Role> eldest) {
                    return size() > maximumSize;
                }
            };
        }

        @Override
        public Role get(ProjectId projectId, RoleKey roleKey) {
            final PrehashedKey key = new PrehashedKey(projectId, roleKey);
            synchronized (this) {
                return map.get(key);
            }
        }

        @Override
        public void put(ProjectId projectId, RoleKey roleKey, Role role) {
            final PrehashedKey key = new PrehashedKey(projectId, roleKey);
            synchronized (this) {
                map.put(key, role);
            }
        }
    }

    /** A floor with no LRU bookkeeping at all. Not a candidate, since it cannot bound its size. */
    private static final class ConcurrentRoleCache implements RoleCache {
        private final ConcurrentHashMap<ProjectScoped<RoleKey>, Role> map = new ConcurrentHashMap<>();

        @Override
        public Role get(ProjectId projectId, RoleKey roleKey) {
            return map.get(new ProjectScoped<>(projectId, roleKey));
        }

        @Override
        public void put(ProjectId projectId, RoleKey roleKey, Role role) {
            map.put(new ProjectScoped<>(projectId, roleKey), role);
        }
    }
}
