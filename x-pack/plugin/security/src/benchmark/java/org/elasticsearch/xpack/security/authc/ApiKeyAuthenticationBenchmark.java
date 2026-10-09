/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyCredentials;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService.ApiKeyDoc;
import org.elasticsearch.xpack.security.authc.ApiKeyService.CachedApiKeyDoc;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Measures API key authentication when the API key document is already in the doc cache, which is the steady state of a node
 * that keeps authenticating the same API keys. The API keys use the default {@link Hasher#SSHA256} stored hash, so the whole
 * authentication completes on the calling thread and no request leaves the node.
 * <p>
 * That leaves the per-request cost of verifying the credentials, including the API key authentication cache wherever it is
 * used: the cache lookup, its segment and LRU list locks, and the listener plumbing around it. Run with several thread counts
 * ({@code -t}) to see lock contention and with {@code -prof gc} to see the allocations per authentication.
 */
@Fork(value = 1, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class ApiKeyAuthenticationBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final BytesReference ROLE_DESCRIPTORS = new BytesArray("""
        {"role":{"cluster":["monitor"],"indices":[{"names":["logs-*"],"privileges":["read"]}]}}""");
    private static final BytesReference LIMITED_BY_ROLE_DESCRIPTORS = new BytesArray("""
        {"owner_role":{"cluster":["manage_own_api_key"],"indices":[{"names":["*"],"privileges":["all"]}]}}""");

    /**
     * Fits into the API key caches with the default {@code xpack.security.authc.api_key.cache.max_keys} of 25,000, so every
     * authentication is served from the caches.
     */
    @Param({ "10000" })
    public int numApiKeys;

    private final Clock clock = Clock.systemUTC();
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private ApiKeyService apiKeyService;
    private ThreadContext threadContext;
    private ApiKeyCredentials[] credentials;

    @Setup
    public void setup() {
        final Settings settings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "benchmark")
            // the maximum TTL, so that cached docs cannot expire during a run
            .put(ApiKeyService.DOC_CACHE_TTL_SETTING.getKey(), "15m")
            .build();
        threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
        clusterService = new ClusterService(
            settings,
            new ClusterSettings(
                settings,
                Sets.union(
                    ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                    Set.of(ApiKeyService.DELETE_RETENTION_PERIOD, ApiKeyService.DELETE_INTERVAL)
                )
            ),
            threadPool,
            null
        );
        // The client and the security index are only used to load API key docs that are not cached and to manage API keys,
        // neither of which happens here.
        apiKeyService = new ApiKeyService(
            settings,
            clock,
            null,
            null,
            clusterService,
            new CacheInvalidatorRegistry(),
            threadPool,
            MeterRegistry.NOOP,
            new FeatureService(List.of())
        );
        threadContext = threadPool.getThreadContext();

        final Random random = new Random(0);
        credentials = new ApiKeyCredentials[numApiKeys];
        for (int i = 0; i < numApiKeys; i++) {
            final String id = UUIDs.randomBase64UUID(random);
            final SecureString secret = randomSecret(random);
            cacheDoc(id, buildDoc(i, secret));
            credentials[i] = new ApiKeyCredentials(id, secret, ApiKey.Type.REST);
        }
        // Authenticate every API key once, so that the authentication cache is warm wherever it is used.
        final ResultListener listener = new ResultListener();
        for (int i = 0; i < numApiKeys; i++) {
            authenticateApiKey(i, listener);
        }
    }

    @TearDown
    public void teardown() throws IOException {
        IOUtils.close(credentials);
        IOUtils.close(apiKeyService, clusterService);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    @Benchmark
    public AuthenticationResult<Tuple<User, ApiKeyDoc>> authenticate(PerThread perThread) {
        return authenticateApiKey(perThread.nextApiKey(), perThread.listener);
    }

    AuthenticationResult<Tuple<User, ApiKeyDoc>> authenticateApiKey(int apiKey, ResultListener listener) {
        apiKeyService.loadApiKeyAndValidateCredentials(threadContext, credentials[apiKey], listener);
        return listener.takeSuccess();
    }

    long docCacheMisses() {
        return apiKeyService.getDocCache().stats().getMisses();
    }

    /**
     * Puts the doc into the doc cache the same way {@link ApiKeyService} does after loading it from the security index.
     */
    private void cacheDoc(String id, ApiKeyDoc doc) {
        final CachedApiKeyDoc cachedDoc = doc.toCachedApiKeyDoc();
        apiKeyService.getDocCache().put(id, cachedDoc);
        apiKeyService.getRoleDescriptorsBytesCache().put(cachedDoc.roleDescriptorsHash, doc.roleDescriptorsBytes);
        apiKeyService.getRoleDescriptorsBytesCache().put(cachedDoc.limitedByRoleDescriptorsHash, doc.limitedByRoleDescriptorsBytes);
    }

    private ApiKeyDoc buildDoc(int apiKey, SecureString secret) {
        final Map<String, Object> creator = new HashMap<>();
        creator.put("principal", principal(apiKey));
        creator.put("full_name", null);
        creator.put("email", null);
        creator.put("metadata", Map.of());
        creator.put("realm", "native1");
        creator.put("realm_type", "native");
        return new ApiKeyDoc(
            "api_key",
            ApiKey.Type.REST,
            clock.millis(),
            -1L,
            false,
            null,
            new String(Hasher.SSHA256.hash(secret)),
            "api-key-" + apiKey,
            ApiKey.CURRENT_API_KEY_VERSION.version(),
            ROLE_DESCRIPTORS,
            LIMITED_BY_ROLE_DESCRIPTORS,
            creator,
            null,
            null
        );
    }

    static String principal(int apiKey) {
        return "user-" + apiKey;
    }

    /**
     * Same shape as the secrets of created API keys: 16 random bytes, Base64 encoded into 22 characters.
     */
    private static SecureString randomSecret(Random random) {
        final byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        return new SecureString(Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).toCharArray());
    }

    /**
     * Gives every thread its own order of the API keys, so that threads spread over all keys without sharing any state.
     */
    @State(Scope.Thread)
    public static class PerThread {
        final ResultListener listener = new ResultListener();
        private int[] apiKeys;
        private int next;

        @Setup
        public void setup(ApiKeyAuthenticationBenchmark benchmark, ThreadParams threadParams) {
            init(benchmark.numApiKeys, threadParams.getThreadIndex());
        }

        void init(int numApiKeys, long seed) {
            final List<Integer> order = new ArrayList<>(numApiKeys);
            for (int i = 0; i < numApiKeys; i++) {
                order.add(i);
            }
            Collections.shuffle(order, new Random(seed));
            apiKeys = order.stream().mapToInt(Integer::intValue).toArray();
            next = 0;
        }

        int nextApiKey() {
            final int apiKey = apiKeys[next];
            next = next + 1 == apiKeys.length ? 0 : next + 1;
            return apiKey;
        }
    }

    /**
     * Captures the result of an authentication, which must complete on the calling thread.
     */
    static final class ResultListener implements ActionListener<AuthenticationResult<Tuple<User, ApiKeyDoc>>> {
        private AuthenticationResult<Tuple<User, ApiKeyDoc>> result;
        private Exception failure;

        @Override
        public void onResponse(AuthenticationResult<Tuple<User, ApiKeyDoc>> result) {
            this.result = result;
        }

        @Override
        public void onFailure(Exception e) {
            this.failure = e;
        }

        AuthenticationResult<Tuple<User, ApiKeyDoc>> takeSuccess() {
            final AuthenticationResult<Tuple<User, ApiKeyDoc>> success = result;
            if (success == null || success.getStatus() != AuthenticationResult.Status.SUCCESS) {
                throw new IllegalStateException(
                    "expected a successful authentication on the calling thread, but got [" + success + "]",
                    failure
                );
            }
            result = null;
            return success;
        }
    }
}
