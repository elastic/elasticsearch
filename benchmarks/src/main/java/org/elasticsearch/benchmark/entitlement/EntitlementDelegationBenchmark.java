/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.entitlement;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.entitlement.runtime.delegation.ActiveDelegation;
import org.elasticsearch.entitlement.runtime.delegation.DelegationHolder;
import org.elasticsearch.entitlement.runtime.delegation.DelegationPolicy;
import org.elasticsearch.entitlement.runtime.delegation.DelegationRuntime;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyChecker;
import org.elasticsearch.entitlement.runtime.policy.PolicyCheckerImpl;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager.PolicyScope;
import org.elasticsearch.entitlement.runtime.policy.Scope;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ExitVMEntitlement;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ALL_UNNAMED;

/**
 * Measures the performance of entitlement checks that pass, with and without
 * {@link org.elasticsearch.entitlement.runtime.api.EntitlementDelegation}.
 * This class acts as "the plugin"; {@link BenchmarkCallerUtility} and {@link BenchmarkCreatorUtility} act as
 * "server code".
 * <p>
 * The benchmarks can be run on Java 21, where they exercise the {@code ThreadLocal} holder, or on Java 25 or later,
 * where they exercise the {@code ScopedValue} holder.
 * <p>
 * Like tests, benchmarks do not run with modules, so JDK, entitlement and server classes are identified by classloader,
 * package and scope instead of module. This may cost differently from production, but runs can still be compared.
 * <p>
 * {@link #selfTest()} runs before any iteration and checks that the delegating checks use entitlements.
 */
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
public class EntitlementDelegationBenchmark {

    private static final String PLUGIN_NAME = "bench-plugin";

    private static final PathLookup EMPTY_PATH_LOOKUP = new PathLookup() {
        @Override
        public Path pidFile() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Stream<Path> getBaseDirPaths(BaseDir baseDir) {
            return Stream.empty();
        }

        @Override
        public Stream<Path> resolveSettingPaths(BaseDir baseDir, String settingName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isPathOnDefaultFilesystem(Path path) {
            return true;
        }
    };

    static {
        BenchmarkLogging.configure();
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
    }

    private DelegationPolicy previousPolicy;
    private PolicyChecker checker;
    private BenchmarkCreatorUtility creatorUtility;

    @Setup
    public void setup() {
        var policyManager = new ClasspathPolicyManager(List.of(new ExitVMEntitlement()));
        checker = new PolicyCheckerImpl(Set.of(), PolicyManager.class.getModule(), policyManager, EMPTY_PATH_LOOKUP);
        previousPolicy = DelegationRuntime.policy();
        DelegationRuntime.install(policyManager.delegationPolicy());
        creatorUtility = new BenchmarkCreatorUtility();
    }

    @TearDown
    public void tearDown() {
        DelegationRuntime.install(previousPolicy);
    }

    /** Reads the active delegation. Every check by server code does this. */
    @Benchmark
    public ActiveDelegation holderRead() {
        return DelegationHolder.current();
    }

    /** Measures a check by plugin code. Delegation adds only the {@code canDelegate} lookup to it. Most checks are like this. */
    @Benchmark
    public void undelegatedCheck() {
        checker.checkEntitlementPresent(EntitlementDelegationBenchmark.class, ExitVMEntitlement.class);
    }

    /** Measures a check by server code that delegates to its caller. Each call walks the stack. */
    @Benchmark
    public void asCallerCheck() {
        BenchmarkCallerUtility.delegatedCheck(checker);
    }

    /** Measures a check by server code that delegates to the creator of the instance. */
    @Benchmark
    public void asCreatorCheck() {
        creatorUtility.delegatedCheck(checker);
    }

    /**
     * With a plugin that lacks the entitlement that server has, the delegating checks must fail and the others must pass.
     */
    static void selfTest() {
        var previous = DelegationRuntime.policy();
        try {
            var policyManager = new ClasspathPolicyManager(List.of());
            var pluginNotEntitled = new PolicyCheckerImpl(Set.of(), PolicyManager.class.getModule(), policyManager, EMPTY_PATH_LOOKUP);
            DelegationRuntime.install(policyManager.delegationPolicy());

            pluginNotEntitled.checkEntitlementPresent(BenchmarkCallerUtility.class, ExitVMEntitlement.class);
            expectNotEntitled(
                () -> pluginNotEntitled.checkEntitlementPresent(EntitlementDelegationBenchmark.class, ExitVMEntitlement.class)
            );
            expectNotEntitled(() -> BenchmarkCallerUtility.delegatedCheck(pluginNotEntitled));
            var creatorUtility = new BenchmarkCreatorUtility();
            expectNotEntitled(() -> creatorUtility.delegatedCheck(pluginNotEntitled));
        } finally {
            DelegationRuntime.install(previous);
        }
    }

    private static void expectNotEntitled(Runnable check) {
        try {
            check.run();
        } catch (RuntimeException e) {
            if (e.getClass().getSimpleName().equals("NotEntitledException")) {
                return;
            }
            throw e;
        }
        throw new AssertionError("expected the check to use the entitlements of [" + PLUGIN_NAME + "] and fail");
    }

    /**
     * Grants {@link ExitVMEntitlement} to server, and the given entitlements to the plugin, which is the code of
     * {@link EntitlementDelegationBenchmark}.
     */
    private static class ClasspathPolicyManager extends PolicyManager {
        private final Map<Class<?>, ModuleEntitlements> classEntitlements = new ConcurrentHashMap<>();

        ClasspathPolicyManager(List<Entitlement> pluginEntitlements) {
            super(
                new Policy("server", List.of(new Scope(ALL_UNNAMED, List.of(new ExitVMEntitlement())))),
                Map.of(PLUGIN_NAME, new Policy(PLUGIN_NAME, List.of(new Scope(ALL_UNNAMED, pluginEntitlements)))),
                Map.of(),
                type -> type.getNestHost() == EntitlementDelegationBenchmark.class
                    ? PolicyScope.plugin(PLUGIN_NAME, ALL_UNNAMED)
                    : PolicyScope.server(ALL_UNNAMED),
                name -> List.of(),
                EMPTY_PATH_LOOKUP
            );
        }

        /**
         * All classes are in the unnamed module, so entitlements are cached per class instead of per module.
         */
        @Override
        protected ModuleEntitlements getEntitlements(Class<?> requestingClass) {
            return classEntitlements.computeIfAbsent(requestingClass, this::computeEntitlements);
        }

        @Override
        protected boolean isTrustedSystemClass(Class<?> requestingClass) {
            ClassLoader loader = requestingClass.getClassLoader();
            return loader == null || loader == ClassLoader.getPlatformClassLoader();
        }

        @Override
        protected boolean isEntitlementClass(Class<?> type) {
            return type.getPackageName().startsWith("org.elasticsearch.entitlement");
        }

        @Override
        protected boolean canDelegate(Class<?> type) {
            return type.getNestHost() != EntitlementDelegationBenchmark.class;
        }
    }
}
