/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link ApplicationPermission#grants} for literal resource strings.
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class ApplicationPermissionGrantsBenchmark {

    @Param({ "10", "100", "1000" })
    int resourceCount;

    private ApplicationPermission permission;
    private ApplicationPrivilege checkPrivilege;
    private String[] resources;

    @Setup(Level.Trial)
    public void setup() {
        List<ApplicationPrivilegeDescriptor> stored = List.of(
            new ApplicationPrivilegeDescriptor("myapp", "admin", Set.of("action:admin/*"), Map.of())
        );

        ApplicationPrivilege grantedPrivilege = ApplicationPrivilege.get("myapp", Set.of("admin"), stored).iterator().next();
        permission = new ApplicationPermission(List.of(new Tuple<>(grantedPrivilege, Set.of("resource:/org:12345/*"))));

        checkPrivilege = ApplicationPrivilege.get("myapp", Set.of("admin"), stored).iterator().next();

        resources = new String[resourceCount];
        for (int i = 0; i < resourceCount; i++) {
            resources[i] = "resource:/org:12345/" + i;
        }
    }

    @Benchmark
    public void grantsLiteralResources(Blackhole bh) {
        for (String resource : resources) {
            bh.consume(permission.grants(checkPrivilege, resource));
        }
    }
}
