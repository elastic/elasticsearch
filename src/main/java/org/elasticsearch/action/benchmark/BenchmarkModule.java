/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.benchmark;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;

/**
 * Benchmark module
 */
public class BenchmarkModule extends AbstractModule  {

    private final Settings settings;

    public static final String BENCHMARK_COORDINATOR_SERVICE_KEY = "benchmark.service.coordinator.impl";
    public static final String BENCHMARK_EXECUTOR_SERVICE_KEY    = "benchmark.service.executor.impl";

    public BenchmarkModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {

        final Class<? extends BenchmarkCoordinatorService> coordinator = settings.getAsClass(BENCHMARK_COORDINATOR_SERVICE_KEY, BenchmarkCoordinatorService.class);
        if (!BenchmarkCoordinatorService.class.equals(coordinator)) {
            bind(BenchmarkCoordinatorService.class).to(coordinator).asEagerSingleton();
        } else {
            bind(BenchmarkCoordinatorService.class).asEagerSingleton();
        }

        final Class<? extends BenchmarkExecutorService> executor = settings.getAsClass(BENCHMARK_EXECUTOR_SERVICE_KEY, BenchmarkExecutorService.class);
        if (!BenchmarkExecutorService.class.equals(executor)) {
            bind(BenchmarkExecutorService.class).to(executor).asEagerSingleton();
        } else {
            bind(BenchmarkExecutorService.class).asEagerSingleton();
        }
    }
}
