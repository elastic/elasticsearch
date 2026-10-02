/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.util.concurrent.ForkJoinPool;

public class ThreadInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        builder.on(Thread.class, rule -> {
            rule.callingVoid(Thread::start).enforce(Policies::manageThreads).elseThrowNotEntitled();
            rule.callingVoid(Thread::setPriority, Integer.class).enforce(Policies::manageThreads).elseThrowNotEntitled();
            rule.callingVoid(Thread::setName, String.class).enforce(Policies::manageThreads).elseThrowNotEntitled();
            rule.callingVoidStatic(Thread::setDefaultUncaughtExceptionHandler, Thread.UncaughtExceptionHandler.class)
                .enforce(Policies::changeJvmGlobalState)
                .elseThrowNotEntitled();
            rule.callingVoid(Thread::setDaemon, Boolean.class).enforce(Policies::manageThreads).elseThrowNotEntitled();
            rule.callingVoid(Thread::setUncaughtExceptionHandler, Thread.UncaughtExceptionHandler.class)
                .enforce(Policies::manageThreads)
                .elseThrowNotEntitled();
        });

        builder.on(ThreadGroup.class, rule -> {
            rule.callingVoid(ThreadGroup::setMaxPriority, Integer.class).enforce(Policies::manageThreads).elseThrowNotEntitled();
            rule.callingVoid(ThreadGroup::setDaemon, Boolean.class).enforce(Policies::manageThreads).elseThrowNotEntitled();
        });

        builder.on(ForkJoinPool.class, rule -> {
            rule.callingVoid(ForkJoinPool::execute, Runnable.class).enforce(Policies::manageThreads).elseThrowNotEntitled();
            rule.callingVoid(ForkJoinPool::setParallelism, Integer.class).enforce(Policies::manageThreads).elseThrowNotEntitled();
        });
    }
}
