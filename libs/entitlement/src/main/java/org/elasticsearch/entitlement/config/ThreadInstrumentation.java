/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRule;
import org.elasticsearch.entitlement.rules.EntitlementRules;
import org.elasticsearch.entitlement.rules.Policies;

import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

public class ThreadInstrumentation implements InstrumentationConfig {
    @Override
    public void init(Consumer<EntitlementRule> addRule) {
        EntitlementRules.on(addRule, Thread.class)
            .callingVoid(Thread::start)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .callingVoid(Thread::setPriority, Integer.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .callingVoid(Thread::setName, String.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .callingVoidStatic(Thread::setDefaultUncaughtExceptionHandler, Thread.UncaughtExceptionHandler.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled()
            .callingVoid(Thread::setDaemon, Boolean.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .callingVoid(Thread::setUncaughtExceptionHandler, Thread.UncaughtExceptionHandler.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, ThreadGroup.class)
            .callingVoid(ThreadGroup::setMaxPriority, Integer.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .callingVoid(ThreadGroup::setDaemon, Boolean.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, ForkJoinPool.class)
            .callingVoid(ForkJoinPool::execute, Runnable.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .callingVoid(ForkJoinPool::setParallelism, Integer.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled();
    }
}
