/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRules;
import org.elasticsearch.entitlement.rules.Policies;

import java.util.concurrent.ForkJoinPool;

public class ThreadInstrumentation implements InstrumentationConfig {
    @Override
    public void init() {
        EntitlementRules.on(Thread.class)
            .calling(Thread::start)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .calling(Thread::setPriority, Integer.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .calling(Thread::setName, String.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .callingStatic(Thread::setDefaultUncaughtExceptionHandler, Thread.UncaughtExceptionHandler.class)
            .enforce(Policies::changeJvmGlobalState)
            .elseThrowNotEntitled()
            .calling(Thread::setDaemon, Boolean.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .calling(Thread::setUncaughtExceptionHandler, Thread.UncaughtExceptionHandler.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled();

        EntitlementRules.on(ThreadGroup.class)
            .calling(ThreadGroup::setMaxPriority, Integer.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .calling(ThreadGroup::setDaemon, Boolean.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled();

        EntitlementRules.on(ForkJoinPool.class)
            .calling(ForkJoinPool::execute, Runnable.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled()
            .calling(ForkJoinPool::setParallelism, Integer.class)
            .enforce(Policies::manageThreads)
            .elseThrowNotEntitled();
    }
}
