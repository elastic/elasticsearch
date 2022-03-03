/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.test;

import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.junit.runner.RunWith;

@RunWith(RandomizedRunner.class)
@TestMethodProviders({ JUnit4MethodProvider.class, JUnit3MethodProvider.class })
@ThreadLeakFilters(defaultFilters = true, filters = { GradleThreadsFilter.class })
public abstract class GradleUnitTestCase extends BaseTestCase {}
