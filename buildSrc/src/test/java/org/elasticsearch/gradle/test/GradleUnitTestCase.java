package org.elasticsearch.gradle.test;

import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.junit.runner.RunWith;

@RunWith(RandomizedRunner.class)
@TestMethodProviders({ JUnit4MethodProvider.class, JUnit3MethodProvider.class })
@ThreadLeakFilters(defaultFilters = true, filters = { GradleThreadsFilter.class })
public abstract class GradleUnitTestCase extends BaseTestCase {}
