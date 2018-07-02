package org.elasticsearch.gradle.test;

import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import org.junit.runner.RunWith;

@RunWith(RandomizedRunner.class)
@TestMethodProviders({
    JUnit4MethodProvider.class,
    JUnit3MethodProvider.class
})
public abstract class GradleUnitTestCase extends BaseTestCase  {
}
