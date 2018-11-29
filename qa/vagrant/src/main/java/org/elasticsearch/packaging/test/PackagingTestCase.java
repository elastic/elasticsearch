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

package org.elasticsearch.packaging.test;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Installation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import static org.elasticsearch.packaging.util.Cleanup.cleanEverything;
import static org.junit.Assume.assumeTrue;

@RunWith(RandomizedRunner.class)
@TestMethodProviders({
    JUnit3MethodProvider.class
})
@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
/**
 * Class that all packaging test cases should inherit from. This makes working with the packaging tests more similar to what we're
 * familiar with from {@link org.elasticsearch.test.ESTestCase} without having to apply its behavior that's not relevant here
 */
public abstract class PackagingTestCase extends Assert {

    protected final Log logger = LogFactory.getLog(getClass());

    @Rule
    public final TestName testNameRule = new TestName();

    @Before
    public void setup() {
        assumeTrue("only compatible distributions", distribution().packaging.compatible);
        logger.info("[" + testNameRule.getMethodName() + "]: before test");
    }

    protected static Installation installation;

    @BeforeClass
    public static void cleanup() {
        installation = null;
        cleanEverything();
    }

    /** The {@link Distribution} that should be tested in this case */
    protected abstract Distribution distribution();


}
