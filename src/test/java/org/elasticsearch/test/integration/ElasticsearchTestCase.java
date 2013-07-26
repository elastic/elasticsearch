/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.test.integration;

import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import org.apache.lucene.util.LuceneJUnit3MethodProvider;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.junit.listerners.ReproduceInfoPrinter;
import org.junit.runner.RunWith;

@TestMethodProviders({
        LuceneJUnit3MethodProvider.class,
        JUnit4MethodProvider.class
})
@Listeners({
        ReproduceInfoPrinter.class
})
@ThreadLeakFilters(defaultFilters = true, filters = {ElasticsearchTestCase.ElasticSearchThreadFilter.class})
@ThreadLeakScope(Scope.NONE)
@RunWith(value = com.carrotsearch.randomizedtesting.RandomizedRunner.class)
public abstract class ElasticsearchTestCase extends RandomizedTest {

    public static final Version TEST_VERSION_CURRENT = LuceneTestCase.TEST_VERSION_CURRENT;

    protected final ESLogger logger = Loggers.getLogger(getClass());

    public static final boolean NIGHLY = Boolean.parseBoolean(System.getProperty("es.tests.nighly", "false")); // disabled by default

    public static final String CHILD_VM_ID = System.getProperty("junit4.childvm.id", "" + System.currentTimeMillis());

    public static final String SYSPROP_BADAPPLES = "tests.badapples";

    public static class ElasticSearchThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {

            return true;
        }
    }

}
