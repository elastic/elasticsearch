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
package org.elasticsearch;

import com.carrotsearch.randomizedtesting.SeedUtils;
import com.carrotsearch.randomizedtesting.annotations.*;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.google.common.base.Predicate;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.store.mock.MockDirectoryHelper;
import org.elasticsearch.junit.listeners.LoggingListener;
import org.junit.BeforeClass;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@ThreadLeakFilters(defaultFilters = true, filters = {ElasticSearchThreadFilter.class})
@ThreadLeakScope(Scope.NONE)
@TimeoutSuite(millis = TimeUnits.HOUR) // timeout the suite after 1h and fail the test.
@Listeners(LoggingListener.class)
public abstract class ElasticsearchTestCase extends AbstractRandomizedTest {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    public static final String CHILD_VM_ID = System.getProperty("junit4.childvm.id", "" + System.currentTimeMillis());
    
    public static final long SHARED_CLUSTER_SEED = clusterSeed();
    
    private static long clusterSeed() {
        String property = System.getProperty("tests.cluster_seed");
        if (property == null || property.isEmpty()) {
            return System.nanoTime();
        }
        return SeedUtils.parseSeed(property);
        
    }
    
    public void awaitBusy(Predicate<?> breakPredicate) throws InterruptedException {
        awaitBusy(breakPredicate, 10, TimeUnit.SECONDS);
    }
    
    public boolean awaitBusy(Predicate<?> breakPredicate, long maxWaitTime, TimeUnit unit) throws InterruptedException {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        long iterations = Math.max(Math.round(Math.log10(maxTimeInMillis) / Math.log10(2)), 1);
        long timeInMillis = 1;
        long sum = 0;
        for (int i = 0; i < iterations; i++) {
            if (breakPredicate.apply(null)) {
                return true;
            }
            sum += timeInMillis;
            Thread.sleep(timeInMillis);
            timeInMillis *= 2;
        }
        timeInMillis = maxTimeInMillis - sum;
        Thread.sleep(Math.max(timeInMillis, 0));
        return breakPredicate.apply(null);
    }
    
    private static final String[] numericTypes = new String[] {"byte", "short", "integer", "long"};
    
    public static String randomNumericType(Random random) {
        return numericTypes[random.nextInt(numericTypes.length)];
    }
    
    /**
     * Returns a {@link File} pointing to the class path relative resource given
     * as the first argument. In contrast to
     * <code>getClass().getResource(...).getFile()</code> this method will not
     * return URL encoded paths if the parent path contains spaces or other
     * non-standard characters.
     */
    public File getResource(String relativePath) {
        URI uri = URI.create(getClass().getResource(relativePath).toString());
        return new File(uri);
    }
    
    public static void ensureAllFilesClosed() throws IOException {
        try {
            for (MockDirectoryWrapper w : MockDirectoryHelper.wrappers) {
                if (w.isOpen()) {
                    w.close();
                }
            }
        } finally {
            forceClearMockWrappers();
        }
    }
    
    public static void forceClearMockWrappers() {
        MockDirectoryHelper.wrappers.clear();
    }
    
    public static boolean hasUnclosedWrapper() {
        for (MockDirectoryWrapper w : MockDirectoryHelper.wrappers) {
            if (w.isOpen()) {
                return true;
            }
        }
        return false;
    }

    
    @BeforeClass
    public static void registerMockDirectoryHooks() throws Exception {
        closeAfterSuite(new Closeable() {
            @Override
            public void close() throws IOException {
                ensureAllFilesClosed();
            }
        });
    }
  }
