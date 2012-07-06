/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.percolator;

import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.percolator.PercolatorExecutor;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.threadpool.ThreadPoolModule;

import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 *
 */
public class EmbeddedPercolatorBenchmarkTest {

    private static long NUMBER_OF_ITERATIONS = 100000;
    private static int NUMBER_OF_THREADS = 10;
    private static int NUMBER_OF_QUERIES = 100;

    public static void main(String[] args) throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.cache.filter.type", "none")
                .build();
        Index index = new Index("test");
        Injector injector = new ModulesBuilder().add(
                new SettingsModule(settings),
                new ThreadPoolModule(settings),
                new IndicesQueriesModule(),
                new ScriptModule(settings),
                new MapperServiceModule(),
                new IndexSettingsModule(index, settings),
                new IndexCacheModule(settings),
                new AnalysisModule(settings),
                new IndexEngineModule(settings),
                new SimilarityModule(settings),
                new IndexQueryParserModule(settings),
                new IndexNameModule(index),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(PercolatorExecutor.class).asEagerSingleton();
                    }
                }
        ).createInjector();

        final PercolatorExecutor percolatorExecutor = injector.getInstance(PercolatorExecutor.class);

        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field1", 1)
                .field("field2", "value")
                .field("field3", "the quick brown fox jumped over the lazy dog")
                .endObject().endObject();
        final byte[] source = doc.bytes().toBytes();

        PercolatorExecutor.Response percolate = percolatorExecutor.percolate(new PercolatorExecutor.SourceRequest("type1", new BytesArray(source)));

        for (int i = 0; i < NUMBER_OF_QUERIES; i++) {
            percolatorExecutor.addQuery("test" + i, termQuery("field3", "quick"));
        }


        System.out.println("Warming Up (1000)");
        StopWatch stopWatch = new StopWatch().start();
        System.out.println("Running " + 1000);
        for (long i = 0; i < 1000; i++) {
            percolate = percolatorExecutor.percolate(new PercolatorExecutor.SourceRequest("type1", new BytesArray(source)));
        }
        System.out.println("[Warmup] Percolated in " + stopWatch.stop().totalTime() + " TP Millis " + (NUMBER_OF_ITERATIONS / stopWatch.totalTime().millisFrac()));

        System.out.println("Percolating using " + NUMBER_OF_THREADS + " threads with " + NUMBER_OF_ITERATIONS + " iterations, and " + NUMBER_OF_QUERIES + " queries");
        final CountDownLatch latch = new CountDownLatch(NUMBER_OF_THREADS);
        Thread[] threads = new Thread[NUMBER_OF_THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (long i = 0; i < NUMBER_OF_ITERATIONS; i++) {
                        PercolatorExecutor.Response percolate = percolatorExecutor.percolate(new PercolatorExecutor.SourceRequest("type1", new BytesArray(source)));
                    }
                    latch.countDown();
                }
            });
        }
        stopWatch = new StopWatch().start();
        for (Thread thread : threads) {
            thread.start();
        }
        latch.await();
        stopWatch.stop();
        System.out.println("Percolated in " + stopWatch.totalTime() + " TP Millis " + ((NUMBER_OF_ITERATIONS * NUMBER_OF_THREADS) / stopWatch.totalTime().millisFrac()));

    }
}
