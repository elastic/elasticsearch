/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.TestQueryRunner;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.index.EsIndex;
import org.elasticsearch.xpack.esql.core.index.IndexResolution;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Tests that run against actual indices, to confirm that optimized plans remain executable.
 */
public class OptimizerExecutabilityTests extends MapperServiceTestCase {
    private final EsqlParser parser = new EsqlParser();
    // TODO: add constant_keyword, scaled_float, unsigned_long, version, wildcard
    // Currently leads to error: Failed to parse mapping: No handler for type [version] declared on field [version]
    private static final String MAPPING_ALL_TYPES = """
        {
          "mappings": {
            "properties" : {
              "alias-integer": {
                "type": "alias",
                "path": "integer"
              },
              "boolean": {
                "type": "boolean"
              },
              "byte" : {
                "type" : "byte"
              },
              "date": {
                "type": "date"
              },
              "double": {
                "type": "double"
              },
              "float": {
                "type": "float"
              },
              "half_float": {
                "type": "half_float"
              },
              "integer" : {
                "type" : "integer"
              },
              "ip": {
                "type": "ip"
              },
              "keyword" : {
                "type" : "keyword"
              },
              "long": {
                "type": "long"
              },
              "short": {
                "type": "short"
              },
              "text" : {
                "type" : "text"
              }
            }
          }
        }""";

    public void testOutOfRangePushdown() throws IOException {
        // This comparison is out of range and should fail. (It fails on when using a real Lucene index.)
        runQuery("from testidx | where integer < 1E300");
    }

    private void runQuery(String query) throws IOException {
        ThreadPool threadPool;
        Executor executor;

        if (randomBoolean()) {
            int numThreads = randomBoolean() ? 1 : between(2, 16);
            threadPool = new TestThreadPool(
                "CsvTests",
                new FixedExecutorBuilder(Settings.EMPTY, "esql_test", numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
            );
            executor = threadPool.executor("esql_test");
        } else {
            threadPool = new TestThreadPool(getTestName());
            executor = threadPool.executor(ThreadPool.Names.SEARCH);
        }

        HeaderWarning.setThreadContext(threadPool.getThreadContext());
        var queryRunner = new TestQueryRunner(threadPool, executor);

        var parsed = parser.createStatement(query);
        MapperService mapperService = createMapperService(MAPPING_ALL_TYPES);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "integer" : 1 }
            """));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {
            IndexSearcher searcher = newSearcher(ir);
            SearchExecutionContext ctx = createSearchExecutionContext(mapperService, searcher);

            var actualResults = queryRunner.executePlan(
                parsed,
                new EsPhysicalOperationProviders(List.of(new EsPhysicalOperationProviders.DefaultShardContext(0, ctx, AliasFilter.EMPTY))),
                IndexResolution.valid(new EsIndex("testidx", Map.of("integer", new EsField("integer", DataType.INTEGER, Map.of(), true)))),
                new EnrichResolution()
            );

            assertWarnings("No limit defined, adding default limit of [1000]");

            HeaderWarning.removeThreadContext(threadPool.getThreadContext());
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        });

    }
}
