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
import org.elasticsearch.xpack.esql.TestQueryExecutor;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_THREAD_POOL_NAME;

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
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        var threadPool = new TestThreadPool(
            "OptimizerExecutabilityTests",
            new FixedExecutorBuilder(
                Settings.EMPTY,
                ESQL_THREAD_POOL_NAME,
                numThreads,
                1024,
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
        HeaderWarning.setThreadContext(threadPool.getThreadContext());
        var executor = new TestQueryExecutor(threadPool);

        var parsed = parser.createStatement(query);
        MapperService mapperService = createMapperService(MAPPING_ALL_TYPES);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "integer" : 1 }
            """));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {
            IndexSearcher searcher = newSearcher(ir);
            SearchExecutionContext ctx = createSearchExecutionContext(mapperService, searcher);

            var actualResults = executor.executePlan(
                parsed,
                new EsPhysicalOperationProviders(List.of(new EsPhysicalOperationProviders.DefaultShardContext(0, ctx, AliasFilter.EMPTY))),
                IndexResolution.valid(new EsIndex("testidx", Map.of("integer", new EsField("integer", DataTypes.INTEGER, Map.of(), true)))),
                new EnrichResolution(Set.of(), Set.of()),
                true
            );

            assertWarnings("No limit defined, adding default limit of [500]");

            HeaderWarning.removeThreadContext(threadPool.getThreadContext());
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        });

    }
}
