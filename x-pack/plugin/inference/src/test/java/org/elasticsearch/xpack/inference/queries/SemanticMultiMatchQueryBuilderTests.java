/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class SemanticMultiMatchQueryBuilderTests extends MapperServiceTestCase {
    private static TestThreadPool threadPool;
    private static ModelRegistry modelRegistry;

    private static class InferencePluginWithModelRegistry extends InferencePlugin {
        InferencePluginWithModelRegistry(Settings settings) {
            super(settings);
        }

        @Override
        protected Supplier<ModelRegistry> getModelRegistry() {
            return () -> modelRegistry;
        }
    }

    @BeforeClass
    public static void startModelRegistry() {
        threadPool = new TestThreadPool(SemanticMultiMatchQueryBuilderTests.class.getName());
        var clusterService = ClusterServiceUtils.createClusterService(threadPool);
        modelRegistry = new ModelRegistry(clusterService, new NoOpClient(threadPool));
        modelRegistry.clusterChanged(new ClusterChangedEvent("init", clusterService.state(), clusterService.state()) {
            @Override
            public boolean localNodeMaster() {
                return false;
            }
        });
    }

    @AfterClass
    public static void stopModelRegistry() {
        IOUtils.closeWhileHandlingException(threadPool);
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePluginWithModelRegistry(Settings.EMPTY));
    }

    public void testResolveSemanticTextFieldFromWildcard() throws Exception {
        MapperService mapperService = createMapperService("""
            {
              "_doc" : {
                "properties": {
                  "text_field": { "type": "text" },
                  "keyword_field": { "type": "keyword" },
                  "inference_field": { "type": "semantic_text", "inference_id": "test_service" }
                }
              }
            }
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "text_field" : "foo",
              "keyword_field" : "foo",
              "inference_field" : "foo",
              "_inference_fields": {
                "inference_field": {
                  "inference": {
                    "inference_id": "test_service",
                    "model_settings": {
                      "task_type": "sparse_embedding"
                    },
                    "chunks": {
                      "inference_field": [
                        {
                          "start_offset": 0,
                          "end_offset": 3,
                          "embeddings": {
                            "foo": 1.0
                          }
                        }
                      ]
                    }
                  }
                }
              }
            }
            """));

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {
            SearchExecutionContext context = createSearchExecutionContext(mapperService, newSearcher(ir));
            Query query = new MultiMatchQueryBuilder("foo", "*_field").toQuery(context);
            Query expected = new DisjunctionMaxQuery(
                List.of(new TermQuery(new Term("text_field", "foo")), new TermQuery(new Term("keyword_field", "foo"))),
                0f
            );
            assertEquals(expected, query);
        });
    }
}
