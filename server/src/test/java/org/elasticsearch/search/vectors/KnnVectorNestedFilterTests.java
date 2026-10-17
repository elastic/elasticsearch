/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for KNN queries with nested filters, particularly the bug where
 * a KNN query on a nested field with a filter on a different nested field
 * returns 0 results.
 *
 * See https://github.com/elastic/elasticsearch/issues/162043
 */
public class KnnVectorNestedFilterTests extends MapperServiceTestCase {

    private static String getMapping(int dim) {
        return String.format("""
            {
              "_doc": {
                "properties": {
                  "id": {
                    "type": "keyword",
                    "store": true
                  },
                  "chunks": {
                    "type": "nested",
                    "properties": {
                      "vector": {
                        "type": "dense_vector",
                        "dims": %d,
                        "similarity": "l2_norm",
                        "index_options": {
                          "type": "flat"
                        }
                      }
                    }
                  },
                  "parent": {
                    "type": "nested",
                    "properties": {
                      "nodes": {
                        "type": "nested",
                        "properties": {
                          "id": {
                            "type": "long"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """, dim);
    }

    public void testNestedFilterOnNestedKnn() throws IOException {
        int dims = 3;
        var mapperService = createMapperService(getMapping(dims));
        var fieldType = (DenseVectorFieldMapper.DenseVectorFieldType) mapperService.fieldType("chunks.vector");
        var chunksParent = mapperService.mappingLookup().nestedLookup().getNestedMappers().get("chunks");

        float[] queryVector = new float[] { 0.5f, 0.5f, 0.5f };

        withLuceneIndex(mapperService, iw -> {
            // Document 1: has a chunk vector and parent.nodes.id = 1
            SourceToParse source = sourceWithVectorAndNestedFilter("1", queryVector, 1);
            ParsedDocument doc = mapperService.documentMapper().parse(source);
            iw.addDocuments(doc.docs());

            // Document 2: has a chunk vector but parent.nodes.id = 2 (should not match)
            source = sourceWithVectorAndNestedFilter("2", queryVector, 2);
            doc = mapperService.documentMapper().parse(source);
            iw.addDocuments(doc.docs());

            // Document 3: has parent.nodes.id = 1 but no chunk vector (should not match KNN)
            source = sourceWithNestedFilterOnly("3", 1);
            doc = mapperService.documentMapper().parse(source);
            iw.addDocuments(doc.docs());
        }, ir -> {
            var searcher = new IndexSearcher(wrapInMockESDirectoryReader(ir));
            var context = createSearchExecutionContext(mapperService);
            var bitSetProducer = context.bitsetFilter(chunksParent.parentTypeFilter());

            // Build KNN query with a nested filter on parent.nodes.id = 1
            var knnQueryBuilder = new KnnVectorQueryBuilder("chunks.vector", queryVector, 10, 10, null, null, null);
            knnQueryBuilder.addFilterQuery(
                QueryBuilders.nestedQuery("parent.nodes", QueryBuilders.termQuery("parent.nodes.id", 1), ScoreMode.None)
            );

            var knnQuery = knnQueryBuilder.doToQuery(context);

            // Wrap in nested query for chunks
            var nestedQuery = QueryBuilders.nestedQuery("chunks", knnQueryBuilder, ScoreMode.Max).toQuery(context);

            var topDocs = searcher.search(nestedQuery, 10);
            // Should return 1 hit (document 1 which has both matching vector and matching nested filter)
            // Before the fix, this returns 0 hits because the nested filter is built with the wrong scope
            assertThat(topDocs.totalHits.value(), equalTo(1L));
        });
    }

    public void testTopLevelFilterOnNestedKnn() throws IOException {
        int dims = 3;
        var mapperService = createMapperService(getMapping(dims));
        var fieldType = (DenseVectorFieldMapper.DenseVectorFieldType) mapperService.fieldType("chunks.vector");
        var chunksParent = mapperService.mappingLookup().nestedLookup().getNestedMappers().get("chunks");

        float[] queryVector = new float[] { 0.5f, 0.5f, 0.5f };

        withLuceneIndex(mapperService, iw -> {
            // Document 1: has a chunk vector
            SourceToParse source = sourceWithVector("1", queryVector);
            ParsedDocument doc = mapperService.documentMapper().parse(source);
            iw.addDocuments(doc.docs());

            // Document 2: has a chunk vector
            source = sourceWithVector("2", queryVector);
            doc = mapperService.documentMapper().parse(source);
            iw.addDocuments(doc.docs());
        }, ir -> {
            var searcher = new IndexSearcher(wrapInMockESDirectoryReader(ir));
            var context = createSearchExecutionContext(mapperService);
            var bitSetProducer = context.bitsetFilter(chunksParent.parentTypeFilter());

            // Build KNN query with a top-level filter on id
            var knnQueryBuilder = new KnnVectorQueryBuilder("chunks.vector", queryVector, 10, 10, null, null, null);
            knnQueryBuilder.addFilterQuery(QueryBuilders.termQuery("id", "1"));

            var knnQuery = knnQueryBuilder.doToQuery(context);

            // Wrap in nested query for chunks
            var nestedQuery = QueryBuilders.nestedQuery("chunks", knnQueryBuilder, ScoreMode.Max).toQuery(context);

            var topDocs = searcher.search(nestedQuery, 10);
            // Should return 1 hit (document 1)
            assertThat(topDocs.totalHits.value(), equalTo(1L));
        });
    }

    private SourceToParse sourceWithVector(String id, float[] vector) throws IOException {
        try (var builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            builder.field("id", id);
            builder.startArray("chunks");
            builder.startObject();
            builder.field("vector", vector);
            builder.endObject();
            builder.endArray();
            builder.endObject();
            return new SourceToParse(id, BytesReference.bytes(builder), XContentType.JSON);
        }
    }

    private SourceToParse sourceWithVectorAndNestedFilter(String id, float[] vector, int nestedId) throws IOException {
        try (var builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            builder.field("id", id);
            builder.startArray("chunks");
            builder.startObject();
            builder.field("vector", vector);
            builder.endObject();
            builder.endArray();
            builder.startArray("parent");
            builder.startObject();
            builder.startArray("nodes");
            builder.startObject();
            builder.field("id", nestedId);
            builder.endObject();
            builder.endArray();
            builder.endObject();
            builder.endArray();
            builder.endObject();
            return new SourceToParse(id, BytesReference.bytes(builder), XContentType.JSON);
        }
    }

    private SourceToParse sourceWithNestedFilterOnly(String id, int nestedId) throws IOException {
        try (var builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            builder.field("id", id);
            builder.startArray("parent");
            builder.startObject();
            builder.startArray("nodes");
            builder.startObject();
            builder.field("id", nestedId);
            builder.endObject();
            builder.endArray();
            builder.endObject();
            builder.endArray();
            builder.endObject();
            return new SourceToParse(id, BytesReference.bytes(builder), XContentType.JSON);
        }
    }
}
