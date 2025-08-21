/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DiversifyingParentBlockQueryTests extends MapperServiceTestCase {
    private static String getMapping(int dim) {
        return String.format(Locale.ROOT, """
                {
                  "_doc": {
                    "properties": {
                      "id": {
                        "type": "keyword",
                        "store": true
                      },
                      "nested": {
                        "type": "nested",
                          "properties": {
                            "emb": {
                              "type": "dense_vector",
                              "dims": %d,
                              "similarity": "l2_norm",
                              "index_options": {
                                "type": "flat"
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

    public void testRandom() throws IOException {
        int dims = randomIntBetween(3, 10);
        var mapperService = createMapperService(getMapping(dims));
        var fieldType = (DenseVectorFieldMapper.DenseVectorFieldType) mapperService.fieldType("nested.emb");
        var nestedParent = mapperService.mappingLookup().nestedLookup().getNestedMappers().get("nested");

        int numQueries = randomIntBetween(1, 3);
        float[][] queries = new float[numQueries][];
        List<TreeMap<Float, String>> expectedTopDocs = new ArrayList<>();
        for (int i = 0; i < numQueries; i++) {
            queries[i] = randomVector(dims);
            expectedTopDocs.add(new TreeMap<>((o1, o2) -> -Float.compare(o1, o2)));
        }

        withLuceneIndex(mapperService, iw -> {
            int numDocs = randomIntBetween(10, 50);
            for (int i = 0; i < numDocs; i++) {
                int numVectors = randomIntBetween(0, 5);
                float[][] vectors = new float[numVectors][];
                for (int j = 0; j < numVectors; j++) {
                    vectors[j] = randomVector(dims);
                }

                for (int k = 0; k < numQueries; k++) {
                    float maxScore = Float.MIN_VALUE;
                    for (int j = 0; j < numVectors; j++) {
                        float score = EUCLIDEAN.compare(vectors[j], queries[k]);
                        maxScore = Math.max(score, maxScore);
                    }
                    expectedTopDocs.get(k).put(maxScore, Integer.toString(i));
                }

                SourceToParse source = randomSource(Integer.toString(i), vectors);
                ParsedDocument doc = mapperService.documentMapper().parse(source);
                iw.addDocuments(doc.docs());

                if (randomBoolean()) {
                    int numEmpty = randomIntBetween(1, 3);
                    for (int l = 0; l < numEmpty; l++) {
                        source = randomSource(randomAlphaOfLengthBetween(5, 10), new float[0][]);
                        doc = mapperService.documentMapper().parse(source);
                        iw.addDocuments(doc.docs());
                    }
                }
            }
        }, ir -> {
            var storedFields = ir.storedFields();
            var searcher = new IndexSearcher(wrapInMockESDirectoryReader(ir));
            var context = createSearchExecutionContext(mapperService);
            var bitSetproducer = context.bitsetFilter(nestedParent.parentTypeFilter());
            for (int i = 0; i < numQueries; i++) {
                var knnQuery = fieldType.createKnnQuery(
                    VectorData.fromFloats(queries[i]),
                    10,
                    10,
                    null,
                    null,
                    null,
                    bitSetproducer,
                    DenseVectorFieldMapper.FilterHeuristic.ACORN,
                    randomBoolean()
                );
                assertThat(knnQuery, instanceOf(DiversifyingParentBlockQuery.class));
                var nestedQuery = new ToParentBlockJoinQuery(knnQuery, bitSetproducer, ScoreMode.Total);
                var topDocs = searcher.search(nestedQuery, 10);
                for (var doc : topDocs.scoreDocs) {
                    var entry = expectedTopDocs.get(i).pollFirstEntry();
                    assertNotNull(entry);
                    assertThat(doc.score, equalTo(entry.getKey()));
                    var storedDoc = storedFields.document(doc.doc, Set.of("id"));
                    assertThat(storedDoc.getField("id").binaryValue().utf8ToString(), equalTo(entry.getValue()));
                }
            }
        });
    }

    private SourceToParse randomSource(String id, float[][] vectors) throws IOException {
        try (var builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            builder.field("id", id);
            builder.startArray("nested");
            for (int i = 0; i < vectors.length; i++) {
                builder.startObject();
                builder.field("emb", vectors[i]);
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return new SourceToParse(id, BytesReference.bytes(builder), XContentType.JSON);
        }
    }

    private float[] randomVector(int dim) {
        float[] vector = new float[dim];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }
}
