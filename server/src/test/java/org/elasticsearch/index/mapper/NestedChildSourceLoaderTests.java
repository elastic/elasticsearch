/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * {@link MappingLookup#newNestedChildSourceLoader}, which ES|QL reads the fields under a nested object without doc values with.
 */
public class NestedChildSourceLoaderTests extends MapperServiceTestCase {

    private static final String NESTED_MAPPING = """
        {
            "_doc": {
                "properties": {
                    "name": { "type": "keyword" },
                    "children": {
                        "type": "nested",
                        "properties": {
                            "value": { "type": "keyword" }
                        }
                    }
                }
            }
        }
        """;

    private static final String VECTOR_MAPPING = """
        {
            "_doc": {
                "properties": {
                    "name": { "type": "keyword" },
                    "embedding": { "type": "dense_vector", "dims": 3 },
                    "children": {
                        "type": "nested",
                        "properties": {
                            "value": { "type": "keyword" }
                        }
                    }
                }
            }
        }
        """;

    /**
     * When vectors are left out of the stored {@code _source} and patched back in on read, a child still yields its own slice
     * of the root's source, and the root still gets its vectors.
     */
    public void testVectorsPatchedIntoSource() throws IOException {
        MapperService mapperService = createMapperService(
            Settings.builder().put(IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.getKey(), true).build(),
            VECTOR_MAPPING
        );

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("name", "root");
            b.array("embedding", 1.0f, 2.0f, 3.0f);
            b.startArray("children");
            b.startObject().field("value", "child1").endObject();
            b.endArray();
        }));

        // doc 0 = nested child, doc 1 = root
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            SourceLoader loader = childLoader(mapperService);
            SourceLoader.Leaf leaf = loader.leaf(reader.leaves().get(0), null);
            LeafStoredFieldLoader leafLoader = storedFieldLoader(loader).getLoader(reader.leaves().get(0), null);

            leafLoader.advanceTo(0);
            Source childSource = leaf.source(leafLoader, 0);
            @SuppressWarnings("unchecked")
            Map<String, Object> childrenMap = (Map<String, Object>) childSource.source().get("children");
            assertNotNull(childSource.source().toString(), childrenMap);
            assertEquals("child1", childrenMap.get("value"));
            assertNull(childSource.source().get("name"));

            leafLoader.advanceTo(1);
            Source rootSource = leaf.source(leafLoader, 1);
            assertEquals("root", rootSource.source().get("name"));
            assertEquals(3, ((List<?>) rootSource.source().get("embedding")).size());
        });
    }

    /**
     * With synthetic {@code _source}, each child gets its own object of the source synthesized for its root, rather than a
     * root synthesized at the child's doc ID.
     */
    public void testSyntheticSource() throws IOException {
        MapperService mapperService = createMapperService(
            Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic").build(),
            """
                { "_doc": { "properties": {
                  "name": { "type": "keyword" },
                  "children": { "type": "nested", "properties": { "value": { "type": "keyword", "doc_values": false } } } } } }
                """
        );

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("name", "root");
            b.startArray("children");
            b.startObject().field("value", "child1").endObject();
            b.startObject().field("value", "child2").endObject();
            b.endArray();
        }));

        // docs 0 and 1 = nested children, doc 2 = root
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            // The synthetic loader of a nested object looks its children up through the bitset cache, which needs the shard
            DirectoryReader wrapped = ElasticsearchDirectoryReader.wrap(reader, new ShardId(mapperService.index(), 0));
            SourceLoader loader = childLoader(mapperService);
            SourceLoader.Leaf leaf = loader.leaf(wrapped.leaves().get(0), null);
            LeafStoredFieldLoader leafLoader = storedFieldLoader(loader).getLoader(wrapped.leaves().get(0), null);
            for (int child = 0; child < 2; child++) {
                leafLoader.advanceTo(child);
                Source childSource = leaf.source(leafLoader, child);
                assertEquals(Map.of("children", Map.of("value", "child" + (child + 1))), childSource.source());
            }
            leafLoader.advanceTo(2);
            assertEquals("root", leaf.source(leafLoader, 2).source().get("name"));
        });
    }

    /**
     * A child's slice is small, but the root's source it was cut from is kept for the root's other children, so that is what a
     * reader reserving memory for the source it loads counts.
     */
    public void testChildReportsTheRootSourceItHolds() throws IOException {
        MapperService mapperService = createMapperService(NESTED_MAPPING);
        String large = "x".repeat(10_000);
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("name", large);
            b.startArray("children");
            b.startObject().field("value", "child1").endObject();
            b.endArray();
        }));

        // doc 0 = nested child, doc 1 = root
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
            SourceLoader loader = childLoader(mapperService);
            var storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                storedFieldLoader(loader).getLoader(reader.leaves().get(0), null),
                loader.leaf(reader.leaves().get(0), null)
            );
            storedFields.advanceTo(0);
            assertThat(storedFields.source().internalSourceRef().length(), lessThan(100));
            assertThat(storedFields.lastSourceBytesSize(), greaterThan((long) large.length()));
        });
    }

    private SourceLoader childLoader(MapperService mapperService) {
        NestedDocuments nestedDocuments = new NestedDocuments(
            mapperService.mappingLookup(),
            QueryBitSetProducer::new,
            IndexVersion.current()
        );
        return mapperService.mappingLookup().newNestedChildSourceLoader(nestedDocuments);
    }

    private StoredFieldLoader storedFieldLoader(SourceLoader loader) {
        return StoredFieldLoader.create(true, loader.requiredStoredFields());
    }
}
