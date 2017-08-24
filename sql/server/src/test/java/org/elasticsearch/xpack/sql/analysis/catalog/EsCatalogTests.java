/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.hasSize;

public class EsCatalogTests extends ESTestCase {
    public void testEmpty() {
        EsCatalog catalog = catalog(ClusterState.builder(ClusterName.DEFAULT).build());
        assertEquals(emptyList(), catalog.listIndices("*"));
        assertNull(catalog.getIndex("test"));
    }

    public void testIndexExists() throws IOException {
        EsCatalog catalog = catalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index("test")
                                .putMapping("test", "{}"))
                        .build())
                .build());

        List<EsIndex> indices = catalog.listIndices("*"); 
        assertThat(indices, hasSize(1));
        assertEquals("test", indices.get(0).name());
        assertEquals(emptyMap(), catalog.getIndex("test").mapping());
    }

    public void testIndexWithDefaultType() throws IOException {
        EsCatalog catalog = catalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index("test")
                                .putMapping("test", "{}")
                                .putMapping("_default_", "{}"))
                        .build())
                .build());

        List<EsIndex> indices = catalog.listIndices("*"); 
        assertThat(indices, hasSize(1));
        assertEquals("test", indices.get(0).name());
        assertEquals(emptyMap(), catalog.getIndex("test").mapping());
    }

    public void testIndexWithTwoTypes() throws IOException {
        EsCatalog catalog = catalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index("test")
                                .putMapping("first_type", "{}")
                                .putMapping("second_type", "{}"))
                        .build())
                .build());

        assertEquals(emptyList(), catalog.listIndices("*"));
        Exception e = expectThrows(IllegalArgumentException.class, () -> catalog.getIndex("test"));
        assertEquals(e.getMessage(), "[test] has more than one type [first_type, second_type]");
    }

    private EsCatalog catalog(ClusterState state) {
        EsCatalog catalog = new EsCatalog(() -> state);
        catalog.setIndexNameExpressionResolver(new IndexNameExpressionResolver(Settings.EMPTY));
        return catalog;
    }

    private IndexMetaData.Builder index(String name) throws IOException {
        return IndexMetaData.builder("test")
                .settings(Settings.builder()
                        .put("index.version.created", Version.CURRENT)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1));
    }
}
