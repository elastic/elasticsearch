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
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog.GetIndexResult;

import java.io.IOException;

import static java.util.Collections.emptyMap;

public class EsCatalogTests extends ESTestCase {
    public void testEmpty() {
        Catalog catalog = new EsCatalog(ClusterState.builder(ClusterName.DEFAULT).build());
        assertEquals(GetIndexResult.notFound("test"), catalog.getIndex("test"));
    }

    public void testIndexExists() throws IOException {
        Catalog catalog = new EsCatalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index("test")
                                .putMapping("test", "{}")))
                .build());

        GetIndexResult result = catalog.getIndex("test");
        assertTrue(result.isValid());
        assertEquals(emptyMap(), result.get().mapping());
    }

    public void testIndexAbsent() throws IOException {
        Catalog catalog = new EsCatalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index("test")
                                .putMapping("test", "{}")))
                .build());

        GetIndexResult result = catalog.getIndex("foo");
        assertFalse(result.isValid());
    }

    public void testIndexWithDefaultType() throws IOException {
        Catalog catalog = new EsCatalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index("test")
                                .putMapping("test", "{}")
                                .putMapping("_default_", "{}")))
                .build());

        GetIndexResult result = catalog.getIndex("test");
        assertTrue(result.isValid());
        assertEquals(emptyMap(), result.get().mapping());
    }

    public void testIndexWithTwoTypes() throws IOException {
        Catalog catalog = new EsCatalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index("test")
                                .putMapping("first_type", "{}")
                                .putMapping("second_type", "{}")))
                .build());

        GetIndexResult result = catalog.getIndex("test");
        assertFalse(result.isValid());
        Exception e = expectThrows(MappingException.class, result::get);
        assertEquals(e.getMessage(), "[test] contains more than one type [first_type, second_type] so it is incompatible with sql");
    }

    public void testIndexWithNoTypes() throws IOException {
        Catalog catalog = new EsCatalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index("test")))
                .build());

        GetIndexResult result = catalog.getIndex("test");
        assertFalse(result.isValid());
        Exception e = expectThrows(MappingException.class, result::get);
        assertEquals(e.getMessage(), "[test] doesn't have any types so it is incompatible with sql");
    }

    public void testIndexStartsWithDot() throws IOException {
        Catalog catalog = new EsCatalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index(".security")
                                .putMapping("test", "{}"))
                        .build())
                .build());

        GetIndexResult result = catalog.getIndex(".security");
        assertFalse(result.isValid());
        Exception e = expectThrows(MappingException.class, result::get);
        assertEquals(e.getMessage(), "[.security] starts with [.] so it is considered internal and incompatible with sql");
    }

    private IndexMetaData.Builder index(String name) throws IOException {
        return IndexMetaData.builder(name)
                .settings(Settings.builder()
                        .put("index.version.created", Version.CURRENT)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1));
    }
}
