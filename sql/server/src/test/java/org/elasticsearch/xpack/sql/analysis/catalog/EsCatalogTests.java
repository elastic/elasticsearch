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
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;

import static java.util.Collections.emptyMap;

public class EsCatalogTests extends ESTestCase {
    public void testEmpty() {
        Catalog catalog = new EsCatalog(ClusterState.builder(ClusterName.DEFAULT).build());
        assertNull(catalog.getIndex("test"));
    }

    public void testIndexExists() throws IOException {
        Catalog catalog = new EsCatalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index()
                                .putMapping("test", "{}"))
                        .build())
                .build());

        assertEquals(emptyMap(), catalog.getIndex("test").mapping());
    }

    public void testIndexWithDefaultType() throws IOException {
        Catalog catalog = new EsCatalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index()
                                .putMapping("test", "{}")
                                .putMapping("_default_", "{}"))
                        .build())
                .build());

        assertEquals(emptyMap(), catalog.getIndex("test").mapping());
    }

    public void testIndexWithTwoTypes() throws IOException {
        Catalog catalog = new EsCatalog(ClusterState.builder(ClusterName.DEFAULT)
                .metaData(MetaData.builder()
                        .put(index()
                                .putMapping("first_type", "{}")
                                .putMapping("second_type", "{}"))
                        .build())
                .build());

        Exception e = expectThrows(SqlIllegalArgumentException.class, () -> catalog.getIndex("test"));
        assertEquals(e.getMessage(), "[test] has more than one type [first_type, second_type]");
    }

    private IndexMetaData.Builder index() throws IOException {
        return IndexMetaData.builder("test")
                .settings(Settings.builder()
                        .put("index.version.created", Version.CURRENT)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 1));
    }
}
