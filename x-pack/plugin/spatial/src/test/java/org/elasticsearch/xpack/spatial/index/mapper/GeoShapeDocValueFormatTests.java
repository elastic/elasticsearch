/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.startsWith;

public class GeoShapeDocValueFormatTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateSpatialPlugin.class);
    }

    /**
     * geo_shape uses binary doc values internally. As geo_shape does not work well with binary doc_value compression, if logsdb
     * is used, geo_shape should use the Lucene doc value format rather than ES819TSDBDocValuesFormat.
     */
    public void testGeoShapeDocValueUseLuceneFormat() throws IOException {
        String indexName = "test";
        String fieldName = "field_name";
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName()).build();
        IndexService indexService = createIndex(indexName, settings, "@timestamp", "type=date", fieldName, "type=geo_shape");

        var indexRequest = new IndexRequest(indexName).opType(DocWriteRequest.OpType.CREATE).source("""
            {
              "@timestamp": "2025-10-01T12:34:56.789",
              "%field": {
                "type" : "Point",
                "coordinates" : [-77.03653, 38.897676]
              }
            }
            """.replace("%field", fieldName), XContentType.JSON);
        var response = client().bulk(new BulkRequest().add(indexRequest)).actionGet();
        assertFalse(response.hasFailures());
        safeGet(indicesAdmin().refresh(new RefreshRequest(indexName).indicesOptions(IndicesOptions.lenientExpandOpenHidden())));

        try (var searcher = indexService.getShard(0).acquireSearcher(indexName)) {
            try (var indexReader = searcher.getIndexReader()) {
                var leaves = indexReader.leaves();
                assertThat(leaves.size(), equalTo(1));
                FieldInfos fieldInfos = leaves.getFirst().reader().getFieldInfos();
                FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);
                assertNotNull(fieldInfo);
                Map<String, String> attributes = fieldInfo.attributes();
                assertThat(attributes, hasKey("PerFieldDocValuesFormat.format"));
                assertThat(attributes.get("PerFieldDocValuesFormat.format"), startsWith("Lucene"));
            }
        }
    }
}
