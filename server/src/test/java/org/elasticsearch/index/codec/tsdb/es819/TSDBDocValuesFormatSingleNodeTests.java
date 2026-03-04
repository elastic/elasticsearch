/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.Elasticsearch92Lucene103Codec;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.sameInstance;

public class TSDBDocValuesFormatSingleNodeTests extends ESSingleNodeTestCase {

    public void testTSDBIndexUsesCorrectDocValuesFormat() throws Exception {
        String indexName = "tsdb-test";
        Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2024-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-01-01T00:00:00Z")
            .build();

        createIndex(
            indexName,
            settings,
            "@timestamp",
            "type=date",
            "hostname",
            "type=keyword,time_series_dimension=true",
            "gauge",
            "type=long,time_series_metric=gauge"
        );

        indexDocuments(indexName);

        Set<String> expectedFields = Set.of("@timestamp", "hostname", "gauge", "_tsid", "_ts_routing_hash");
        assertDocValuesFormat(indexName, TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_NUMERIC_BLOCK, expectedFields);
    }

    public void testStandardIndexWithTSDBDocValuesFormatSetting() throws Exception {
        String indexName = "standard-tsdb-dv-test";
        Settings settings = Settings.builder().put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true).build();

        createIndex(indexName, settings, "@timestamp", "type=date", "hostname", "type=keyword", "gauge", "type=long");

        indexDocuments(indexName);

        Set<String> expectedFields = Set.of("@timestamp", "hostname", "gauge", "_seq_no");
        assertDocValuesFormat(indexName, TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT, expectedFields);
    }

    private void indexDocuments(String indexName) throws Exception {
        long baseTimestamp = 1704067200000L;
        int numDocs = randomIntBetween(5, 20);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", baseTimestamp + i * 1000)
                    .field("hostname", "host-" + (i % 3))
                    .field("gauge", randomLong())
                    .endObject()
            ).get();
        }
    }

    private void assertDocValuesFormat(String indexName, DocValuesFormat expectedFormat, Set<String> expectedFields) {
        var indexService = getInstanceFromNode(IndicesService.class).indexServiceSafe(resolveIndex(indexName));
        var shard = indexService.getShard(0);
        var codec = (Elasticsearch92Lucene103Codec) shard.getEngineOrNull().config().getCodec();
        for (String field : expectedFields) {
            DocValuesFormat writerFormat = codec.getDocValuesFormatForField(field);
            assertThat(
                "IndexWriter codec for field [" + field + "] should return the expected TSDB doc values format instance",
                writerFormat,
                sameInstance(expectedFormat)
            );
        }

        indicesAdmin().flush(new FlushRequest(indexName).force(true)).actionGet();
        indicesAdmin().prepareRefresh(indexName).get();

        try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
            var leaves = searcher.getDirectoryReader().leaves();
            assertThat(leaves.size(), greaterThan(0));

            for (var leaf : leaves) {
                for (FieldInfo fieldInfo : leaf.reader().getFieldInfos()) {
                    if (fieldInfo.getDocValuesType() == DocValuesType.NONE) {
                        continue;
                    }
                    String formatAttr = fieldInfo.getAttribute("PerFieldDocValuesFormat.format");
                    if (expectedFields.contains(fieldInfo.name)) {
                        assertThat(
                            "field [" + fieldInfo.name + "] should use the TSDB doc values format",
                            formatAttr,
                            equalTo(ES819Version3TSDBDocValuesFormat.CODEC_NAME)
                        );
                    }
                }
            }
        }
    }
}
