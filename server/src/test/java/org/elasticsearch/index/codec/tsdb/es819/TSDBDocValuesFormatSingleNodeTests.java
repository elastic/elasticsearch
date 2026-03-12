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
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.LegacyPerFieldMapperCodec;
import org.elasticsearch.index.codec.tsdb.ES93TSDBDefaultCompressionLucene103Codec;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.index.IndexSettings.ALLOW_LARGE_BINARY_BLOCK_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class TSDBDocValuesFormatSingleNodeTests extends ESSingleNodeTestCase {

    public void testTSDBIndexUsesCorrectDocValuesFormat() throws Exception {
        String indexName = "tsdb-test";
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2024-01-01T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-01-01T00:00:00Z");
        if (IndexSettings.TSDB_SYNTHETIC_ID_FEATURE_FLAG) {
            settingsBuilder.put(IndexSettings.SYNTHETIC_ID.getKey(), randomBoolean());
        }
        Settings settings = settingsBuilder.build();

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

    public void testTimeSeriesDocValuesFormatLargeBinaryBlockSize() throws Exception {
        assumeTrue("requires feature flag enabled", ALLOW_LARGE_BINARY_BLOCK_SIZE.isEnabled());
        String indexName = "standard-larger-binary-db-block-size-dv-test";
        Settings settings = Settings.builder()
            .put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true)
            .put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK_SIZE.getKey(), true)
            .build();

        createIndex(indexName, settings, "@timestamp", "type=date", "hostname", "type=keyword", "gauge", "type=long");

        indexDocuments(indexName);

        Set<String> expectedFields = Set.of("@timestamp", "hostname", "gauge", "_seq_no");
        assertDocValuesFormat(indexName, TSDBDocValuesFormatFactory.ES_819_3_TSDB_DOC_VALUES_FORMAT_LARGE_BINARY_BLOCK, expectedFields);
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
        var codec = shard.withEngineOrNull(engine -> engine.config().getCodec());
        Function<String, DocValuesFormat> docValuesFormatProvider;
        if (codec instanceof Elasticsearch93Lucene104Codec es93104codec) {
            docValuesFormatProvider = es93104codec::getDocValuesFormatForField;
        } else if (codec instanceof CodecService.DeduplicateFieldInfosCodec deduplicateFieldInfosCodec) {
            if (deduplicateFieldInfosCodec.delegate() instanceof LegacyPerFieldMapperCodec legacyPerFieldMapperCodec) {
                docValuesFormatProvider = legacyPerFieldMapperCodec::getDocValuesFormatForField;
            } else if (deduplicateFieldInfosCodec.delegate() instanceof ES93TSDBDefaultCompressionLucene103Codec es93TSDB103Codec) {
                assertThat(es93TSDB103Codec.docValuesFormat(), instanceOf(PerFieldDocValuesFormat.class));
                docValuesFormatProvider = (field) -> ((PerFieldDocValuesFormat) es93TSDB103Codec.docValuesFormat())
                    .getDocValuesFormatForField(field);
            } else {
                fail("Unexpected codec type: " + codec.getClass().getName());
                return;
            }
        } else {
            fail("Unexpected codec type: " + codec.getClass().getName());
            return;
        }
        for (String field : expectedFields) {
            DocValuesFormat writerFormat = docValuesFormatProvider.apply(field);
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
