/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.apache.lucene.util.English;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class IndexDiskUsageAnalyzerIT extends ESIntegTestCase {

    public void testSimple() throws Exception {
        final XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("_doc");
            {
                mapping.startObject("properties");
                {
                    mapping.startObject("english_text");
                    mapping.field("type", "text");
                    mapping.endObject();

                    mapping.startObject("value");
                    mapping.field("type", "long");
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        final String index = "test-index";
        client().admin().indices().prepareCreate(index)
            .setMapping(mapping)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();
        ensureGreen(index);

        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            int value = randomIntBetween(1, 1024);
            final XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .field("english_text", English.intToEnglish(value))
                .field("value", value)
                .endObject();
            client().prepareIndex(index)
                .setId("id-" + i)
                .setSource(doc)
                .get();
        }
        final boolean forceNorms = randomBoolean();
        if (forceNorms) {
            final XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .field("english_text", "A long sentence to make sure that norms is non-zero")
                .endObject();
            client().prepareIndex(index)
                .setId("id")
                .setSource(doc)
                .get();
        }
        PlainActionFuture<AnalyzeIndexDiskUsageResponse> future = PlainActionFuture.newFuture();
        client().execute(AnalyzeIndexDiskUsageAction.INSTANCE,
            new AnalyzeIndexDiskUsageRequest(new String[] {index}, AnalyzeIndexDiskUsageRequest.DEFAULT_INDICES_OPTIONS, true),
            future);

        AnalyzeIndexDiskUsageResponse resp = future.actionGet();
        final IndexDiskUsageStats stats = resp.getStats().get(index);
        logger.info("--> stats {}", stats);
        assertNotNull(stats);
        assertThat(stats.getIndexSizeInBytes(), greaterThan(100L));

        final IndexDiskUsageStats.PerFieldDiskUsage englishField = stats.getFields().get("english_text");
        assertThat(englishField.getInvertedIndexBytes(), greaterThan(0L));
        assertThat(englishField.getStoredFieldBytes(), equalTo(0L));
        if (forceNorms) {
            assertThat(englishField.getNormsBytes(), greaterThan(0L));
        }
        final IndexDiskUsageStats.PerFieldDiskUsage valueField = stats.getFields().get("value");
        assertThat(valueField.getInvertedIndexBytes(), equalTo(0L));
        assertThat(valueField.getStoredFieldBytes(), equalTo(0L));
        assertThat(valueField.getPointsBytes(), greaterThan(0L));
        assertThat(valueField.getDocValuesBytes(), greaterThan(0L));

        assertMetadataFields(stats);
    }


    void assertMetadataFields(IndexDiskUsageStats stats) {
        final IndexDiskUsageStats.PerFieldDiskUsage sourceField = stats.getFields().get("_source");
        assertThat(sourceField.getInvertedIndexBytes(), equalTo(0L));
        assertThat(sourceField.getStoredFieldBytes(), greaterThan(0L));
        assertThat(sourceField.getPointsBytes(), equalTo(0L));
        assertThat(sourceField.getDocValuesBytes(), equalTo(0L));

        final IndexDiskUsageStats.PerFieldDiskUsage idField = stats.getFields().get("_id");
        assertThat(idField.getInvertedIndexBytes(), greaterThan(0L));
        assertThat(idField.getStoredFieldBytes(), greaterThan(0L));
        assertThat(idField.getPointsBytes(), equalTo(0L));
        assertThat(idField.getDocValuesBytes(), equalTo(0L));

        final IndexDiskUsageStats.PerFieldDiskUsage seqNoField = stats.getFields().get("_seq_no");
        assertThat(seqNoField.getInvertedIndexBytes(), equalTo(0L));
        assertThat(seqNoField.getStoredFieldBytes(), equalTo(0L));
        assertThat(seqNoField.getPointsBytes(), greaterThan(0L));
        assertThat(seqNoField.getDocValuesBytes(), greaterThan(0L));
    }
}
