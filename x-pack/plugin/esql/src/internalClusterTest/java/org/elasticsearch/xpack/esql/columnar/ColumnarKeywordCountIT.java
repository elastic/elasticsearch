/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.columnar;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class ColumnarKeywordCountIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    public void testCountSingleValuedKeywordCountsValues() {
        createColumnarKeywordIndex("single_value");

        int docs = randomIntBetween(2, 10);
        for (int doc = 0; doc < docs; doc++) {
            prepareIndex("single_value").setSource("kw", randomAlphaOfLengthBetween(3, 12)).get();
        }
        indicesAdmin().prepareRefresh("single_value").get();

        assertCount("single_value", docs);
    }

    public void testCountMultivaluedKeywordCountsValues() {
        createColumnarKeywordIndex("multi_value");

        int expectedCount = 0;
        int docs = randomIntBetween(2, 10);
        for (int doc = 0; doc < docs; doc++) {
            int valuesInDoc = randomIntBetween(2, 8);
            List<String> values = new ArrayList<>(valuesInDoc);
            for (int value = 0; value < valuesInDoc; value++) {
                values.add(randomAlphaOfLengthBetween(3, 12));
            }
            expectedCount += valuesInDoc;

            prepareIndex("multi_value").setSource("kw", values).get();
        }
        indicesAdmin().prepareRefresh("multi_value").get();

        assertCount("multi_value", expectedCount);
    }

    private void createColumnarKeywordIndex(String index) {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            ).setMapping("kw", "type=keyword")
        );
    }

    private void assertCount(String index, int expectedCount) {
        try (EsqlQueryResponse response = run("FROM " + index + " | STATS n = COUNT(kw)")) {
            Iterator<Object> row = response.values().next();
            assertThat(row.next(), equalTo((long) expectedCount));
            assertFalse(row.hasNext());
        }
    }
}
