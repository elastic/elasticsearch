/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;

public class RangeFieldSortIT extends ESSingleNodeTestCase {

    private static final String FIELD_NAME = "range";

    public void testSortingOnIntegerRangeFieldThrows400() throws Exception {
        String indexName = "int_range_index";
        ;
        createIndex(indexName, FIELD_NAME, RangeType.INTEGER.typeName());
        assertFailures(
            client().prepareSearch(indexName).setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort(FIELD_NAME).order(SortOrder.ASC)),
            RestStatus.BAD_REQUEST
        );
    }

    public void testSortingOnLongRangeFieldThrows400() throws Exception {
        String indexName = "long_range_index";
        createIndex(indexName, FIELD_NAME, RangeType.LONG.typeName());
        assertFailures(
            client().prepareSearch(indexName).setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort(FIELD_NAME).order(SortOrder.ASC)),
            RestStatus.BAD_REQUEST
        );
    }

    public void testSortingOnFloatRangeFieldThrows400() throws Exception {
        String indexName = "float_range_index";
        createIndex(indexName, FIELD_NAME, RangeType.FLOAT.typeName());
        assertFailures(
            client().prepareSearch(indexName).setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort(FIELD_NAME).order(SortOrder.ASC)),
            RestStatus.BAD_REQUEST
        );
    }

    public void testSortingOnDoubleRangeFieldThrows400() throws Exception {
        String indexName = "double_range_index";
        createIndex(indexName, FIELD_NAME, RangeType.DOUBLE.typeName());
        assertFailures(
            client().prepareSearch(indexName).setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort(FIELD_NAME).order(SortOrder.ASC)),
            RestStatus.BAD_REQUEST
        );
    }

    public void testSortingOnIpRangeFieldThrows400() throws Exception {
        String indexName = "ip_range_index";
        createIndex(indexName, FIELD_NAME, RangeType.IP.typeName());
        assertFailures(
            client().prepareSearch(indexName).setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort(FIELD_NAME).order(SortOrder.ASC)),
            RestStatus.BAD_REQUEST
        );
    }

    public void testSortingOnDateRangeFieldThrows400() throws Exception {
        String indexName = "date_range_index";
        createIndex(indexName, FIELD_NAME, RangeType.DATE.typeName());
        assertFailures(
            client().prepareSearch(indexName).setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort(FIELD_NAME).order(SortOrder.ASC)),
            RestStatus.BAD_REQUEST
        );
    }

    private void createIndex(String indexName, String rangeFieldName, String rangeFieldType) throws Exception {
        int numShards = randomIntBetween(1, 3);
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", numShards))
            .setMapping(createMapping(rangeFieldName, rangeFieldType))
            .get();
    }

    private XContentBuilder createMapping(String fieldName, String fieldType) throws Exception {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", fieldType)
            .endObject()
            .endObject()
            .endObject();
    }
}
