/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FrequentItemSetsAggregatorTests extends AggregatorTestCase {

    private static final String DATE_FIELD = "tVal";
    private static final String INT_FIELD = "iVal";
    private static final String FLOAT_FIELD = "fVal";
    private static final String KEYWORD_FIELD = "kVal";

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new MachineLearning(Settings.EMPTY));
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.KEYWORD,
            CoreValuesSourceType.IP
        );
    }

    public void testKeywords() throws IOException {
        List<MultiValuesSourceFieldConfig> fields = new ArrayList<>();
        double minimumSupport = 0.01;
        int minimumSetSize = 1;
        int size = 10;
        Query query = new MatchAllDocsQuery();
        MappedFieldType dateType = dateFieldType(DATE_FIELD);

        MappedFieldType intType = new NumberFieldMapper.NumberFieldType(INT_FIELD, NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType floatType = new NumberFieldMapper.NumberFieldType(FLOAT_FIELD, NumberFieldMapper.NumberType.FLOAT);
        MappedFieldType keywordType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD);

        FrequentItemSetsAggregationBuilder builder = new FrequentItemSetsAggregationBuilder(
            "fi",
            fields,
            minimumSupport,
            minimumSetSize,
            size
        );

        testCase(builder, query, iw -> {}, results -> {}, dateType, intType, floatType, keywordType);
    }

    private DateFieldMapper.DateFieldType dateFieldType(String name) {
        return new DateFieldMapper.DateFieldType(
            name,
            true,
            false,
            true,
            DateFormatter.forPattern("strict_date"),
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );
    }
}
