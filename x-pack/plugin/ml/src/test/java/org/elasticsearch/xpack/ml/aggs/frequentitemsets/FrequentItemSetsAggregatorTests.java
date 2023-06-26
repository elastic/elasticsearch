/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.EclatMapReducer.EclatResult;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.FrequentItemSetCollector.FrequentItemSet;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.InternalItemSetMapReduceAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.xpack.ml.aggs.frequentitemsets.FrequentItemSetsAggregationBuilder.EXECUTION_HINT_ALLOWED_MODES;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class FrequentItemSetsAggregatorTests extends AggregatorTestCase {

    private static final String DATE_FIELD = "tVal";
    private static final String INT_FIELD = "iVal";
    private static final String FLOAT_FIELD = "fVal";
    private static final String IP_FIELD = "ipVal";
    private static final String KEYWORD_FIELD1 = "kVal-1";
    private static final String KEYWORD_FIELD2 = "kVal-2";
    private static final String KEYWORD_FIELD3 = "kVal-3";

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

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new FrequentItemSetsAggregationBuilder(
            "fi",
            List.of(new MultiValuesSourceFieldConfig.Builder().setFieldName(fieldName).build()),
            FrequentItemSetsAggregationBuilder.DEFAULT_MINIMUM_SUPPORT,
            FrequentItemSetsAggregationBuilder.DEFAULT_MINIMUM_SET_SIZE,
            FrequentItemSetsAggregationBuilder.DEFAULT_SIZE,
            null,
            randomFrom(EXECUTION_HINT_ALLOWED_MODES)
        );
    }

    public void testKeywordsArray() throws IOException {
        List<MultiValuesSourceFieldConfig> fields = new ArrayList<>();

        String exclude = randomBoolean() ? randomFrom("item-3", "item-4", "item-5", "item-99") : null;
        fields.add(
            new MultiValuesSourceFieldConfig.Builder().setFieldName(KEYWORD_FIELD1)
                .setIncludeExclude(
                    exclude != null ? new IncludeExclude(null, null, null, new TreeSet<>(Set.of(new BytesRef(exclude)))) : null
                )
                .build()
        );

        double minimumSupport = randomDoubleBetween(0.13, 0.41, true);
        int minimumSetSize = randomIntBetween(2, 5);
        int size = randomIntBetween(1, 100);
        Query query = new MatchAllDocsQuery();
        MappedFieldType keywordType = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD1);

        List<FrequentItemSet> expectedResults = List.of(
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-1", "item-3")), 7, 0.7),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-1", "item-7")), 6, 0.6),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-3", "item-7")), 5, 0.5),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-1", "item-3", "item-7")), 4, 0.4),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-3", "item-7", "item-8")), 3, 0.3),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-3", "item-4")), 3, 0.3),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-3", "item-6")), 3, 0.3),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-1", "item-3", "item-7", "item-8")), 2, 0.2),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-1", "item-3", "item-7", "item-9")), 2, 0.2),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-3", "item-4", "item-7", "item-8")), 2, 0.2),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-1", "item-3", "item-4")), 2, 0.2),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-1", "item-3", "item-6")), 2, 0.2),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-3", "item-4", "item-6")), 2, 0.2),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-3", "item-6", "item-7")), 2, 0.2),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-2", "item-3")), 2, 0.2),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("item-3", "item-5")), 2, 0.2)
        );

        FrequentItemSetsAggregationBuilder builder = new FrequentItemSetsAggregationBuilder(
            "fi",
            fields,
            minimumSupport,
            minimumSetSize,
            size,
            null,
            randomFrom(EXECUTION_HINT_ALLOWED_MODES)
        );

        testCase(iw -> {
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-2")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-3"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-3")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-5"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-8")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-3")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-9")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-7")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-4"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-7")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-8")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-3"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-7"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-7")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-6")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-3"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-3")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-6")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-4"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-7")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-9")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-3"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-2")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-3")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-4")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-5")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-6")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-7")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-8"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("item-7"))
                )
            );
        }, (InternalItemSetMapReduceAggregation<?, ?, ?, EclatResult> results) -> {
            assertNotNull(results);
            assertResults(
                expectedResults,
                results.getMapReduceResult().getFrequentItemSets(),
                minimumSupport,
                minimumSetSize,
                size,
                exclude,
                null
            );
        }, new AggTestConfig(builder, keywordType).withQuery(query));
    }

    public void testMixedSingleValues() throws IOException {
        List<MultiValuesSourceFieldConfig> fields = new ArrayList<>();

        String stringExclude = randomBoolean() ? randomFrom("host-2", "192.168.0.1", "client-2", "127.0.0.1") : null;
        Integer intExclude = randomBoolean() ? randomIntBetween(0, 10) : null;

        fields.add(
            new MultiValuesSourceFieldConfig.Builder().setFieldName(KEYWORD_FIELD1)
                .setIncludeExclude(
                    stringExclude != null ? new IncludeExclude(null, null, null, new TreeSet<>(Set.of(new BytesRef(stringExclude)))) : null
                )
                .build()
        );
        fields.add(
            new MultiValuesSourceFieldConfig.Builder().setFieldName(KEYWORD_FIELD2)
                .setIncludeExclude(
                    stringExclude != null ? new IncludeExclude(null, null, null, new TreeSet<>(Set.of(new BytesRef(stringExclude)))) : null
                )
                .build()
        );
        fields.add(
            new MultiValuesSourceFieldConfig.Builder().setFieldName(KEYWORD_FIELD3)
                .setIncludeExclude(
                    stringExclude != null ? new IncludeExclude(null, null, null, new TreeSet<>(Set.of(new BytesRef(stringExclude)))) : null
                )
                .build()
        );
        fields.add(new MultiValuesSourceFieldConfig.Builder().setFieldName(FLOAT_FIELD).build());
        fields.add(
            new MultiValuesSourceFieldConfig.Builder().setFieldName(INT_FIELD)
                .setIncludeExclude(
                    intExclude != null
                        ? new IncludeExclude(null, null, null, new TreeSet<>(Set.of(new BytesRef(String.valueOf(intExclude)))))
                        : null
                )
                .build()
        );
        fields.add(
            new MultiValuesSourceFieldConfig.Builder().setFieldName(IP_FIELD)
                .setIncludeExclude(
                    stringExclude != null
                        ? stringExclude.startsWith("1") // only add exclude if it is an IP
                            ? new IncludeExclude(null, null, null, new TreeSet<>(Set.of(new BytesRef(stringExclude))))
                            : null
                        : null
                )
                .build()
        );

        double minimumSupport = randomDoubleBetween(0.13, 0.51, true);
        int minimumSetSize = randomIntBetween(2, 6);
        int size = randomIntBetween(1, 100);

        Query query = new MatchAllDocsQuery();
        MappedFieldType keywordType1 = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD1);
        MappedFieldType keywordType2 = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD2);
        MappedFieldType keywordType3 = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD3);
        MappedFieldType intType = new NumberFieldMapper.NumberFieldType(INT_FIELD, NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType floatType = new NumberFieldMapper.NumberFieldType(FLOAT_FIELD, NumberFieldMapper.NumberType.FLOAT);
        MappedFieldType ipType = new IpFieldMapper.IpFieldType(IP_FIELD);

        List<FrequentItemSet> expectedResults = List.of(
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("host-1"), KEYWORD_FIELD3, List.of("type-1")), 5, 0.5),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("host-2"), KEYWORD_FIELD3, List.of("type-2")), 3, 0.3),
            new FrequentItemSet(
                Map.of(
                    FLOAT_FIELD,
                    List.of(4.1f),
                    KEYWORD_FIELD1,
                    List.of("host-1"),
                    KEYWORD_FIELD2,
                    List.of("client-1"),
                    KEYWORD_FIELD3,
                    List.of("type-1")
                ),
                2,
                0.2
            ),
            new FrequentItemSet(
                Map.of(IP_FIELD, List.of("192.168.0.5"), KEYWORD_FIELD1, List.of("host-1"), KEYWORD_FIELD3, List.of("type-1")),
                2,
                0.2
            ),
            new FrequentItemSet(
                Map.of(KEYWORD_FIELD1, List.of("host-1"), KEYWORD_FIELD2, List.of("client-2"), KEYWORD_FIELD3, List.of("type-1")),
                2,
                0.2
            ),
            new FrequentItemSet(
                Map.of(KEYWORD_FIELD1, List.of("host-2"), KEYWORD_FIELD2, List.of("client-2"), KEYWORD_FIELD3, List.of("type-3")),
                2,
                0.2
            ),
            new FrequentItemSet(Map.of(IP_FIELD, List.of("192.168.0.1"), FLOAT_FIELD, List.of(4.1f), INT_FIELD, List.of(2)), 2, 0.2),
            new FrequentItemSet(Map.of(FLOAT_FIELD, List.of(5.0f), KEYWORD_FIELD1, List.of("host-2")), 2, 0.2),
            new FrequentItemSet(Map.of(INT_FIELD, List.of(5), KEYWORD_FIELD1, List.of("host-2")), 2, 0.2),
            new FrequentItemSet(Map.of(FLOAT_FIELD, List.of(5.0f), KEYWORD_FIELD2, List.of("client-2")), 2, 0.2),
            new FrequentItemSet(Map.of(IP_FIELD, List.of("192.168.0.5"), KEYWORD_FIELD2, List.of("client-2")), 2, 0.2)
        );

        FrequentItemSetsAggregationBuilder builder = new FrequentItemSetsAggregationBuilder(
            "fi",
            fields,
            minimumSupport,
            minimumSetSize,
            size,
            null,
            randomFrom(EXECUTION_HINT_ALLOWED_MODES)
        );

        testCase(iw -> {
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-1")),
                    new NumericDocValuesField(INT_FIELD, 2),
                    new FloatDocValuesField(FLOAT_FIELD, 4.1f),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-1"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-2")),
                    new NumericDocValuesField(INT_FIELD, 5),
                    new FloatDocValuesField(FLOAT_FIELD, 3.0f),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.4")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-2"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-1")),
                    new NumericDocValuesField(INT_FIELD, 7),
                    new FloatDocValuesField(FLOAT_FIELD, 5.0f),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.4")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-2")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-1"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-2")),
                    new NumericDocValuesField(INT_FIELD, 1),
                    new FloatDocValuesField(FLOAT_FIELD, 5.2f),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.22")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-3")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-2"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-1")),
                    new NumericDocValuesField(INT_FIELD, 1),
                    new FloatDocValuesField(FLOAT_FIELD, 14.0f),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.12")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-5")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-1"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-2")),
                    new NumericDocValuesField(INT_FIELD, 2),
                    new FloatDocValuesField(FLOAT_FIELD, 4.1f),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-2")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-3"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-2")),
                    new NumericDocValuesField(INT_FIELD, 5),
                    new FloatDocValuesField(FLOAT_FIELD, 5.0f),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.5")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-2")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-3"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-1")),
                    new NumericDocValuesField(INT_FIELD, 4),
                    new FloatDocValuesField(FLOAT_FIELD, 4.1f),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.5")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-1"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-1")),
                    new NumericDocValuesField(INT_FIELD, 6),
                    new FloatDocValuesField(FLOAT_FIELD, 7.0f),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.5")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-2")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-1"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-2")),
                    new NumericDocValuesField(INT_FIELD, 15),
                    new FloatDocValuesField(FLOAT_FIELD, 25.0f),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.15")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-8")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-2"))
                )
            );
        }, (InternalItemSetMapReduceAggregation<?, ?, ?, EclatResult> results) -> {
            assertNotNull(results);
            assertResults(
                expectedResults,
                results.getMapReduceResult().getFrequentItemSets(),
                minimumSupport,
                minimumSetSize,
                size,
                stringExclude,
                intExclude
            );
        }, new AggTestConfig(builder, keywordType1, keywordType2, keywordType3, intType, floatType, ipType).withQuery(query));

    }

    public void testSingleValueWithDate() throws IOException {
        List<MultiValuesSourceFieldConfig> fields = new ArrayList<>();

        String dateExclude = randomBoolean() ? randomFrom("2022-06-02", "2022-06-03", "1970-01-01") : null;

        fields.add(new MultiValuesSourceFieldConfig.Builder().setFieldName(KEYWORD_FIELD1).build());
        fields.add(new MultiValuesSourceFieldConfig.Builder().setFieldName(KEYWORD_FIELD2).build());
        fields.add(new MultiValuesSourceFieldConfig.Builder().setFieldName(KEYWORD_FIELD3).build());
        fields.add(
            new MultiValuesSourceFieldConfig.Builder().setFieldName(DATE_FIELD)
                .setIncludeExclude(
                    dateExclude != null ? new IncludeExclude(null, null, null, new TreeSet<>(Set.of(new BytesRef(dateExclude)))) : null
                )
                .build()
        );

        double minimumSupport = randomDoubleBetween(0.13, 0.51, true);
        int minimumSetSize = randomIntBetween(2, 6);
        int size = randomIntBetween(1, 100);

        Query query = new MatchAllDocsQuery();
        MappedFieldType keywordType1 = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD1);
        MappedFieldType keywordType2 = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD2);
        MappedFieldType keywordType3 = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD3);
        MappedFieldType dateType = dateFieldType(DATE_FIELD);

        // mix in an ip field, which we do not analyze
        MappedFieldType ipType = new IpFieldMapper.IpFieldType(IP_FIELD);

        List<FrequentItemSet> expectedResults = List.of(
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("host-1"), KEYWORD_FIELD3, List.of("type-1")), 5, 0.5),
            new FrequentItemSet(
                Map.of(KEYWORD_FIELD1, List.of("host-1"), KEYWORD_FIELD2, List.of("client-1"), KEYWORD_FIELD3, List.of("type-1")),
                3,
                0.3
            ),
            new FrequentItemSet(
                Map.of(KEYWORD_FIELD1, List.of("host-1"), KEYWORD_FIELD3, List.of("type-1"), DATE_FIELD, List.of("2022-06-03")),
                3,
                0.3
            ),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("host-2"), DATE_FIELD, List.of("2022-06-02")), 3, 0.3),
            new FrequentItemSet(Map.of(KEYWORD_FIELD1, List.of("host-2"), KEYWORD_FIELD3, List.of("type-2")), 3, 0.3),
            new FrequentItemSet(Map.of(KEYWORD_FIELD2, List.of("client-1"), DATE_FIELD, List.of("2022-06-03")), 3, 0.3),
            new FrequentItemSet(
                Map.of(
                    KEYWORD_FIELD1,
                    List.of("host-2"),
                    KEYWORD_FIELD2,
                    List.of("client-2"),
                    DATE_FIELD,
                    List.of("2022-06-02"),
                    KEYWORD_FIELD3,
                    List.of("type-3")
                ),
                2,
                0.2
            ),
            new FrequentItemSet(
                Map.of(
                    KEYWORD_FIELD1,
                    List.of("host-1"),
                    KEYWORD_FIELD2,
                    List.of("client-1"),
                    DATE_FIELD,
                    List.of("2022-06-03"),
                    KEYWORD_FIELD3,
                    List.of("type-1")
                ),
                2,
                0.2
            ),
            new FrequentItemSet(
                Map.of(KEYWORD_FIELD1, List.of("host-2"), KEYWORD_FIELD2, List.of("client-2"), KEYWORD_FIELD3, List.of("type-2")),
                2,
                0.2
            ),
            new FrequentItemSet(
                Map.of(KEYWORD_FIELD1, List.of("host-1"), DATE_FIELD, List.of("2022-06-01"), KEYWORD_FIELD3, List.of("type-1")),
                2,
                0.2
            ),
            new FrequentItemSet(
                Map.of(KEYWORD_FIELD1, List.of("host-1"), KEYWORD_FIELD2, List.of("client-2"), KEYWORD_FIELD3, List.of("type-1")),
                2,
                0.2
            )

        );

        FrequentItemSetsAggregationBuilder builder = new FrequentItemSetsAggregationBuilder(
            "fi",
            fields,
            minimumSupport,
            minimumSetSize,
            size,
            null,
            randomFrom(EXECUTION_HINT_ALLOWED_MODES)
        );

        testCase(iw -> {
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-1")),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-1")),
                    new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-06-03"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-2")),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.4")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-2")),
                    new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-06-03"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-1")),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.4")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-1")),
                    new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-06-01"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-2")),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.22")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-3")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-2")),
                    new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-06-01"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-1")),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.12")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-2")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-1")),
                    new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-06-03"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-2")),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-2")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-3")),
                    new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-06-02"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-2")),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.5")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-2")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-3")),
                    new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-06-02"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-1")),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.5")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-1")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-1")),
                    new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-06-03"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-1")),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.5")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-2")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-1")),
                    new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-06-01"))
                )
            );
            iw.addDocument(
                List.of(
                    new SortedSetDocValuesField(KEYWORD_FIELD1, new BytesRef("host-2")),
                    new SortedSetDocValuesField(IP_FIELD, encodeIp("192.168.0.15")),
                    new SortedSetDocValuesField(KEYWORD_FIELD2, new BytesRef("client-3")),
                    new SortedSetDocValuesField(KEYWORD_FIELD3, new BytesRef("type-2")),
                    new SortedNumericDocValuesField(DATE_FIELD, dateFieldType(DATE_FIELD).parse("2022-06-02"))
                )
            );
        }, (InternalItemSetMapReduceAggregation<?, ?, ?, EclatResult> results) -> {
            assertNotNull(results);
            assertResults(
                expectedResults,
                results.getMapReduceResult().getFrequentItemSets(),
                minimumSupport,
                minimumSetSize,
                size,
                dateExclude,
                null
            );
        }, new AggTestConfig(builder, keywordType1, keywordType2, keywordType3, dateType, ipType).withQuery(query));

    }

    private static BytesRef encodeIp(String ip) {
        return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(ip)));
    }

    private DateFieldMapper.DateFieldType dateFieldType(String name) {
        return new DateFieldMapper.DateFieldType(
            name,
            true,
            false,
            true,
            DateFormatter.forPattern("strict_date_optional_time"),
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );
    }

    private void assertResults(
        List<FrequentItemSet> expected,
        FrequentItemSet[] actual,
        double minSupport,
        int minimumSetSize,
        int size,
        String stringExclude,
        Integer intExclude
    ) {
        // sort the expected results descending by doc count
        expected.get(0).getFields().values().stream().mapToLong(v -> v.stream().count()).sum();

        List<FrequentItemSet> filteredExpectedWithDups = expected.stream()
            .map(
                fi -> new FrequentItemSet(
                    fi.getFields()
                        .entrySet()
                        .stream()
                        .map(
                            // filter the string exclude from the list of objects
                            keyValues -> tuple(
                                keyValues.getKey(),
                                keyValues.getValue().stream().filter(v -> v.equals(stringExclude) == false).collect(Collectors.toList())
                            )
                        )
                        .map(
                            // filter the int exclude
                            keyValues -> tuple(
                                keyValues.v1(),
                                keyValues.v2().stream().filter(v -> v.equals(intExclude) == false).collect(Collectors.toList())
                            )
                        )
                        // after filtering out excludes the list of objects might be empty
                        .filter(t -> t.v2().size() > 0)
                        .collect(Collectors.toMap(Tuple::v1, Tuple::v2)),
                    fi.getDocCount(),
                    fi.getSupport()
                )
            )
            .filter(fi -> fi.getSupport() >= minSupport)
            .filter(fi -> {
                return fi.getFields().values().stream().map(v -> v.stream().count()).mapToLong(e -> e.longValue()).sum() >= minimumSetSize;
            })
            .sorted((a, b) -> {
                if (a.getDocCount() == b.getDocCount()) {
                    if (b.getFields().size() == a.getFields().size()) {
                        return Strings.collectionToCommaDelimitedString(
                            a.getFields()
                                .entrySet()
                                .stream()
                                .map(e -> e.getKey() + ": " + e.getValue())
                                .sorted(String::compareTo)
                                .collect(Collectors.toList())
                        )
                            .compareTo(
                                Strings.collectionToCommaDelimitedString(
                                    b.getFields()
                                        .entrySet()
                                        .stream()
                                        .map(e -> e.getKey() + ": " + e.getValue())
                                        .sorted(String::compareTo)
                                        .collect(Collectors.toList())
                                )
                            );
                    }
                    return b.getFields().size() - a.getFields().size();
                }
                return (int) (b.getDocCount() - a.getDocCount());
            })
            .collect(Collectors.toList());

        // after removing excluded items there might be duplicate entries, which need to be collapsed, we do this by very simple hashing
        List<FrequentItemSet> filteredExpected = new ArrayList<>();
        Set<Integer> valuesSeen = new HashSet<>();
        for (FrequentItemSet fi : filteredExpectedWithDups) {
            if (valuesSeen.add(
                fi.getFields().entrySet().stream().mapToInt(v -> Objects.hash(v.getKey(), v.getValue())).reduce(13, (t, s) -> 41 * t + s)
            )) {
                filteredExpected.add(fi);
            }
        }

        // if size applies, cut the list, however if sets have the same number of items it's unclear which ones are returned
        int additionalSetsThatShareTheSameDocCount = 0;
        if (size < filteredExpected.size()) {
            int sizeAtCut = filteredExpected.get(size - 1).getFields().size();
            int startCutPosition = size;

            while (startCutPosition < filteredExpected.size() && filteredExpected.get(startCutPosition).getFields().size() == sizeAtCut) {
                ++startCutPosition;
                ++additionalSetsThatShareTheSameDocCount;
            }

            filteredExpected = filteredExpected.subList(0, startCutPosition);
        }

        String setsAssertMessage = "expected: ["
            + Strings.collectionToDelimitedString(filteredExpected, ", ")
            + "] got ["
            + Strings.arrayToDelimitedString(actual, ", ")
            + "] parameters: minumum_support: "
            + minSupport
            + " minimum_set_size: "
            + minimumSetSize
            + " size: "
            + size
            + " string exclude: ["
            + stringExclude
            + "] int exclude: ["
            + intExclude
            + "]";

        assertEquals(
            "number of results do not match, " + setsAssertMessage,
            filteredExpected.size() - additionalSetsThatShareTheSameDocCount,
            actual.length
        );
        List<FrequentItemSet> unmatchedActual = new ArrayList<>(Arrays.asList(actual));

        // matching the expected set and actual set is complicated as values can be differently ordered,
        // we can rely on the item sets being ordered by doc count, however the values in arrays or not deterministic
        for (FrequentItemSet expectedSet : filteredExpected) {
            boolean foundSet = false;
            for (int i = 0; i < unmatchedActual.size(); ++i) {
                // the doc count must be the same
                assertEquals(
                    "did not find item [" + expectedSet + "], " + setsAssertMessage,
                    unmatchedActual.get(i).getDocCount(),
                    expectedSet.getDocCount()
                );
                // support is just doc count in relation to overall count, so this should not bring any surprises
                assertEquals(
                    "did not find item [" + expectedSet + "], " + setsAssertMessage,
                    unmatchedActual.get(i).getSupport(),
                    expectedSet.getSupport(),
                    0.00001
                );

                assertEquals(
                    "did not find item in the expected order(longer first) [" + expectedSet + "], all sets: " + setsAssertMessage,
                    expectedSet.getFields().size(),
                    unmatchedActual.get(i).getFields().size()
                );
                if (expectedSet.getFields().keySet().equals(unmatchedActual.get(i).getFields().keySet())) {
                    boolean matchedAllValuesForOneField = false;
                    for (Entry<String, List<Object>> entry : expectedSet.getFields().entrySet()) {
                        if (containsInAnyOrder(entry.getValue()).matches(unmatchedActual.get(i).getFields().get(entry.getKey()))) {
                            matchedAllValuesForOneField = true;
                        }

                        if (matchedAllValuesForOneField) {
                            break;
                        }
                    }

                    // found the expected item, remove it and jump to the next item set
                    if (matchedAllValuesForOneField == false) {
                        unmatchedActual.remove(i);
                        foundSet = true;
                        break;
                    }
                }
            }
            if (foundSet == false && additionalSetsThatShareTheSameDocCount > 0) {
                // did not find an item, but it could be one cut by the size parameter
                --additionalSetsThatShareTheSameDocCount;
            } else {
                assertTrue("did not find item [" + expectedSet + "], " + setsAssertMessage, foundSet);
            }
        }

        assertEquals("more items found than expected, " + setsAssertMessage, 0, unmatchedActual.size());
    }
}
