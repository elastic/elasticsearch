/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.aggs.frequentitemsets.FrequentItemSetsAggregationBuilder.EXECUTION_HINT_ALLOWED_MODES;
import static org.hamcrest.Matchers.hasSize;

public class FrequentItemSetsAggregationBuilderTests extends AbstractXContentSerializingTestCase<FrequentItemSetsAggregationBuilder> {

    public static FrequentItemSetsAggregationBuilder randomFrequentItemsSetsAggregationBuilder() {
        int numberOfFields = randomIntBetween(1, 20);
        Set<String> fieldNames = new HashSet<String>(numberOfFields);
        while (fieldNames.size() < numberOfFields) {
            fieldNames.add(randomAlphaOfLength(5));
        }
        List<MultiValuesSourceFieldConfig> fields = fieldNames.stream().map(name -> {
            MultiValuesSourceFieldConfig.Builder field = new MultiValuesSourceFieldConfig.Builder();
            field.setFieldName(randomAlphaOfLength(5));

            if (randomBoolean()) {
                field.setMissing(randomAlphaOfLength(5));
            }

            if (randomBoolean()) {
                field.setIncludeExclude(randomIncludeExclude());
            }

            return field.build();
        }).collect(Collectors.toList());

        return new FrequentItemSetsAggregationBuilder(
            randomAlphaOfLength(5),
            fields,
            randomDoubleBetween(0.0, 1.0, false),
            randomIntBetween(1, 20),
            randomIntBetween(1, 20),
            randomBoolean() ? QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)) : null,
            randomFrom(EXECUTION_HINT_ALLOWED_MODES)
        );
    }

    @Override
    protected FrequentItemSetsAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        assertThat(parsed.getAggregatorFactories(), hasSize(1));
        assertThat(parsed.getPipelineAggregatorFactories(), hasSize(0));
        FrequentItemSetsAggregationBuilder agg = (FrequentItemSetsAggregationBuilder) parsed.getAggregatorFactories().iterator().next();
        assertNull(parser.nextToken());
        assertNotNull(agg);

        return agg;
    }

    @Override
    protected Reader<FrequentItemSetsAggregationBuilder> instanceReader() {
        return FrequentItemSetsAggregationBuilder::new;
    }

    @Override
    protected FrequentItemSetsAggregationBuilder createTestInstance() {
        return randomFrequentItemsSetsAggregationBuilder();
    }

    @Override
    protected FrequentItemSetsAggregationBuilder mutateInstance(FrequentItemSetsAggregationBuilder instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                BaseAggregationBuilder.class,
                new ParseField(FrequentItemSetsAggregationBuilder.NAME),
                (p, n) -> FrequentItemSetsAggregationBuilder.PARSER.apply(p, (String) n)
            )
        );
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    public void testValidation() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new FrequentItemSetsAggregationBuilder(
                "fi",
                List.of(
                    new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldA").build(),
                    new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldB").build()
                ),
                1.2,
                randomIntBetween(1, 20),
                randomIntBetween(1, 20),
                null,
                randomFrom(EXECUTION_HINT_ALLOWED_MODES)
            )
        );
        assertEquals("[minimum_support] must be greater than 0 and less or equal to 1. Found [1.2] in [fi]", e.getMessage());

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new FrequentItemSetsAggregationBuilder(
                "fi",
                List.of(
                    new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldA").build(),
                    new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldB").build()
                ),
                randomDoubleBetween(0.0, 1.0, false),
                -4,
                randomIntBetween(1, 20),
                null,
                randomFrom(EXECUTION_HINT_ALLOWED_MODES)
            )
        );

        assertEquals("[minimum_set_size] must be greater than 0. Found [-4] in [fi]", e.getMessage());

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new FrequentItemSetsAggregationBuilder(
                "fi",
                List.of(
                    new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldA").build(),
                    new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldB").build()
                ),
                randomDoubleBetween(0.0, 1.0, false),
                randomIntBetween(1, 20),
                -2,
                null,
                randomFrom(EXECUTION_HINT_ALLOWED_MODES)
            )
        );

        assertEquals("[size] must be greater than 0. Found [-2] in [fi]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () ->

        new FrequentItemSetsAggregationBuilder(
            "fi",
            List.of(
                new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldA").build(),
                new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldB").build()
            ),
            randomDoubleBetween(0.0, 1.0, false),
            randomIntBetween(1, 20),
            randomIntBetween(1, 20),
            null,
            randomFrom(EXECUTION_HINT_ALLOWED_MODES)
        ).subAggregation(AggregationBuilders.avg("fieldA")));

        assertEquals("Aggregator [fi] of type [frequent_item_sets] cannot accept sub-aggregations", e.getMessage());

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new FrequentItemSetsAggregationBuilder(
                "fi",
                List.of(
                    new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldA").build(),
                    new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldB").build()
                ),
                randomDoubleBetween(0.0, 1.0, false),
                randomIntBetween(1, 20),
                randomIntBetween(1, 20),
                null,
                randomFrom(EXECUTION_HINT_ALLOWED_MODES)
            ).subAggregations(new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("fieldA")))
        );

        assertEquals("Aggregator [fi] of type [frequent_item_sets] cannot accept sub-aggregations", e.getMessage());

        e = expectThrows(
            IllegalArgumentException.class,
            () -> new FrequentItemSetsAggregationBuilder(
                "fi",
                List.of(
                    new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldA").build(),
                    new MultiValuesSourceFieldConfig.Builder().setFieldName("fieldB").build()
                ),
                randomDoubleBetween(0.0, 1.0, false),
                randomIntBetween(1, 20),
                randomIntBetween(1, 20),
                null,
                "wrong-execution-mode"
            )
        );

        assertEquals("[execution_hint] must be one of [global_ordinals,map]. Found [wrong-execution-mode]", e.getMessage());
    }

    private static IncludeExclude randomIncludeExclude() {
        switch (randomInt(7)) {
            case 0:
                return new IncludeExclude("incl*de", null, null, null);
            case 1:
                return new IncludeExclude("incl*de", "excl*de", null, null);
            case 2:
                return new IncludeExclude("incl*de", null, null, new TreeSet<>(Set.of(newBytesRef("exclude"))));
            case 3:
                return new IncludeExclude(null, "excl*de", null, null);
            case 4:
                return new IncludeExclude(null, "excl*de", new TreeSet<>(Set.of(newBytesRef("include"))), null);
            case 5:
                return new IncludeExclude(null, null, new TreeSet<>(Set.of(newBytesRef("include"))), null);
            case 6:
                return new IncludeExclude(
                    null,
                    null,
                    new TreeSet<>(Set.of(newBytesRef("include"))),
                    new TreeSet<>(Set.of(newBytesRef("exclude")))
                );
            default:
                return new IncludeExclude(null, null, null, new TreeSet<>(Set.of(newBytesRef("exclude"))));
        }
    }
}
