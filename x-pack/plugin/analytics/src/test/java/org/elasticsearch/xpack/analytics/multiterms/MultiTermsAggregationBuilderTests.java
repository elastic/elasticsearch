/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.multiterms;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.InternalAggregationTestCase.randomNumericDocValueFormat;
import static org.elasticsearch.xpack.analytics.multiterms.InternalMultiTermsTests.randomBucketOrder;
import static org.hamcrest.Matchers.hasSize;

public class MultiTermsAggregationBuilderTests extends AbstractSerializingTestCase<MultiTermsAggregationBuilder> {
    String aggregationName;

    @Before
    public void setupName() {
        aggregationName = randomAlphaOfLength(10);
    }

    @Override
    protected MultiTermsAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        assertThat(parsed.getAggregatorFactories(), hasSize(1));
        assertThat(parsed.getPipelineAggregatorFactories(), hasSize(0));
        MultiTermsAggregationBuilder agg = (MultiTermsAggregationBuilder) parsed.getAggregatorFactories().iterator().next();
        assertNull(parser.nextToken());
        assertNotNull(agg);
        return agg;
    }

    static MultiValuesSourceFieldConfig randomTermConfig() {
        String field = randomAlphaOfLength(10);
        Object missing = randomBoolean() ? randomAlphaOfLength(10) : null;
        ZoneId timeZone = randomBoolean() ? randomZone() : null;
        String format = randomBoolean() ? randomNumericDocValueFormat().toString() : null;
        ValueType userValueTypeHint = randomBoolean()
            ? randomFrom(ValueType.STRING, ValueType.DOUBLE, ValueType.LONG, ValueType.DATE, ValueType.IP, ValueType.BOOLEAN)
            : null;
        return new MultiValuesSourceFieldConfig.Builder().setFieldName(field)
            .setMissing(missing)
            .setScript(null)
            .setTimeZone(timeZone)
            .setFormat(format)
            .setUserValueTypeHint(userValueTypeHint)
            .build();
    }

    @Override
    protected MultiTermsAggregationBuilder createTestInstance() {
        MultiTermsAggregationBuilder aggregationBuilder = new MultiTermsAggregationBuilder(aggregationName);

        int termCount = randomIntBetween(2, 10);
        List<MultiValuesSourceFieldConfig> terms = new ArrayList<>();
        for (int i = 0; i < termCount; i++) {
            terms.add(randomTermConfig());
        }
        aggregationBuilder.terms(terms);
        if (randomBoolean()) {
            if (randomBoolean()) {
                aggregationBuilder.showTermDocCountError(randomBoolean());
            }
        }
        if (randomBoolean()) {
            aggregationBuilder.size(randomIntBetween(1, 1000));
        }
        if (randomBoolean()) {
            aggregationBuilder.shardSize(randomIntBetween(1, 1000));
        }
        if (randomBoolean()) {
            aggregationBuilder.order(randomBucketOrder());
        }
        if (randomBoolean()) {
            aggregationBuilder.collectMode(randomFrom(Aggregator.SubAggCollectionMode.values()));
        }
        if (randomBoolean()) {
            aggregationBuilder.collectMode(randomFrom(Aggregator.SubAggCollectionMode.values()));
        }
        return aggregationBuilder;
    }

    @Override
    protected Writeable.Reader<MultiTermsAggregationBuilder> instanceReader() {
        return MultiTermsAggregationBuilder::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                BaseAggregationBuilder.class,
                new ParseField(MultiTermsAggregationBuilder.NAME),
                (p, n) -> MultiTermsAggregationBuilder.PARSER.apply(p, (String) n)
            )
        );
        namedXContent.addAll(new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }
}
