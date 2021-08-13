/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;

import static org.elasticsearch.test.InternalAggregationTestCase.randomNumericDocValueFormat;
import static org.hamcrest.Matchers.equalTo;

public class MultiValuesSourceFieldConfigTests extends AbstractSerializingTestCase<MultiValuesSourceFieldConfig> {

    @Override
    protected MultiValuesSourceFieldConfig doParseInstance(XContentParser parser) throws IOException {
        return MultiValuesSourceFieldConfig.parserBuilder(true, true, true, true).apply(parser, null).build();
    }

    @Override
    protected MultiValuesSourceFieldConfig createTestInstance() {
        String field = randomAlphaOfLength(10);
        Object missing = randomBoolean() ? randomAlphaOfLength(10) : null;
        ZoneId timeZone = randomBoolean() ? randomZone() : null;
        QueryBuilder filter = randomBoolean() ? QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)) : null;
        String format = randomBoolean() ? randomNumericDocValueFormat().toString() : null;
        ValueType userValueTypeHint = randomBoolean()
            ? randomFrom(ValueType.STRING, ValueType.DOUBLE, ValueType.LONG, ValueType.DATE, ValueType.IP, ValueType.BOOLEAN)
            : null;
        return new MultiValuesSourceFieldConfig.Builder().setFieldName(field)
            .setMissing(missing)
            .setScript(null)
            .setTimeZone(timeZone)
            .setFilter(filter)
            .setFormat(format)
            .setUserValueTypeHint(userValueTypeHint)
            .build();
    }

    @Override
    protected Writeable.Reader<MultiValuesSourceFieldConfig> instanceReader() {
        return MultiValuesSourceFieldConfig::new;
    }

    public void testMissingFieldScript() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MultiValuesSourceFieldConfig.Builder().build());
        assertThat(e.getMessage(), equalTo("[field] and [script] cannot both be null.  Please specify one or the other."));
    }

    public void testBothFieldScript() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> new MultiValuesSourceFieldConfig.Builder().setFieldName("foo").setScript(new Script("foo")).build());
        assertThat(e.getMessage(), equalTo("[field] and [script] cannot both be configured.  Please specify one or the other."));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList())
            .getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList())
            .getNamedXContents());
    }
}
