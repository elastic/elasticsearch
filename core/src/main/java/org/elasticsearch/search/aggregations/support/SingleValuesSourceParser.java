/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public abstract class SingleValuesSourceParser<VS extends ValuesSource> extends ValuesSourceParser<VS> {
    private String field = null;
    private Script script = null;

    public abstract static class AnyValuesSourceParser extends SingleValuesSourceParser<ValuesSource> {
        protected AnyValuesSourceParser(boolean scriptable, boolean formattable) {
            super(scriptable, formattable, false, ValuesSourceType.ANY, null);
        }
    }

    public abstract static class NumericValuesSourceParser extends SingleValuesSourceParser<ValuesSource.Numeric> {
        protected NumericValuesSourceParser(boolean scriptable, boolean formattable, boolean timezoneAware) {
            super(scriptable, formattable, timezoneAware, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
        }
    }

    public abstract static class BytesValuesSourceParser extends SingleValuesSourceParser<ValuesSource.Bytes> {
        protected BytesValuesSourceParser(boolean scriptable, boolean formattable) {
            super(scriptable, formattable, false, ValuesSourceType.BYTES, ValueType.STRING);
        }
    }

    public abstract static class GeoPointValuesSourceParser extends SingleValuesSourceParser<ValuesSource.GeoPoint> {
        protected GeoPointValuesSourceParser(boolean scriptable, boolean formattable) {
            super(scriptable, formattable, false, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
        }
    }

    private SingleValuesSourceParser(boolean scriptable, boolean formattable, boolean timezoneAware,
            ValuesSourceType valuesSourceType, ValueType targetValueType) {
        super(scriptable, formattable, timezoneAware, valuesSourceType, targetValueType);
    }

    @Override
    protected void parseField(XContentParser parser) throws IOException {
        field = parser.text();
    }

    @Override
    protected void parseScript(final String aggregationName, final String currentFieldName,
            XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        script = Script.parse(parser, parseFieldMatcher);
    }

    /**
     * Creates a {@link SingleValuesSourceAggregatorBuilder} from the information
     * gathered by the subclass. Options parsed in
     * {@link SingleValuesSourceParser} itself will be added to the factory
     * after it has been returned by this method.
     *
     * @param aggregationName
     *            the name of the aggregation
     * @param valuesSourceType
     *            the type of the {@link ValuesSource}
     * @param targetValueType
     *            the target type of the final value output by the aggregation
     * @param otherOptions
     *            a {@link Map} containing the extra options parsed by the
     *            {@link #token(String, String, org.elasticsearch.common.xcontent.XContentParser.Token,
     *             XContentParser, ParseFieldMatcher, Map)}
     *            method
     * @return the created factory
     */
    protected abstract SingleValuesSourceAggregatorBuilder<VS, ?> createFactory(String aggregationName, ValuesSourceType valuesSourceType,
            ValueType targetValueType, Map<ParseField, Object> otherOptions);

    @Override
    protected SingleValuesSourceAggregatorBuilder<VS, ?> createBuilder(String aggregationName, final ValueType parsedValueType,
            final String parsedFormat, final Object parsedMissing, final DateTimeZone parsedTimeZone,
            Map<ParseField, Object> otherOptions) {
        SingleValuesSourceAggregatorBuilder<VS, ?> factory = createFactory(aggregationName, this.valuesSourceType, this.targetValueType,
            otherOptions);
        if (field != null) {
            factory.field(field);
        }
        if (script != null) {
            factory.script(script);
        }
        if (parsedValueType != null) {
            factory.valueType(parsedValueType);
        }
        if (parsedFormat != null) {
            factory.format(parsedFormat);
        }
        if (parsedMissing != null) {
            factory.missing(parsedMissing);
        }
        if (parsedTimeZone != null) {
            factory.timeZone(parsedTimeZone);
        }
        return factory;
    }
}
