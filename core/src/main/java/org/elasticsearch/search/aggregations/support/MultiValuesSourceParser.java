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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class MultiValuesSourceParser<VS extends ValuesSource> extends ValuesSourceParser<VS> {
    private List<String> fields = null;
    private HashMap<String, Script> scripts = null;

    public abstract static class AnyValuesSourceParser extends MultiValuesSourceParser<ValuesSource> {

        protected AnyValuesSourceParser(boolean scriptable, boolean formattable) {
            super(scriptable, formattable, false, ValuesSourceType.ANY, null);
        }
    }

    public abstract static class NumericValuesSourceParser extends MultiValuesSourceParser<ValuesSource.Numeric> {

        protected NumericValuesSourceParser(boolean scriptable, boolean formattable, boolean timezoneAware) {
            super(scriptable, formattable, timezoneAware, ValuesSourceType.NUMERIC, ValueType.NUMERIC);
        }
    }

    public abstract static class BytesValuesSourceParser extends MultiValuesSourceParser<ValuesSource.Bytes> {

        protected BytesValuesSourceParser(boolean scriptable, boolean formattable) {
            super(scriptable, formattable, false, ValuesSourceType.BYTES, ValueType.STRING);
        }
    }

    public abstract static class GeoPointValuesSourceParser extends MultiValuesSourceParser<ValuesSource.GeoPoint> {

        protected GeoPointValuesSourceParser(boolean scriptable, boolean formattable) {
            super(scriptable, formattable, false, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
        }
    }

    private MultiValuesSourceParser(boolean scriptable, boolean formattable, boolean timezoneAware,
                                    ValuesSourceType valuesSourceType, ValueType targetValueType) {
        super(scriptable, formattable, timezoneAware, valuesSourceType, targetValueType);
    }


    @Override
    protected void parseField(XContentParser parser) throws IOException {
        fields = Collections.singletonList(parser.text());
    }

    @Override
    protected void parseScript(final String aggregationName, final String currentFieldName,
                               XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        if (scripts == null) {
            scripts = new HashMap<>();
        } else {
            scripts.clear();
        }
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parseScriptAndAdd(aggregationName, currentFieldName, parser, parseFieldMatcher);
        }
    }

    /**
     * Parses a key:value script in the form of: {script_name} : {script} and adds to the provided script map
     */
    private final void parseScriptAndAdd(final String aggregationName, final String currentFieldName,
                                         XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        XContentParser.Token token = parser.currentToken();
        // If the parser hasn't yet been pushed to the first token, do it now
        if (token == null) {
            token = parser.nextToken();
        }

        // each script must have a name
        if (token == XContentParser.Token.FIELD_NAME) {
            final String name = parser.currentName();
            if (scripts.containsKey(name)) {
                throw new ParsingException(parser.getTokenLocation(),
                    "Script name " + name + " already defined [" + currentFieldName + "] in [" + aggregationName + "].");
            }
            //advance parser
            parser.nextToken();
            // add script and name
            scripts.put(name, Script.parse(parser, parseFieldMatcher));
        } else {
            throw new ParsingException(parser.getTokenLocation(),
                "Unexpected token " + token + " [" + currentFieldName + "] in [" + aggregationName + "].");
        }
    }

    /**
     * Creates a {@link ValuesSourceAggregatorBuilder} from the information
     * gathered by the subclass. Options parsed in
     * {@link MultiValuesSourceParser} itself will be added to the factory
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
    protected abstract MultiValuesSourceAggregatorBuilder<VS, ?> createFactory(String aggregationName, ValuesSourceType valuesSourceType,
            ValueType targetValueType, Map<ParseField, Object> otherOptions);

    @Override
    protected MultiValuesSourceAggregatorBuilder<VS, ?> createBuilder(String aggregationName, final ValueType parsedValueType,
            final String parsedFormat, final Object parsedMissing,
            final DateTimeZone parsedTimeZone, Map<ParseField, Object> otherOptions) {
        MultiValuesSourceAggregatorBuilder<VS, ?> factory = createFactory(aggregationName, this.valuesSourceType, this.targetValueType,
            otherOptions);
        if (fields != null) {
            factory.fields(fields);
        }
        if (scripts != null) {
            factory.scripts(scripts);
        }
        return factory;
    }

    /**
     * Parse field and scripts as arrays
     */
    @Override
    protected boolean internalToken(String aggregationName, String currentFieldName, XContentParser.Token token, XContentParser parser,
                                    ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {
        if (scriptable && token == XContentParser.Token.START_ARRAY) {
            if (parseFieldMatcher.match(currentFieldName, ScriptField.SCRIPT)) {
                if (scripts == null) {
                    scripts = new HashMap<>();
                } else {
                    scripts.clear();
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    // since each script must have a name, this must be an array of objects
                    if (token == XContentParser.Token.START_OBJECT) {
                        parser.nextToken();
                        parseScriptAndAdd(aggregationName, currentFieldName, parser, parseFieldMatcher);
                    } else {
                        return false;
                    }
                }
            } else if (parseFieldMatcher.match(currentFieldName, ParseField.FIELD_FIELD)) {
                fields = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        fields.add(parser.text());
                    } else {
                        return false;
                    }
                }
            } else if (!token(aggregationName, currentFieldName, token, parser, parseFieldMatcher, otherOptions)) {
                return false;
            }
        }
        return true;
    }
}
