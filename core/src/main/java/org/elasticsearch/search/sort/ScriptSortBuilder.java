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

package org.elasticsearch.search.sort;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.ScriptField;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Script sort builder allows to sort based on a custom script expression.
 */
public class ScriptSortBuilder extends SortBuilder<ScriptSortBuilder> implements NamedWriteable<ScriptSortBuilder>,
    SortElementParserTemp<ScriptSortBuilder> {

    private static final String NAME = "_script";
    static final ScriptSortBuilder PROTOTYPE = new ScriptSortBuilder(new Script("_na_"), "_na_");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField SCRIPT_FIELD = new ParseField("script");
    public static final ParseField SORTMODE_FIELD = new ParseField("mode");
    public static final ParseField NESTED_PATH_FIELD = new ParseField("nested_path");
    public static final ParseField NESTED_FILTER_FIELD = new ParseField("nested_filter");
    public static final ParseField PARAMS_FIELD = new ParseField("params");

    private final Script script;

    // TODO make this an enum
    private final String type;

    // TODO make this an enum
    private String sortMode;

    private QueryBuilder<?> nestedFilter;

    private String nestedPath;

    /**
     * Constructs a script sort builder with the given script.
     *
     * @param script
     *            The script to use.
     * @param type
     *            The type of the script, can be either {@link ScriptSortParser#STRING_SORT_TYPE} or
     *            {@link ScriptSortParser#NUMBER_SORT_TYPE}
     */
    public ScriptSortBuilder(Script script, String type) {
        Objects.requireNonNull(script, "script cannot be null");
        Objects.requireNonNull(type, "type cannot be null");
        this.script = script;
        this.type = type;
    }

    ScriptSortBuilder(ScriptSortBuilder original) {
        this.script = original.script;
        this.type = original.type;
        this.order = original.order;
        this.sortMode = original.sortMode;
        this.nestedFilter = original.nestedFilter;
        this.nestedPath = original.nestedPath;
    }

    /**
     * Get the script used in this sort.
     */
    public Script script() {
        return this.script;
    }

    /**
     * Get the type used in this sort.
     */
    public String type() {
        return this.type;
    }

    /**
     * Defines which distance to use for sorting in the case a document contains multiple geo points.
     * Possible values: min and max
     */
    public ScriptSortBuilder sortMode(String sortMode) {
        this.sortMode = sortMode;
        return this;
    }

    /**
     * Get the sort mode.
     */
    public String sortMode() {
        return this.sortMode;
    }

    /**
     * Sets the nested filter that the nested objects should match with in order to be taken into account
     * for sorting.
     */
    public ScriptSortBuilder setNestedFilter(QueryBuilder<?> nestedFilter) {
        this.nestedFilter = nestedFilter;
        return this;
    }

    /**
     * Gets the nested filter.
     */
    public QueryBuilder<?> getNestedFilter() {
        return this.nestedFilter;
    }

    /**
     * Sets the nested path if sorting occurs on a field that is inside a nested object. For sorting by script this
     * needs to be specified.
     */
    public ScriptSortBuilder setNestedPath(String nestedPath) {
        this.nestedPath = nestedPath;
        return this;
    }

    /**
     * Gets the nested path.
     */
    public String getNestedPath() {
        return this.nestedPath;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject(NAME);
        builder.field(SCRIPT_FIELD.getPreferredName(), script);
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        if (sortMode != null) {
            builder.field(SORTMODE_FIELD.getPreferredName(), sortMode);
        }
        if (nestedPath != null) {
            builder.field(NESTED_PATH_FIELD.getPreferredName(), nestedPath);
        }
        if (nestedFilter != null) {
            builder.field(NESTED_FILTER_FIELD.getPreferredName(), nestedFilter, builderParams);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public ScriptSortBuilder fromXContent(QueryParseContext context, String elementName) throws IOException {
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
        XContentParser parser = context.parser();
        ParseFieldMatcher parseField = context.parseFieldMatcher();
        Script script = null;
        String type = null;
        String sortMode = null;
        SortOrder order = null;
        QueryBuilder<?> nestedFilter = null;
        String nestedPath = null;
        Map<String, Object> params = new HashMap<>();

        XContentParser.Token token;
        String currentName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseField.match(currentName, ScriptField.SCRIPT)) {
                    script = Script.parse(parser, parseField);
                } else if (parseField.match(currentName, PARAMS_FIELD)) {
                    params = parser.map();
                } else if (parseField.match(currentName, NESTED_FILTER_FIELD)) {
                    nestedFilter = context.parseInnerQueryBuilder();
                }
            } else if (token.isValue()) {
                if (parseField.match(currentName, ORDER_FIELD)) {
                    order = SortOrder.fromString(parser.text());
                } else if (scriptParameterParser.token(currentName, token, parser, parseField)) {
                    // Do Nothing (handled by ScriptParameterParser
                } else if (parseField.match(currentName, TYPE_FIELD)) {
                    type = parser.text();
                } else if (parseField.match(currentName, SORTMODE_FIELD)) {
                    sortMode = parser.text();
                } else if (parseField.match(currentName, NESTED_PATH_FIELD)) {
                    nestedPath = parser.text();
                }
            }
        }

        if (script == null) { // Didn't find anything using the new API so try using the old one instead
            ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
            if (scriptValue != null) {
                if (params == null) {
                    params = new HashMap<>();
                }
                script = new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), params);
            }
        }

        ScriptSortBuilder result = new ScriptSortBuilder(script, type);
        if (order != null) {
            result.order(order);
        }
        if (sortMode != null) {
            result.sortMode(sortMode);
        }
        if (nestedFilter != null) {
            result.setNestedFilter(nestedFilter);
        }
        if (nestedPath != null) {
            result.setNestedPath(nestedPath);
        }
        return result;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ScriptSortBuilder other = (ScriptSortBuilder) object;
        return Objects.equals(script, other.script) &&
                Objects.equals(type, other.type) &&
                Objects.equals(order, other.order) &&
                Objects.equals(sortMode, other.sortMode) &&
                Objects.equals(nestedFilter, other.nestedFilter) &&
                Objects.equals(nestedPath, other.nestedPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(script, type, order, sortMode, nestedFilter, nestedPath);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        script.writeTo(out);
        out.writeString(type);
        order.writeTo(out);
        out.writeOptionalString(sortMode);
        out.writeOptionalString(nestedPath);
        boolean hasNestedFilter = nestedFilter != null;
        out.writeBoolean(hasNestedFilter);
        if (hasNestedFilter) {
            out.writeQuery(nestedFilter);
        }
    }

    @Override
    public ScriptSortBuilder readFrom(StreamInput in) throws IOException {
        ScriptSortBuilder builder = new ScriptSortBuilder(Script.readScript(in), in.readString());
        builder.order(SortOrder.readOrderFrom(in));
        builder.sortMode = in.readOptionalString();
        builder.nestedPath = in.readOptionalString();
        if (in.readBoolean()) {
            builder.nestedFilter = in.readQuery();
        }
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
