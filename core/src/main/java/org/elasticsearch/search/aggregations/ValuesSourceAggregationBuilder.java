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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.io.IOException;
import java.util.Map;

/**
 * A base class for all bucket aggregation builders that are based on values (either script generated or field data values)
 */
public abstract class ValuesSourceAggregationBuilder<B extends ValuesSourceAggregationBuilder<B>> extends AggregationBuilder<B> {

    private Script script;
    private String field;
    @Deprecated
    private String scriptString;
    @Deprecated
    private String lang;
    @Deprecated
    private Map<String, Object> params;
    private Object missing;

    /**
     * Constructs a new builder.
     *
     * @param name  The name of the aggregation.
     * @param type  The type of the aggregation.
     */
    protected ValuesSourceAggregationBuilder(String name, String type) {
        super(name, type);
    }

    /**
     * Sets the field from which the values will be extracted.
     *
     * @param field     The name of the field
     * @return          This builder (fluent interface support)
     */
    @SuppressWarnings("unchecked")
    public B field(String field) {
        this.field = field;
        return (B) this;
    }

    /**
     * Sets the script which generates the values. If the script is configured along with the field (as in {@link #field(String)}), then
     * this script will be treated as a {@code value script}. A <i>value script</i> will be applied on the values that are extracted from
     * the field data (you can refer to that value in the script using the {@code _value} reserved variable). If only the script is configured
     * (and the no field is configured next to it), then the script will be responsible to generate the values that will be aggregated.
     *
     * @param script    The configured script.
     * @return          This builder (fluent interface support)
     */
    @SuppressWarnings("unchecked")
    public B script(Script script) {
        this.script = script;
        return (B) this;
    }

    /**
     * Configure the value to use when documents miss a value.
     */
    public B missing(Object missingValue) {
        this.missing = missingValue;
        return (B) this;
    }

    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }

        if (script == null) {
            if (scriptString != null) {
                builder.field("script", new Script(scriptString, ScriptType.INLINE, lang, params));
            }
        } else {
            builder.field("script", script);
        }
        if (missing != null) {
            builder.field("missing", missing);
        }

        doInternalXContent(builder, builderParams);
        return builder.endObject();
    }

    protected abstract XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException;
}
