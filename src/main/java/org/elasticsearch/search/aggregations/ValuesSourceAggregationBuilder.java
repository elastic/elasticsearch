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

import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * A base class for all bucket aggregation builders that are based on values (either script generated or field data values)
 */
public abstract class ValuesSourceAggregationBuilder<B extends ValuesSourceAggregationBuilder<B>> extends AggregationBuilder<B> {

    private String field;
    private String script;
    private String lang;
    private Map<String, Object> params;
    private Object trackMissing;
    private Object trackValueCount;

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
    public B script(String script) {
        this.script = script;
        return (B) this;
    }

    /**
     * Sets the language of the script (if one is defined).
     * <p/>
     * Also see {@link #script(String)}.
     *
     * @param lang    The language of the script.
     * @return        This builder (fluent interface support)
     */
    @SuppressWarnings("unchecked")
    public B lang(String lang) {
        this.lang = lang;
        return (B) this;
    }

    /**
     * Sets the value of a parameter that is used in the script (if one is configured).
     *
     * @param name      The name of the parameter.
     * @param value     The value of the parameter.
     * @return          This builder (fluent interface support)
     */
    @SuppressWarnings("unchecked")
    public B param(String name, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(name, value);
        return (B) this;
    }

    /**
     * Enables syntactic sugar for {@code missing} aggregation. This will cause a {@code missing} aggregation to be added as a sibling
     * to this aggregation, and the {@code missing} aggregation will be applied on the same values source as this aggregation is
     * configured to work with. The name of the {@code missing} aggregation will be {@code $name$_missing} where {@code $name$}
     * is the name of this aggregation.
     *
     * @param trackMissing  Indicates whether to track the documents that have no values for this aggregation
     * @return              This builder (fluent interface support)
     */
    public B trackMissing(boolean trackMissing) {
        this.trackMissing = trackMissing;
        return (B) this;
    }

    /**
     * Same as {@link #trackMissing(boolean)} except this enables you to customize the name of the {@code missing} aggregation
     * that will be added as a sibling to this one.
     *
     * @param trackMissing  The name of the {@code missing} aggregation
     * @return              This builder (fluent interface support)
     */
    public B trackMissing(String trackMissing) {
        this.trackMissing = trackMissing;
        return (B) this;
    }

    /**
     * Enables syntactic sugar for {@code value_count} aggregation. This will cause a {@code value_count} aggregation to be added as a sibling
     * to this aggregation, and the {@code value_count} aggregation will be applied on the same values source as this aggregation is
     * configured to work with. The name of the {@code value_count} aggregation will be {@code $name$_value_count} where {@code $name$}
     * is the name of this aggregation.
     *
     * @param trackValueCount   Indicates whether to track the number of values extracted from the documents
     * @return                  This builder (fluent interface support)
     */
    public B trackValueCount(boolean trackValueCount) {
        this.trackValueCount = trackValueCount;
        return (B) this;
    }

    /**
     * Same as {@link #trackValueCount(boolean)} except this enables you to customize the name of the {@code value_count} aggregation
     * that will be added as a sibling to this one.
     *
     * @param trackValueCount   The name of the {@code value_count} aggregation
     * @return                  This builder (fluent interface support)
     */
    public B trackValueCount(String trackValueCount) {
        this.trackValueCount = trackValueCount;
        return (B) this;
    }

    /**
     * Sets the values of a parameters that are used in the script (if one is configured).
     *
     * @param params    The the parameters.
     * @return          This builder (fluent interface support)
     */
    @SuppressWarnings("unchecked")
    public B params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = Maps.newHashMap();
        }
        this.params.putAll(params);
        return (B) this;
    }

    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        if (script != null) {
            builder.field("script", script);
        }
        if (lang != null) {
            builder.field("lang", lang);
        }
        if (this.params != null) {
            builder.field("params").map(this.params);
        }
        if (this.trackMissing != null) {
            builder.field("track_missing", trackMissing);
        }
        if (this.trackValueCount != null) {
            builder.field("track_value_count", trackValueCount);
        }

        doInternalXContent(builder, params);
        return builder.endObject();
    }

    protected abstract XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException;
}
