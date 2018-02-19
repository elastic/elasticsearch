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

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * An implementation of {@link Aggregation} that is parsed from a REST response.
 * Serves as a base class for all aggregation implementations that are parsed from REST.
 */
public abstract class ParsedAggregation implements Aggregation, ToXContentFragment {

    protected static void declareAggregationFields(ObjectParser<? extends ParsedAggregation, Void> objectParser) {
        objectParser.declareObject((parsedAgg, metadata) -> parsedAgg.metadata = Collections.unmodifiableMap(metadata),
                (parser, context) -> parser.map(), InternalAggregation.CommonFields.META);
    }

    private String name;
    protected Map<String, Object> metadata;

    @Override
    public final String getName() {
        return name;
    }

    protected void setName(String name) {
        this.name = name;
    }

    @Override
    public final Map<String, Object> getMetaData() {
        return metadata;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // Concatenates the type and the name of the aggregation (ex: top_hits#foo)
        builder.startObject(String.join(InternalAggregation.TYPED_KEYS_DELIMITER, getType(), name));
        if (this.metadata != null) {
            builder.field(InternalAggregation.CommonFields.META.getPreferredName());
            builder.map(this.metadata);
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    /**
     * Parse a token of type XContentParser.Token.VALUE_NUMBER or XContentParser.Token.STRING to a double.
     * In other cases the default value is returned instead.
     */
    protected static double parseDouble(XContentParser parser, double defaultNullValue) throws IOException {
        Token currentToken = parser.currentToken();
        if (currentToken == XContentParser.Token.VALUE_NUMBER || currentToken == XContentParser.Token.VALUE_STRING) {
            return parser.doubleValue();
        } else {
            return defaultNullValue;
        }
    }
}