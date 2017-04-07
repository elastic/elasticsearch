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
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * An implementation of {@link Aggregation} that is parsed from a REST response.
 * Serves as a base class for all aggregation implementations that are parsed from REST.
 */
public abstract class ParsedAggregation implements Aggregation, ToXContent {

    protected static void declareCommonFields(ObjectParser<? extends ParsedAggregation, Void> objectParser) {
        objectParser.declareObject((parsedAgg, metadata) -> parsedAgg.metadata = Collections.unmodifiableMap(metadata),
                (parser, context) -> parser.map(), InternalAggregation.CommonFields.META);
    }

    String name;
    Map<String, Object> metadata;

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final Map<String, Object> getMetaData() {
        return metadata;
    }

    /**
     * Returns a string representing the type of the aggregation. This type is added to
     * the aggregation name in the response, so that it can later be used by REST clients
     * to determine the internal type of the aggregation.
     */
    //TODO it may make sense to move getType to the Aggregation interface given that we are duplicating it in both implementations
    protected abstract String getType();

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
}
