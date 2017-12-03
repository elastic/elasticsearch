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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.ParsedAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A base class for all the single bucket aggregations.
 */
public abstract class ParsedSingleBucketAggregation extends ParsedAggregation implements SingleBucketAggregation {

    private long docCount;
    protected Aggregations aggregations = new Aggregations(Collections.emptyList());

    @Override
    public long getDocCount() {
        return docCount;
    }

    protected void setDocCount(long docCount) {
        this.docCount = docCount;
    }

    @Override
    public Aggregations getAggregations() {
        return aggregations;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
        aggregations.toXContentInternal(builder, params);
        return builder;
    }

    protected static <T extends ParsedSingleBucketAggregation> T parseXContent(final XContentParser parser, T aggregation, String name)
            throws IOException {
        aggregation.setName(name);
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = parser.currentName();
        if (token == XContentParser.Token.FIELD_NAME) {
            token = parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);

        List<Aggregation> aggregations = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                    aggregation.setDocCount(parser.longValue());
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (CommonFields.META.getPreferredName().equals(currentFieldName)) {
                    aggregation.metadata = parser.map();
                } else {
                    XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Aggregation.class,
                            aggregations::add);
                }
            }
        }
        aggregation.aggregations = new Aggregations(aggregations);
        return aggregation;
    }
}
