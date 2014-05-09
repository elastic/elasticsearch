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


package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;


public class TermsParametersParser extends AbstractTermsParametersParser {

    private static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(1, 0, 10, -1);

    public String getOrderKey() {
        return orderKey;
    }

    public boolean isOrderAsc() {
        return orderAsc;
    }

    String orderKey = "_count";
    boolean orderAsc = false;

    @Override
    public void parseSpecial(String aggregationName, XContentParser parser, SearchContext context, XContentParser.Token token, String currentFieldName) throws IOException {
        if (token == XContentParser.Token.START_OBJECT) {
            if ("order".equals(currentFieldName)) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        orderKey = parser.currentName();
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        String dir = parser.text();
                        if ("asc".equalsIgnoreCase(dir)) {
                            orderAsc = true;
                        } else if ("desc".equalsIgnoreCase(dir)) {
                            orderAsc = false;
                        } else {
                            throw new SearchParseException(context, "Unknown terms order direction [" + dir + "] in terms aggregation [" + aggregationName + "]");
                        }
                    } else {
                        throw new SearchParseException(context, "Unexpected token " + token + " for [order] in [" + aggregationName + "].");
                    }
                }
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
            }
        } else {
            throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
        }
    }

    @Override
    public TermsAggregator.BucketCountThresholds getDefaultBucketCountThresholds() {
        return new TermsAggregator.BucketCountThresholds(DEFAULT_BUCKET_COUNT_THRESHOLDS);
    }
}
