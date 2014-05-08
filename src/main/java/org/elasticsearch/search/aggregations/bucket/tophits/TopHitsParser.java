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
package org.elasticsearch.search.aggregations.bucket.tophits;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.source.FetchSourceParseElement;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import java.io.IOException;

/**
 *
 */
public class TopHitsParser implements Aggregator.Parser {

    private final FetchPhase fetchPhase;
    private final SortParseElement sortParseElement;
    private final FetchSourceParseElement sourceParseElement;

    @Inject
    public TopHitsParser(FetchPhase fetchPhase, SortParseElement sortParseElement, FetchSourceParseElement sourceParseElement) {
        this.fetchPhase = fetchPhase;
        this.sortParseElement = sortParseElement;
        this.sourceParseElement = sourceParseElement;
    }

    @Override
    public String type() {
        return InternalTopHits.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {
        TopHitsContext topHitsContext = new TopHitsContext(context);
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("size".equals(currentFieldName)) {
                    topHitsContext.size(parser.intValue());
                } else if ("sort".equals(currentFieldName)) {
                    parseSort(parser, topHitsContext);
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("sort".equals(currentFieldName)) {
                    parseSort(parser, topHitsContext);
                } else if ("_source".equals(currentFieldName)) {
                    try {
                        sourceParseElement.parse(parser, topHitsContext);
                    } catch (Exception e) {
                        throw ExceptionsHelper.convertToElastic(e);
                    }
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("sort".equals(currentFieldName)) {
                    parseSort(parser, topHitsContext);
                }
            } else {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }
        return new TopHitsAggregator.Factory(aggregationName, fetchPhase, topHitsContext);
    }

    private void parseSort(XContentParser parser, SearchContext context) {
        try {
            sortParseElement.parse(parser, context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

}
