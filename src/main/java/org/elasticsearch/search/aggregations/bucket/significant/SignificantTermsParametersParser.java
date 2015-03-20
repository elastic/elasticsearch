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


package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParser;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicParserMapper;
import org.elasticsearch.search.aggregations.bucket.terms.AbstractTermsParametersParser;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;


public class SignificantTermsParametersParser extends AbstractTermsParametersParser {

    private static final TermsAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new TermsAggregator.BucketCountThresholds(3, 0, 10, -1);
    private final SignificanceHeuristicParserMapper significanceHeuristicParserMapper;

    public SignificantTermsParametersParser(SignificanceHeuristicParserMapper significanceHeuristicParserMapper) {
        this.significanceHeuristicParserMapper = significanceHeuristicParserMapper;
    }

    public Filter getFilter() {
        return filter;
    }

    private Filter filter = null;

    private SignificanceHeuristic significanceHeuristic;

    @Override
    public TermsAggregator.BucketCountThresholds getDefaultBucketCountThresholds() {
        return new TermsAggregator.BucketCountThresholds(DEFAULT_BUCKET_COUNT_THRESHOLDS);
    }

    static final ParseField BACKGROUND_FILTER = new ParseField("background_filter");

    @Override
    public void parseSpecial(String aggregationName, XContentParser parser, SearchContext context, XContentParser.Token token, String currentFieldName) throws IOException {
        
        if (token == XContentParser.Token.START_OBJECT) {
            SignificanceHeuristicParser significanceHeuristicParser = significanceHeuristicParserMapper.get(currentFieldName);
            if (significanceHeuristicParser != null) {
                significanceHeuristic = significanceHeuristicParser.parse(parser);
            } else if (BACKGROUND_FILTER.match(currentFieldName)) {
                filter = context.queryParserService().parseInnerFilter(parser).filter();
            } else {
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
            }
        } else {
            throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
        }
    }

    public SignificanceHeuristic getSignificanceHeuristic() {
        return significanceHeuristic;
    }
}
