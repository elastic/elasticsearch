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
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.QueryParsingException;
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
    private SampleSettings samplingSettings = null;
    static final ParseField BACKGROUND_FILTER = new ParseField("background_filter");
    static final ParseField SAMPLING_SETTINGS = new ParseField("sampling");
    static final ParseField DOCS_PER_SHARD = new ParseField("docs_per_shard");
    static final ParseField DUPLICATE_PARA_WORD_LENGTH = new ParseField("duplicate_para_length");
    static final ParseField MAX_TOKENS_PARSED_PER_DOC = new ParseField("max_tokens_parsed_per_doc");
    static final ParseField NUM_RECORDED_TOKENS = new ParseField("num_recorded_tokens");
    
    

    private SignificanceHeuristic significanceHeuristic;

    public TermsAggregator.BucketCountThresholds getDefaultBucketCountThresholds() {
        return new TermsAggregator.BucketCountThresholds(DEFAULT_BUCKET_COUNT_THRESHOLDS);
    }

    public SampleSettings getSamplingSettings() {
        return samplingSettings;
    }

    @Override
    public void parseSpecial(String aggregationName, XContentParser parser, SearchContext context, XContentParser.Token token, String currentFieldName) throws IOException {
        
        if (token == XContentParser.Token.START_OBJECT) {
            SignificanceHeuristicParser significanceHeuristicParser = significanceHeuristicParserMapper.get(currentFieldName);
            if (significanceHeuristicParser != null) {
                significanceHeuristic = significanceHeuristicParser.parse(parser);
            } else if (BACKGROUND_FILTER.match(currentFieldName)) {
                filter = context.queryParserService().parseInnerFilter(parser).filter();
            } else if (SAMPLING_SETTINGS.match(currentFieldName)) {
                samplingSettings = parseSampleSettings(aggregationName, context, parser);
            } else{
                throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
            }
        } else {
            throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
        }
    }
    public SignificanceHeuristic getSignificanceHeuristic() {
        return significanceHeuristic;
    }
    
    public SampleSettings parseSampleSettings(String aggregationName, SearchContext context, XContentParser parser) throws IOException, QueryParsingException {
        SampleSettings result=new SampleSettings();
        Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (DOCS_PER_SHARD.match(currentFieldName)) {
                    result.setNumDocsPerShard(parser.intValue());
                } else if (DUPLICATE_PARA_WORD_LENGTH.match(currentFieldName))  {
                    result.setDuplicateParagraphLengthInWords(parser.intValue());
                } else if (MAX_TOKENS_PARSED_PER_DOC.match(currentFieldName))  {
                    result.setMaxTokensParsedPerDocument(parser.intValue());
                } else if (NUM_RECORDED_TOKENS.match(currentFieldName))  {
                    result.numRecordedTokens = parser.intValue();
                } else {
                    throw new SearchParseException(context, "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            }
        }        
        return result;
    }    
}
