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

package org.elasticsearch.index.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.search.morelikethis.MoreLikeThisFetchService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.mapper.Uid.createUidAsBytes;

/**
 *
 */
public class MoreLikeThisQueryParser implements QueryParser {

    public static final String NAME = "mlt";
    private MoreLikeThisFetchService fetchService = null;

    public static class Fields {
        public static final ParseField LIKE_TEXT = new ParseField("like_text").withAllDeprecated("like");
        public static final ParseField MIN_TERM_FREQ = new ParseField("min_term_freq");
        public static final ParseField MAX_QUERY_TERMS = new ParseField("max_query_terms");
        public static final ParseField MIN_WORD_LENGTH = new ParseField("min_word_length", "min_word_len");
        public static final ParseField MAX_WORD_LENGTH = new ParseField("max_word_length", "max_word_len");
        public static final ParseField MIN_DOC_FREQ = new ParseField("min_doc_freq");
        public static final ParseField MAX_DOC_FREQ = new ParseField("max_doc_freq");
        public static final ParseField BOOST_TERMS = new ParseField("boost_terms");
        public static final ParseField MINIMUM_SHOULD_MATCH = new ParseField("minimum_should_match");
        public static final ParseField PERCENT_TERMS_TO_MATCH = new ParseField("percent_terms_to_match").withAllDeprecated("minimum_should_match");
        public static final ParseField FAIL_ON_UNSUPPORTED_FIELD = new ParseField("fail_on_unsupported_field");
        public static final ParseField STOP_WORDS = new ParseField("stop_words");
        public static final ParseField DOCUMENT_IDS = new ParseField("ids").withAllDeprecated("like");
        public static final ParseField DOCUMENTS = new ParseField("docs").withAllDeprecated("like");
        public static final ParseField LIKE = new ParseField("like");
        public static final ParseField IGNORE_LIKE = new ParseField("ignore_like");
        public static final ParseField INCLUDE = new ParseField("include");
    }

    public MoreLikeThisQueryParser() {

    }

    @Inject(optional = true)
    public void setFetchService(@Nullable MoreLikeThisFetchService fetchService) {
        this.fetchService = fetchService;
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "more_like_this", "moreLikeThis"};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        MoreLikeThisQuery mltQuery = new MoreLikeThisQuery();
        mltQuery.setSimilarity(parseContext.searchSimilarity());
        Analyzer analyzer = null;
        List<String> moreLikeFields = null;
        boolean failOnUnsupportedField = true;
        String queryName = null;
        boolean include = false;

        XContentParser.Token token;
        String currentFieldName = null;

        List<String> likeTexts = new ArrayList<>();
        MultiTermVectorsRequest likeItems = new MultiTermVectorsRequest();

        List<String> ignoreTexts = new ArrayList<>();
        MultiTermVectorsRequest ignoreItems = new MultiTermVectorsRequest();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (Fields.LIKE_TEXT.match(currentFieldName, parseContext.parseFlags())) {
                    likeTexts.add(parser.text());
                } else if (Fields.LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, likeTexts, likeItems);
                } else if (Fields.IGNORE_LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, ignoreTexts, ignoreItems);
                } else if (Fields.MIN_TERM_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinTermFrequency(parser.intValue());
                } else if (Fields.MAX_QUERY_TERMS.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMaxQueryTerms(parser.intValue());
                } else if (Fields.MIN_DOC_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinDocFreq(parser.intValue());
                } else if (Fields.MAX_DOC_FREQ.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMaxDocFreq(parser.intValue());
                } else if (Fields.MIN_WORD_LENGTH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinWordLen(parser.intValue());
                } else if (Fields.MAX_WORD_LENGTH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMaxWordLen(parser.intValue());
                } else if (Fields.BOOST_TERMS.match(currentFieldName, parseContext.parseFlags())) {
                    float boostFactor = parser.floatValue();
                    if (boostFactor != 0) {
                        mltQuery.setBoostTerms(true);
                        mltQuery.setBoostTermsFactor(boostFactor);
                    }
                } else if (Fields.MINIMUM_SHOULD_MATCH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinimumShouldMatch(parser.text());
                } else if (Fields.PERCENT_TERMS_TO_MATCH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setMinimumShouldMatch(Math.round(parser.floatValue() * 100) + "%");
                } else if ("analyzer".equals(currentFieldName)) {
                    analyzer = parseContext.analysisService().analyzer(parser.text());
                } else if ("boost".equals(currentFieldName)) {
                    mltQuery.setBoost(parser.floatValue());
                } else if (Fields.FAIL_ON_UNSUPPORTED_FIELD.match(currentFieldName, parseContext.parseFlags())) {
                    failOnUnsupportedField = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if (Fields.INCLUDE.match(currentFieldName, parseContext.parseFlags())) {
                    include = parser.booleanValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (Fields.STOP_WORDS.match(currentFieldName, parseContext.parseFlags())) {
                    Set<String> stopWords = Sets.newHashSet();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        stopWords.add(parser.text());
                    }
                    mltQuery.setStopWords(stopWords);
                } else if ("fields".equals(currentFieldName)) {
                    moreLikeFields = Lists.newLinkedList();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        moreLikeFields.add(parseContext.indexName(parser.text()));
                    }
                } else if (Fields.DOCUMENT_IDS.match(currentFieldName, parseContext.parseFlags())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (!token.isValue()) {
                            throw new ElasticsearchIllegalArgumentException("ids array element should only contain ids");
                        }
                        likeItems.add(newTermVectorsRequest().id(parser.text()));
                    }
                } else if (Fields.DOCUMENTS.match(currentFieldName, parseContext.parseFlags())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new ElasticsearchIllegalArgumentException("docs array element should include an object");
                        }
                        likeItems.add(parseDocument(parser));
                    }
                } else if (Fields.LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseLikeField(parser, likeTexts, likeItems);
                    }
                } else if (Fields.IGNORE_LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseLikeField(parser, ignoreTexts, ignoreItems);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (Fields.LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, likeTexts, likeItems);
                }
                else if (Fields.IGNORE_LIKE.match(currentFieldName, parseContext.parseFlags())) {
                    parseLikeField(parser, ignoreTexts, ignoreItems);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (likeTexts.isEmpty() && likeItems.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "more_like_this requires 'like' to be specified");
        }
        if (moreLikeFields != null && moreLikeFields.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "more_like_this requires 'fields' to be non-empty");
        }

        // set analyzer
        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }
        mltQuery.setAnalyzer(analyzer);

        // set like text fields
        boolean useDefaultField = (moreLikeFields == null);
        if (useDefaultField) {
            moreLikeFields = Lists.newArrayList(parseContext.defaultField());
        }
        // possibly remove unsupported fields
        removeUnsupportedFields(moreLikeFields, analyzer, failOnUnsupportedField);
        if (moreLikeFields.isEmpty()) {
            return null;
        }
        mltQuery.setMoreLikeFields(moreLikeFields.toArray(Strings.EMPTY_ARRAY));

        // support for named query
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, mltQuery);
        }

        // handle like texts
        if (!likeTexts.isEmpty()) {
            mltQuery.setLikeText(likeTexts);
        }
        if (!ignoreTexts.isEmpty()) {
            mltQuery.setIgnoreText(ignoreTexts);
        }

        // handle items
        if (!likeItems.isEmpty()) {
            // set default index, type and fields if not specified
            MultiTermVectorsRequest items = likeItems;
            for (TermVectorsRequest item : ignoreItems) {
                items.add(item);
            }

            for (TermVectorsRequest item : items) {
                if (item.index() == null) {
                    item.index(parseContext.index().name());
                }
                if (item.type() == null) {
                    if (parseContext.queryTypes().size() > 1) {
                        throw new QueryParsingException(parseContext.index(),
                                "ambiguous type for item with id: " + item.id() + " and index: " + item.index());
                    } else {
                        item.type(parseContext.queryTypes().iterator().next());
                    }
                }
                // default fields if not present but don't override for artificial docs
                if (item.selectedFields() == null && item.doc() == null) {
                    if (useDefaultField) {
                        item.selectedFields("*");
                    } else {
                        item.selectedFields(moreLikeFields.toArray(new String[moreLikeFields.size()]));
                    }
                }
            }
            // fetching the items with multi-termvectors API
            BooleanQuery boolQuery = new BooleanQuery();
            MultiTermVectorsResponse responses = fetchService.fetchResponse(items);

            // getting the Fields for liked items
            mltQuery.setLikeText(MoreLikeThisFetchService.getFields(responses, likeItems));

            // getting the Fields for ignored items
            if (!ignoreItems.isEmpty()) {
                org.apache.lucene.index.Fields[] ignoreFields = MoreLikeThisFetchService.getFields(responses, ignoreItems);
                if (ignoreFields.length > 0) {
                    mltQuery.setIgnoreText(ignoreFields);
                }
            }

            boolQuery.add(mltQuery, BooleanClause.Occur.SHOULD);

            // exclude the items from the search
            if (!include) {
                handleExclude(boolQuery, likeItems);
            }
            return boolQuery;
        }

        return mltQuery;
    }

    private TermVectorsRequest parseDocument(XContentParser parser) throws IOException {
        TermVectorsRequest termVectorsRequest = newTermVectorsRequest();
        TermVectorsRequest.parseRequest(termVectorsRequest, parser);
        return termVectorsRequest;
    }

    private void parseLikeField(XContentParser parser, List<String> likeTexts, MultiTermVectorsRequest items) throws IOException {
        if (parser.currentToken().isValue()) {
            likeTexts.add(parser.text());
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            items.add(parseDocument(parser));
        } else {
            throw new ElasticsearchIllegalArgumentException("Content of 'like' parameter should either be a string or an object");
        }
    }

    private TermVectorsRequest newTermVectorsRequest() {
        return new TermVectorsRequest()
                .positions(false)
                .offsets(false)
                .payloads(false)
                .fieldStatistics(false)
                .termStatistics(false);
    }

    private List<String> removeUnsupportedFields(List<String> moreLikeFields, Analyzer analyzer, boolean failOnUnsupportedField) throws IOException {
        for (Iterator<String> it = moreLikeFields.iterator(); it.hasNext(); ) {
            final String fieldName = it.next();
            if (!Analysis.generatesCharacterTokenStream(analyzer, fieldName)) {
                if (failOnUnsupportedField) {
                    throw new ElasticsearchIllegalArgumentException("more_like_this doesn't support binary/numeric fields: [" + fieldName + "]");
                } else {
                    it.remove();
                }
            }
        }
        return moreLikeFields;
    }

    private void handleExclude(BooleanQuery boolQuery, MultiTermVectorsRequest likeItems) {
        // artificial docs get assigned a random id and should be disregarded
        List<BytesRef> uids = new ArrayList<>();
        for (TermVectorsRequest item : likeItems) {
            if (item.doc() != null) {
                continue;
            }
            uids.add(createUidAsBytes(item.type(), item.id()));
        }
        if (!uids.isEmpty()) {
            TermsQuery query = new TermsQuery(UidFieldMapper.NAME, uids.toArray(new BytesRef[0]));
            boolQuery.add(query, BooleanClause.Occur.MUST_NOT);
        }
    }
}