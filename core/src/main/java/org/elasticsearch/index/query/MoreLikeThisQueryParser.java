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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.index.search.morelikethis.MoreLikeThisFetchService;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.mapper.Uid.createUidAsBytes;

/**
 * Parser for the The More Like This Query (MLT Query) which finds documents that are "like" a given set of documents.
 *
 * The documents are provided as a set of strings and/or a list of {@link Item}.
 */
public class MoreLikeThisQueryParser implements QueryParser {

    public static final String NAME = "mlt";
    private MoreLikeThisFetchService fetchService = null;

    public interface Field {
        ParseField FIELDS = new ParseField("fields");
        ParseField LIKE = new ParseField("like");
        ParseField UNLIKE = new ParseField("unlike");
        ParseField LIKE_TEXT = new ParseField("like_text").withAllDeprecated("like");
        ParseField IDS = new ParseField("ids").withAllDeprecated("like");
        ParseField DOCS = new ParseField("docs").withAllDeprecated("like");
        ParseField MAX_QUERY_TERMS = new ParseField("max_query_terms");
        ParseField MIN_TERM_FREQ = new ParseField("min_term_freq");
        ParseField MIN_DOC_FREQ = new ParseField("min_doc_freq");
        ParseField MAX_DOC_FREQ = new ParseField("max_doc_freq");
        ParseField MIN_WORD_LENGTH = new ParseField("min_word_length", "min_word_len");
        ParseField MAX_WORD_LENGTH = new ParseField("max_word_length", "max_word_len");
        ParseField STOP_WORDS = new ParseField("stop_words");
        ParseField ANALYZER = new ParseField("analyzer");
        ParseField MINIMUM_SHOULD_MATCH = new ParseField("minimum_should_match");
        ParseField BOOST_TERMS = new ParseField("boost_terms");
        ParseField INCLUDE = new ParseField("include");
        ParseField FAIL_ON_UNSUPPORTED_FIELD = new ParseField("fail_on_unsupported_field");
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

        List<String> likeTexts = new ArrayList<>();
        List<String> unlikeTexts = new ArrayList<>();
        List<Item> likeItems = new ArrayList<>();
        List<Item> unlikeItems = new ArrayList<>();

        List<String> moreLikeFields = null;
        Analyzer analyzer = null;
        boolean include = false;

        boolean failOnUnsupportedField = true;
        String queryName = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, Field.LIKE)) {
                    parseLikeField(parseContext, likeTexts, likeItems);
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.UNLIKE)) {
                    parseLikeField(parseContext, unlikeTexts, unlikeItems);
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.LIKE_TEXT)) {
                    likeTexts.add(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MAX_QUERY_TERMS)) {
                    mltQuery.setMaxQueryTerms(parser.intValue());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MIN_TERM_FREQ)) {
                    mltQuery.setMinTermFrequency(parser.intValue());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MIN_DOC_FREQ)) {
                    mltQuery.setMinDocFreq(parser.intValue());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MAX_DOC_FREQ)) {
                    mltQuery.setMaxDocFreq(parser.intValue());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MIN_WORD_LENGTH)) {
                    mltQuery.setMinWordLen(parser.intValue());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MAX_WORD_LENGTH)) {
                    mltQuery.setMaxWordLen(parser.intValue());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.ANALYZER)) {
                    analyzer = parseContext.analysisService().analyzer(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MINIMUM_SHOULD_MATCH)) {
                    mltQuery.setMinimumShouldMatch(parser.text());
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.BOOST_TERMS)) {
                    float boostFactor = parser.floatValue();
                    if (boostFactor != 0) {
                        mltQuery.setBoostTerms(true);
                        mltQuery.setBoostTermsFactor(boostFactor);
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.INCLUDE)) {
                    include = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.FAIL_ON_UNSUPPORTED_FIELD)) {
                    failOnUnsupportedField = parser.booleanValue();
                } else if ("boost".equals(currentFieldName)) {
                    mltQuery.setBoost(parser.floatValue());
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext, "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, Field.FIELDS)) {
                    moreLikeFields = new LinkedList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String field = parser.text();
                        MappedFieldType fieldType = parseContext.fieldMapper(field);
                        moreLikeFields.add(fieldType == null ? field : fieldType.names().indexName());
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.LIKE)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseLikeField(parseContext, likeTexts, likeItems);
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.UNLIKE)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseLikeField(parseContext, unlikeTexts, unlikeItems);
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.IDS)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (!token.isValue()) {
                            throw new IllegalArgumentException("ids array element should only contain ids");
                        }
                        likeItems.add(new Item(null, null, parser.text()));
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.DOCS)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token != XContentParser.Token.START_OBJECT) {
                            throw new IllegalArgumentException("docs array element should include an object");
                        }
                        likeItems.add(Item.parse(parser, parseContext.parseFieldMatcher(), new Item()));
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.STOP_WORDS)) {
                    Set<String> stopWords = new HashSet<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        stopWords.add(parser.text());
                    }
                    mltQuery.setStopWords(stopWords);
                } else {
                    throw new QueryParsingException(parseContext, "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, Field.LIKE)) {
                    parseLikeField(parseContext, likeTexts, likeItems);
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.UNLIKE)) {
                    parseLikeField(parseContext, unlikeTexts, unlikeItems);
                } else {
                    throw new QueryParsingException(parseContext, "[mlt] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (likeTexts.isEmpty() && likeItems.isEmpty()) {
            throw new QueryParsingException(parseContext, "more_like_this requires 'like' to be specified");
        }
        if (moreLikeFields != null && moreLikeFields.isEmpty()) {
            throw new QueryParsingException(parseContext, "more_like_this requires 'fields' to be non-empty");
        }

        // set analyzer
        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }
        mltQuery.setAnalyzer(analyzer);

        // set like text fields
        boolean useDefaultField = (moreLikeFields == null);
        if (useDefaultField) {
            moreLikeFields = Collections.singletonList(parseContext.defaultField());
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
        if (!unlikeTexts.isEmpty()) {
            mltQuery.setUnlikeText(unlikeTexts);
        }

        // handle items
        if (!likeItems.isEmpty()) {
            return handleItems(parseContext, mltQuery, likeItems, unlikeItems, include, moreLikeFields, useDefaultField);
        } else {
            return mltQuery;
        }
    }

    private static void parseLikeField(QueryParseContext parseContext, List<String> texts, List<Item> items) throws IOException {
        XContentParser parser = parseContext.parser();
        if (parser.currentToken().isValue()) {
            texts.add(parser.text());
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            items.add(Item.parse(parser, parseContext.parseFieldMatcher(), new Item()));
        } else {
            throw new IllegalArgumentException("Content of 'like' parameter should either be a string or an object");
        }
    }

    private static List<String> removeUnsupportedFields(List<String> moreLikeFields, Analyzer analyzer, boolean failOnUnsupportedField) throws IOException {
        for (Iterator<String> it = moreLikeFields.iterator(); it.hasNext(); ) {
            final String fieldName = it.next();
            if (!Analysis.generatesCharacterTokenStream(analyzer, fieldName)) {
                if (failOnUnsupportedField) {
                    throw new IllegalArgumentException("more_like_this doesn't support binary/numeric fields: [" + fieldName + "]");
                } else {
                    it.remove();
                }
            }
        }
        return moreLikeFields;
    }

    private Query handleItems(QueryParseContext parseContext, MoreLikeThisQuery mltQuery, List<Item> likeItems, List<Item> unlikeItems,
                              boolean include, List<String> moreLikeFields, boolean useDefaultField) throws IOException {
        // set default index, type and fields if not specified
        for (Item item : likeItems) {
            setDefaultIndexTypeFields(parseContext, item, moreLikeFields, useDefaultField);
        }
        for (Item item : unlikeItems) {
            setDefaultIndexTypeFields(parseContext, item, moreLikeFields, useDefaultField);
        }

        // fetching the items with multi-termvectors API
        MultiTermVectorsResponse responses = fetchService.fetchResponse(likeItems, unlikeItems, SearchContext.current());

        // getting the Fields for liked items
        mltQuery.setLikeText(MoreLikeThisFetchService.getFieldsFor(responses, likeItems));

        // getting the Fields for unliked items
        if (!unlikeItems.isEmpty()) {
            org.apache.lucene.index.Fields[] unlikeFields = MoreLikeThisFetchService.getFieldsFor(responses, unlikeItems);
            if (unlikeFields.length > 0) {
                mltQuery.setUnlikeText(unlikeFields);
            }
        }

        BooleanQuery boolQuery = new BooleanQuery();
        boolQuery.add(mltQuery, BooleanClause.Occur.SHOULD);

        // exclude the items from the search
        if (!include) {
            handleExclude(boolQuery, likeItems);
        }
        return boolQuery;
    }

    private static void setDefaultIndexTypeFields(QueryParseContext parseContext, Item item, List<String> moreLikeFields,
                                                  boolean useDefaultField) {
        if (item.index() == null) {
            item.index(parseContext.index().name());
        }
        if (item.type() == null) {
            if (parseContext.queryTypes().size() > 1) {
                throw new QueryParsingException(parseContext,
                            "ambiguous type for item with id: " + item.id() + " and index: " + item.index());
            } else {
                item.type(parseContext.queryTypes().iterator().next());
            }
        }
        // default fields if not present but don't override for artificial docs
        if ((item.fields() == null || item.fields().length == 0) && item.doc() == null) {
            if (useDefaultField) {
                item.fields("*");
            } else {
                item.fields(moreLikeFields.toArray(new String[moreLikeFields.size()]));
            }
        }
    }

    private static void handleExclude(BooleanQuery boolQuery, List<Item> likeItems) {
        // artificial docs get assigned a random id and should be disregarded
        List<BytesRef> uids = new ArrayList<>();
        for (Item item : likeItems) {
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
