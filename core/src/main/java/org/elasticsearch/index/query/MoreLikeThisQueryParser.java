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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for the The More Like This Query (MLT Query) which finds documents that are "like" a given set of documents.
 *
 * The documents are provided as a set of strings and/or a list of {@link Item}.
 */
public class MoreLikeThisQueryParser implements QueryParser<MoreLikeThisQueryBuilder> {

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

    @Override
    public String[] names() {
        return new String[]{MoreLikeThisQueryBuilder.NAME, "more_like_this", "moreLikeThis"};
    }

    @Override
    public MoreLikeThisQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        // document inputs
        List<String> fields = null;
        List<String> likeTexts = new ArrayList<>();
        List<String> unlikeTexts = new ArrayList<>();
        List<Item> likeItems = new ArrayList<>();
        List<Item> unlikeItems = new ArrayList<>();

        // term selection parameters
        int maxQueryTerms = MoreLikeThisQueryBuilder.DEFAULT_MAX_QUERY_TERMS;
        int minTermFreq = MoreLikeThisQueryBuilder.DEFAULT_MIN_TERM_FREQ;
        int minDocFreq = MoreLikeThisQueryBuilder.DEFAULT_MIN_DOC_FREQ;
        int maxDocFreq = MoreLikeThisQueryBuilder.DEFAULT_MAX_DOC_FREQ;
        int minWordLength = MoreLikeThisQueryBuilder.DEFAULT_MIN_WORD_LENGTH;
        int maxWordLength = MoreLikeThisQueryBuilder.DEFAULT_MAX_WORD_LENGTH;
        List<String> stopWords = null;
        String analyzer = null;

        // query formation parameters
        String minimumShouldMatch = MoreLikeThisQueryBuilder.DEFAULT_MINIMUM_SHOULD_MATCH;
        float boostTerms = MoreLikeThisQueryBuilder.DEFAULT_BOOST_TERMS;
        boolean include = MoreLikeThisQueryBuilder.DEFAULT_INCLUDE;

        // other parameters
        boolean failOnUnsupportedField = MoreLikeThisQueryBuilder.DEFAULT_FAIL_ON_UNSUPPORTED_FIELDS;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
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
                    maxQueryTerms = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MIN_TERM_FREQ)) {
                    minTermFreq =parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MIN_DOC_FREQ)) {
                    minDocFreq = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MAX_DOC_FREQ)) {
                    maxDocFreq = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MIN_WORD_LENGTH)) {
                    minWordLength = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MAX_WORD_LENGTH)) {
                    maxWordLength = parser.intValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.ANALYZER)) {
                    analyzer = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.MINIMUM_SHOULD_MATCH)) {
                    minimumShouldMatch = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.BOOST_TERMS)) {
                    boostTerms = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.INCLUDE)) {
                    include = parser.booleanValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.FAIL_ON_UNSUPPORTED_FIELD)) {
                    failOnUnsupportedField = parser.booleanValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, Field.FIELDS)) {
                    fields = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        fields.add(parser.text());
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
                    stopWords = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        stopWords.add(parser.text());
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, Field.LIKE)) {
                    parseLikeField(parseContext, likeTexts, likeItems);
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, Field.UNLIKE)) {
                    parseLikeField(parseContext, unlikeTexts, unlikeItems);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (likeTexts.isEmpty() && likeItems.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "more_like_this requires 'like' to be specified");
        }
        if (fields != null && fields.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "more_like_this requires 'fields' to be non-empty");
        }

        String[] fieldsArray = fields == null ? null : fields.toArray(new String[fields.size()]);
        String[] likeTextsArray = likeTexts.isEmpty() ? null : likeTexts.toArray(new String[likeTexts.size()]);
        String[] unlikeTextsArray = unlikeTexts.isEmpty() ? null : unlikeTexts.toArray(new String[unlikeTexts.size()]);
        Item[] likeItemsArray = likeItems.isEmpty() ? null : likeItems.toArray(new Item[likeItems.size()]);
        Item[] unlikeItemsArray = unlikeItems.isEmpty() ? null : unlikeItems.toArray(new Item[unlikeItems.size()]);

        MoreLikeThisQueryBuilder moreLikeThisQueryBuilder = new MoreLikeThisQueryBuilder(fieldsArray, likeTextsArray, likeItemsArray)
                .unlike(unlikeTextsArray)
                .unlike(unlikeItemsArray)
                .maxQueryTerms(maxQueryTerms)
                .minTermFreq(minTermFreq)
                .minDocFreq(minDocFreq)
                .maxDocFreq(maxDocFreq)
                .minWordLength(minWordLength)
                .maxWordLength(maxWordLength)
                .analyzer(analyzer)
                .minimumShouldMatch(minimumShouldMatch)
                .boostTerms(boostTerms)
                .include(include)
                .failOnUnsupportedField(failOnUnsupportedField)
                .boost(boost)
                .queryName(queryName);
        if (stopWords != null) {
            moreLikeThisQueryBuilder.stopWords(stopWords);
        }
        return moreLikeThisQueryBuilder;
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

    @Override
    public MoreLikeThisQueryBuilder getBuilderPrototype() {
        return MoreLikeThisQueryBuilder.PROTOTYPE;
    }
}
