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
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.search.morelikethis.MoreLikeThisFetchService;
import org.elasticsearch.index.search.morelikethis.MoreLikeThisFetchService.LikeText;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class MoreLikeThisQueryParser implements QueryParser {

    public static final String NAME = "mlt";
    private MoreLikeThisFetchService fetchService = null;

    public static class Fields {
        public static final ParseField LIKE_TEXT = new ParseField("like_text");
        public static final ParseField MIN_TERM_FREQ = new ParseField("min_term_freq");
        public static final ParseField MAX_QUERY_TERMS = new ParseField("max_query_terms");
        public static final ParseField MIN_WORD_LENGTH = new ParseField("min_word_length", "min_word_len");
        public static final ParseField MAX_WORD_LENGTH = new ParseField("max_word_length", "max_word_len");
        public static final ParseField MIN_DOC_FREQ = new ParseField("min_doc_freq");
        public static final ParseField MAX_DOC_FREQ = new ParseField("max_doc_freq");
        public static final ParseField BOOST_TERMS = new ParseField("boost_terms");
        public static final ParseField PERCENT_TERMS_TO_MATCH = new ParseField("percent_terms_to_match");
        public static final ParseField FAIL_ON_UNSUPPORTED_FIELD = new ParseField("fail_on_unsupported_field");
        public static final ParseField STOP_WORDS = new ParseField("stop_words");
        public static final ParseField DOCUMENT_IDS = new ParseField("ids");
        public static final ParseField DOCUMENTS = new ParseField("docs");
        public static final ParseField INCLUDE = new ParseField("include");
        public static final ParseField EXCLUDE = new ParseField("exclude");
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
        List<MultiGetRequest.Item> items = new ArrayList<MultiGetRequest.Item>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (Fields.LIKE_TEXT.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setLikeText(parser.text());
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
                } else if (Fields.PERCENT_TERMS_TO_MATCH.match(currentFieldName, parseContext.parseFlags())) {
                    mltQuery.setPercentTermsToMatch(parser.floatValue());
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
                } else if (Fields.EXCLUDE.match(currentFieldName, parseContext.parseFlags())) {
                    include = !parser.booleanValue();
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
                    MultiGetRequest.parseIds(parser, items);
                } else if (Fields.DOCUMENTS.match(currentFieldName, parseContext.parseFlags())) {
                    MultiGetRequest.parseDocuments(parser, items);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[mlt] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (mltQuery.getLikeText() == null && items.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "more_like_this requires at least 'like_text' or 'ids/docs' to be specified");
        }

        if (analyzer == null) {
            analyzer = parseContext.mapperService().searchAnalyzer();
        }
        mltQuery.setAnalyzer(analyzer);

        if (moreLikeFields == null) {
            moreLikeFields = Lists.newArrayList(parseContext.defaultField());
        } else if (moreLikeFields.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "more_like_this requires 'fields' to be non-empty");
        }

        removeUnsupportedFields(moreLikeFields, analyzer, failOnUnsupportedField);
        if (moreLikeFields.isEmpty()) {
            return null;
        }
        mltQuery.setMoreLikeFields(moreLikeFields.toArray(Strings.EMPTY_ARRAY));

        if (queryName != null) {
            parseContext.addNamedQuery(queryName, mltQuery);
        }

        if (!items.isEmpty()) {
            // set default index, type and fields if not specified
            for (MultiGetRequest.Item item : items) {
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
                if (item.fields() == null && item.fetchSourceContext() == null) {
                    item.fields(moreLikeFields.toArray(new String[moreLikeFields.size()]));
                } else {
                    // TODO how about fields content fetched from _source?
                    removeUnsupportedFields(item, analyzer, failOnUnsupportedField);
                }
            }
            // fetching the items with multi-get
            List<LikeText> likeTexts = fetchService.fetch(items);
            // collapse the text onto the same field name
            Collection<LikeText> likeTextsCollapsed = collapseTextOnField(likeTexts);
            // right now we are just building a boolean query
            BooleanQuery boolQuery = new BooleanQuery();
            for (LikeText likeText : likeTextsCollapsed) {
                addMoreLikeThis(boolQuery, mltQuery, likeText);
            }
            // exclude the items from the search
            if (!include) {
                TermsFilter filter = new TermsFilter(UidFieldMapper.NAME, Uid.createUids(items));
                ConstantScoreQuery query = new ConstantScoreQuery(filter);
                boolQuery.add(query, BooleanClause.Occur.MUST_NOT);
            }
            // add the possible mlt query with like_text
            if (mltQuery.getLikeText() != null) {
                boolQuery.add(mltQuery, BooleanClause.Occur.SHOULD);
            }
            return boolQuery;
        }

        return mltQuery;
    }

    private void addMoreLikeThis(BooleanQuery boolQuery, MoreLikeThisQuery mltQuery, LikeText likeText) {
        MoreLikeThisQuery mlt = new MoreLikeThisQuery();
        mlt.setMoreLikeFields(new String[] {likeText.field});
        mlt.setLikeText(likeText.text);
        mlt.setAnalyzer(mltQuery.getAnalyzer());
        mlt.setPercentTermsToMatch(mltQuery.getPercentTermsToMatch());
        mlt.setBoostTerms(mltQuery.isBoostTerms());
        mlt.setBoostTermsFactor(mltQuery.getBoostTermsFactor());
        mlt.setMinDocFreq(mltQuery.getMinDocFreq());
        mlt.setMaxDocFreq(mltQuery.getMaxDocFreq());
        mlt.setMinWordLen(mltQuery.getMinWordLen());
        mlt.setMaxWordLen(mltQuery.getMaxWordLen());
        mlt.setMinTermFrequency(mltQuery.getMinTermFrequency());
        mlt.setMaxQueryTerms(mltQuery.getMaxQueryTerms());
        mlt.setStopWords(mltQuery.getStopWords());
        boolQuery.add(mlt, BooleanClause.Occur.SHOULD);
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

    public static Collection<LikeText> collapseTextOnField (Collection<LikeText> likeTexts) {
        Map<String, LikeText> collapsedTexts = new HashMap<>();
        for (LikeText likeText : likeTexts) {
            String field = likeText.field;
            String[] text = likeText.text;
            if (collapsedTexts.containsKey(field)) {
                text = ObjectArrays.concat(collapsedTexts.get(field).text, text, String.class);
            }
            collapsedTexts.put(field, new LikeText(field, text));
        }
        return collapsedTexts.values();
    }

    private void removeUnsupportedFields(MultiGetRequest.Item item, Analyzer analyzer, boolean failOnUnsupportedField) throws IOException {
        item.fields((String[]) removeUnsupportedFields(Arrays.asList(item.fields()), analyzer, failOnUnsupportedField).toArray());
    }

}