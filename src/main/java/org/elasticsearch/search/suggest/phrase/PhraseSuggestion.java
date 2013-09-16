/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.suggest.phrase;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.ReduceContext;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;

import java.io.IOException;
import java.util.Iterator;

import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * Suggestion entry returned from the {@link PhraseSuggester}.
 */
public class PhraseSuggestion extends Suggest.Suggestion<PhraseSuggestion.Entry> {
    public static final int TYPE = 3;

    /**
     * Field against which suggestion was generated.
     */
    private Text field;
    /**
     * Type of filter to apply to suggestions. null is invalid.
     */
    private FilterType filterType = FilterType.NONE;
    /**
     * Extra filter to apply to suggestions. a zero length reference means none.
     */
    private BytesReference filterExtra = BytesArray.EMPTY;
    
    public PhraseSuggestion() {
    }

    public PhraseSuggestion(String name, int size) {
        super(name, size);
    }
    
    public void setField(Text field) {
        this.field = field;
    }
    
    public Text getField() {
        return this.field;
    }
    
    public void setFilterType(FilterType filterType) {
        this.filterType = filterType;
    }
    
    public FilterType getFilterType() {
        return filterType;
    }
    
    public void setFilterExtra(BytesReference filterExtra) {
        this.filterExtra = filterExtra;
    }
    
    public BytesReference getFilterExtra() {
        return filterExtra;
    }
    
    @Override
    protected void filter(ReduceContext context) {
        // Don't filter if it wasn't asked for or if the search context doesn't support it.
        if (filterType == FilterType.NONE || context == null) {
            return;
        }
        FilterBuilder filterExtraBuilder = null;
        if (filterExtra.length() > 0) {
            filterExtraBuilder = wrapperFilter(filterExtra);
        }
        SearchRequestBuilder searchRequest = context.getClient().prepareSearch(context.getIndecies());
        searchRequest.setTypes(context.getTypes());
        searchRequest.setRouting(context.getRouting());
        searchRequest.setPreference(context.getPreference());
        searchRequest.setNoFields();
        searchRequest.setFrom(0);
        searchRequest.setSize(1);
        searchRequest.setQuery(matchAllQuery());
        String fieldString = field.string();
        for (Entry entry: this.getEntries()) {
            MultiSearchRequestBuilder multi = context.getClient().prepareMultiSearch();
            for (Option option: entry.getOptions()) {
                MatchQueryBuilder optionQueryBuilder;
                switch (filterType) {
                case MATCH_PHRASE:
                    optionQueryBuilder = matchPhraseQuery(fieldString, option.getText());
                    break;
                case MATCH_AND:
                    optionQueryBuilder = matchQuery(fieldString, option.getText());
                    optionQueryBuilder.operator(Operator.AND);
                    break;
                case MATCH_OR:
                    optionQueryBuilder = matchQuery(fieldString, option.getText());
                    optionQueryBuilder.operator(Operator.OR);
                    break;
                default:
                    throw new ElasticSearchIllegalArgumentException("Unknown filter type:  " + filterType);
                }
                FilterBuilder optionFilterBuilder = queryFilter(optionQueryBuilder);
                if (filterExtraBuilder == null) {
                    searchRequest.setFilter(optionFilterBuilder);
                } else {
                    BoolFilterBuilder searchFilter = boolFilter();
                    searchFilter.must(filterExtraBuilder);
                    searchFilter.must(optionFilterBuilder);
                    searchRequest.setFilter(searchFilter);    
                }
                System.err.println(searchRequest);
                // Force building the searchRequest right now because we reuse the builder for the next option
                multi.add(searchRequest.request());
            }
            MultiSearchResponse multiResponse = multi.get("10s"); // TODO timeout and exception handling
            int responseIndex = 0;
            Iterator<? extends Option> itr = entry.getOptions().iterator();
            while (itr.hasNext()) {
                Option option = itr.next();
                SearchResponse response = multiResponse.getResponses()[responseIndex++].getResponse();
                System.err.println(response);
                if (response.getHits().getTotalHits() < 1) {
                    System.err.println("Removing " + option.getText());
                    itr.remove();
                }
                // TODO stop early when we're sure we have enough results
                // TODO don't just launch all the requests at once?
                // TODO allow the user to limit the maximum number of of results to try to filter
            }
        }
    }

    @Override
    public int getType() {
        return TYPE;
    }

    @Override
    protected Entry newEntry() {
        return new Entry();
    }
    
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        // If the other side is older than 0.90.4 then it shouldn't be sending suggestions of this type but just in case
        // we're going to assume that they are regular suggestions so we won't read anything.
        if (in.getVersion().before(Version.V_0_90_4)) {
            return;
        }
        field = in.readText();
        filterType = FilterType.fromValue(in.readByte());
        filterExtra = in.readBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        // If the other side of the message is older than 0.90.4 it'll interpret these suggestions as regular suggestions
        // so we have to pretend to be one which we can do by just calling the superclass writeTo and doing nothing else
        if (out.getVersion().before(Version.V_0_90_4)) {
            return;
        }
        out.writeText(field);
        out.writeByte(filterType.getValue());
        out.writeBytesReference(filterExtra);
    }

    public static class Entry extends Suggestion.Entry<Suggestion.Entry.Option> {
        static class Fields {
            static final XContentBuilderString CUTOFF_SCORE = new XContentBuilderString("cutoff_score");
        }

        protected double cutoffScore = Double.MIN_VALUE;

        public Entry(Text text, int offset, int length, double cutoffScore) {
            super(text, offset, length);
            this.cutoffScore = cutoffScore;
        }

        public Entry() {
        }

        /**
         * @return cutoff score for suggestions.  input term score * confidence for phrase suggest, 0 otherwise
         */
        public double getCutoffScore() {
            return cutoffScore;
        }

        @Override
        protected void merge(Suggestion.Entry<Suggestion.Entry.Option> other) {
            super.merge(other);
            // If the cluster contains both pre 0.90.4 and post 0.90.4 nodes then we'll see Suggestion.Entry
            // objects being merged with PhraseSuggestion.Entry objects.  We merge Suggestion.Entry objects
            // by assuming they had a low cutoff score rather than a high one as that is the more common scenario
            // and the simplest one for us to implement.
            if (!(other instanceof PhraseSuggestion.Entry)) {
                return;
            }
            PhraseSuggestion.Entry otherSuggestionEntry = (PhraseSuggestion.Entry) other;
            this.cutoffScore = Math.max(this.cutoffScore, otherSuggestionEntry.cutoffScore);
        }

        @Override
        public void addOption(Suggestion.Entry.Option option) {
            if (option.getScore() > this.cutoffScore) {
                this.options.add(option);
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            // If the other side is older than 0.90.4 then it shouldn't be sending suggestions of this type but just in case
            // we're going to assume that they are regular suggestions so we won't read anything.
            if (in.getVersion().before(Version.V_0_90_4)) {
                return;
            }
            cutoffScore = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            // If the other side of the message is older than 0.90.4 it'll interpret these suggestions as regular suggestions
            // so we have to pretend to be one which we can do by just calling the superclass writeTo and doing nothing else
            if (out.getVersion().before(Version.V_0_90_4)) {
                return;
            }
            out.writeDouble(cutoffScore);
        }
    }
    
    public static enum FilterType {
        NONE((byte) 0),
        MATCH_PHRASE((byte) 1),
        MATCH_OR((byte) 2),
        MATCH_AND((byte) 3);
        
        private byte value;
        
        private FilterType(byte value) {
            this.value = value;
        }
        
        public byte getValue() {
            return value;
        }
        
        public static FilterType fromValue(byte value) {
            switch(value) {
            case 0: return NONE;
            case 1: return MATCH_PHRASE;
            case 2: return MATCH_OR;
            case 3: return MATCH_AND;
            }
            throw new ElasticSearchIllegalArgumentException("filter type [" + value + "] not valid, can be one of [0|1|2|3]");
        }
        
        public static FilterType fromString(String filterType) throws ElasticSearchIllegalArgumentException {
            if ("none".equalsIgnoreCase(filterType)) {
                return NONE;
            } else if ("match_phrase".equalsIgnoreCase(filterType) || "matchPhrase".equalsIgnoreCase(filterType)) {
                return MATCH_PHRASE;
            } else if ("match_or".equalsIgnoreCase(filterType) || "matchOr".equalsIgnoreCase(filterType)) {
                return MATCH_OR;
            } else if ("match_and".equalsIgnoreCase(filterType) || "matchAnd".equalsIgnoreCase(filterType)) {
                return MATCH_AND;
            }
            throw new ElasticSearchIllegalArgumentException("filter type [" + filterType + "] not valid, can be one of [none|match_phrase|matchPhrase|match_or|matchOr|match_and|matchAnd]");
        }
    }
}
