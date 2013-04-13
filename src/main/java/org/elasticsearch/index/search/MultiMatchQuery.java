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

package org.elasticsearch.index.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameQuery;

public class MultiMatchQuery extends MatchQuery {

    private boolean useDisMax = true;
    private float tieBreaker;

    public void setUseDisMax(boolean useDisMax) {
        this.useDisMax = useDisMax;
    }

    public void setTieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
    }

    public MultiMatchQuery(QueryParseContext parseContext) {
        super(parseContext);
    }

    public Query parse(Type type, Map<String, Float> fieldNames, Object value) throws IOException {
        if (fieldNames.size() == 1) {
            if (type == Type.ACROSS){
                // In a single term case we're just doing a boolean AND query
                type = Type.BOOLEAN;
                this.setOccur(BooleanClause.Occur.MUST);
            }
            Map.Entry<String, Float> fieldBoost = fieldNames.entrySet().iterator().next();
            Float boostValue = fieldBoost.getValue();
            if (boostValue == null) {
                return parse(type, fieldBoost.getKey(), value);
            } else {
                Query query = parse(type, fieldBoost.getKey(), value);
                query.setBoost(boostValue);
                return query;
            }
        }
        if (type == Type.ACROSS) {
            final BooleanQuery booleanQuery = new BooleanQuery();
            final List<List<Query>> termMatrix = new ArrayList<List<Query>>();
            int fieldNumber = 0;
            for (String fieldName : fieldNames.keySet()){
                FieldMapper mapper = null;
                final String field;
                MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
                if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
                    mapper = smartNameFieldMappers.mapper();
                    field = mapper.names().indexName();
                } else {
                    field = fieldName;
                }

                if (mapper != null && mapper.useTermQueryWithQueryString()) {
                    if (smartNameFieldMappers.explicitTypeInNameWithDocMapper()) {
                        String[] previousTypes = QueryParseContext.setTypesWithPrevious(new String[]{smartNameFieldMappers.docMapper().type()});
                        try {
                            return wrapSmartNameQuery(mapper.termQuery(value, parseContext), smartNameFieldMappers, parseContext);
                        } catch (RuntimeException e) {
                            if (lenient) {
                                return null;
                            }
                            throw e;
                        } finally {
                            QueryParseContext.setTypes(previousTypes);
                        }
                    } else {
                        try {
                            return wrapSmartNameQuery(mapper.termQuery(value, parseContext), smartNameFieldMappers, parseContext);
                        } catch (RuntimeException e) {
                            if (lenient) {
                                return null;
                            }
                            throw e;
                        }
                    }
                }

                Analyzer analyzer = null;
                if (this.analyzer == null) {
                    if (mapper != null) {
                        analyzer = mapper.searchAnalyzer();
                    }
                    if (analyzer == null && smartNameFieldMappers != null) {
                        analyzer = smartNameFieldMappers.searchAnalyzer();
                    }
                    if (analyzer == null) {
                        analyzer = parseContext.mapperService().searchAnalyzer();
                    }
                } else {
                    analyzer = parseContext.mapperService().analysisService().analyzer(this.analyzer);
                    if (analyzer == null) {
                        throw new ElasticSearchIllegalArgumentException("No analyzer found for [" + this.analyzer + "]");
                    }
                }

                // Logic similar to QueryParser#getFieldQuery
                final TokenStream source = analyzer.tokenStream(field, new FastStringReader(value.toString()));
                source.reset();
                int numTokens = 0;
                int positionCount = 0;
                boolean severalTokensAtSamePosition = false;

                final CachingTokenFilter buffer = new CachingTokenFilter(source);
                buffer.reset();
                final CharTermAttribute termAtt = buffer.addAttribute(CharTermAttribute.class);
                final PositionIncrementAttribute posIncrAtt = buffer.addAttribute(PositionIncrementAttribute.class);
                boolean hasMoreTokens =  buffer.incrementToken();
                while (hasMoreTokens) {
                    numTokens++;
                    int positionIncrement = posIncrAtt.getPositionIncrement();
                    if (positionIncrement != 0) {
                        positionCount += positionIncrement;
                    } else {
                        severalTokensAtSamePosition = true;
                    }
                    hasMoreTokens = buffer.incrementToken();
                }
                // rewind the buffer stream
                buffer.reset();
                source.close();

                int matrixMissing = positionCount - termMatrix.size();
                for (int i = 0; i < matrixMissing; i++){
                    List<Query> termList = new ArrayList<Query>();
                    for (int j = 0; j < fieldNumber; j++){
                        termList.add(null);
                    }
                    termMatrix.add(termList);
                }

                int position = 0;
                for (int i = 0; i < numTokens; i++) {
                    boolean hasNext = buffer.incrementToken();
                    assert hasNext == true;
                    int positionIncrement = posIncrAtt.getPositionIncrement();
                    for (int p = 1; p < positionIncrement; p++) {
                        termMatrix.get(position).add(null);
                        position++;
                    }
                    Query q = newTermQuery(mapper, new Term(field, termToByteRef(termAtt)));
                    Float boostValue = fieldNames.get(field);
                    if (boostValue != null) {
                        q.setBoost(boostValue);
                    }
                    termMatrix.get(position).add(wrapSmartNameQuery(q, smartNameFieldMappers, parseContext));
                    if (positionIncrement > 0){
                        position++;
                    }
                }
                int termsToPad = termMatrix.size() - position;
                for (int i = 0; i < termsToPad; i++){
                    termMatrix.get(position + i).add(null);
                }
                fieldNumber++;
            }

            for (List<Query> termList : termMatrix){
                if(termList.contains(null)){
                    // At least once field evalues token to nothing, so this is a SHOULD
                    for (Query query : termList){
                        if (query != null){
                            booleanQuery.add(query, BooleanClause.Occur.SHOULD);
                        }
                    }
                }else{
                    BooleanQuery crossQuery = new BooleanQuery();
                    for (Query query : termList){ //never null. see above test
                        crossQuery.add(query, BooleanClause.Occur.SHOULD);
                    }
                    booleanQuery.add(crossQuery, BooleanClause.Occur.MUST);
                }
            }
            return !booleanQuery.clauses().isEmpty() ? booleanQuery : null;
        }else if (useDisMax) {
            DisjunctionMaxQuery disMaxQuery = new DisjunctionMaxQuery(tieBreaker);
            boolean clauseAdded = false;
            for (String fieldName : fieldNames.keySet()) {
                Query query = parse(type, fieldName, value);
                Float boostValue = fieldNames.get(fieldName);
                if (boostValue != null) {
                    query.setBoost(boostValue);
                }
                if (query != null) {
                    clauseAdded = true;
                    disMaxQuery.add(query);
                }
            }
            return clauseAdded ? disMaxQuery : null;
        } else {
            BooleanQuery booleanQuery = new BooleanQuery();
            for (String fieldName : fieldNames.keySet()) {
                Query query = parse(type, fieldName, value);
                Float boostValue = fieldNames.get(fieldName);
                if (boostValue != null) {
                    query.setBoost(boostValue);
                }
                if (query != null) {
                    booleanQuery.add(query, BooleanClause.Occur.SHOULD);
                }
            }
            return !booleanQuery.clauses().isEmpty() ? booleanQuery : null;
        }
    }

}