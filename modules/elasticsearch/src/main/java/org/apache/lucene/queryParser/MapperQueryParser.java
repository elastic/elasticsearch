/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.apache.lucene.queryParser;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.xcontent.QueryParseContext;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.lucene.search.Queries.*;
import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * A query parser that uses the {@link MapperService} in order to build smarter
 * queries based on the mapping information.
 *
 * <p>Also breaks fields with [type].[name] into a boolean query that must include the type
 * as well as the query on the name.
 *
 * @author kimchy (shay.banon)
 */
public class MapperQueryParser extends QueryParser {

    public static final ImmutableMap<String, FieldQueryExtension> fieldQueryExtensions;

    static {
        fieldQueryExtensions = ImmutableMap.<String, FieldQueryExtension>builder()
                .put(ExistsFieldQueryExtension.NAME, new ExistsFieldQueryExtension())
                .put(MissingFieldQueryExtension.NAME, new MissingFieldQueryExtension())
                .build();
    }

    private final QueryParseContext parseContext;

    private FieldMapper currentMapper;

    private boolean analyzeWildcard;

    public MapperQueryParser(QueryParseContext parseContext) {
        super(Lucene.QUERYPARSER_VERSION, null, null);
        this.parseContext = parseContext;
    }

    public MapperQueryParser(QueryParserSettings settings, QueryParseContext parseContext) {
        super(Lucene.QUERYPARSER_VERSION, settings.defaultField(), settings.analyzer());
        this.parseContext = parseContext;
        reset(settings);
    }

    public void reset(QueryParserSettings settings) {
        this.field = settings.defaultField();
        this.analyzer = settings.analyzer();
        setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);
        setEnablePositionIncrements(settings.enablePositionIncrements());
        setAllowLeadingWildcard(settings.allowLeadingWildcard());
        setLowercaseExpandedTerms(settings.lowercaseExpandedTerms());
        setPhraseSlop(settings.phraseSlop());
        setDefaultOperator(settings.defaultOperator());
        setFuzzyMinSim(settings.fuzzyMinSim());
        setFuzzyPrefixLength(settings.fuzzyPrefixLength());
        this.analyzeWildcard = settings.analyzeWildcard();
    }

    @Override protected Query newTermQuery(Term term) {
        if (currentMapper != null) {
            Query termQuery = currentMapper.queryStringTermQuery(term);
            if (termQuery != null) {
                return termQuery;
            }
        }
        return super.newTermQuery(term);
    }

    @Override protected Query newMatchAllDocsQuery() {
        return Queries.MATCH_ALL_QUERY;
    }

    @Override public Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
        FieldQueryExtension fieldQueryExtension = fieldQueryExtensions.get(field);
        if (fieldQueryExtension != null) {
            return fieldQueryExtension.query(parseContext, queryText);
        }
        currentMapper = null;
        if (parseContext.mapperService() != null) {
            MapperService.SmartNameFieldMappers fieldMappers = parseContext.mapperService().smartName(field);
            if (fieldMappers != null) {
                currentMapper = fieldMappers.fieldMappers().mapper();
                if (currentMapper != null) {
                    Query query = null;
                    if (currentMapper.useFieldQueryWithQueryString()) {
                        query = currentMapper.fieldQuery(queryText, parseContext);
                    }
                    if (query == null) {
                        query = super.getFieldQuery(currentMapper.names().indexName(), queryText, quoted);
                    }
                    return wrapSmartNameQuery(query, fieldMappers, parseContext);
                }
            }
        }
        return super.getFieldQuery(field, queryText, quoted);
    }

    @Override protected Query getRangeQuery(String field, String part1, String part2, boolean inclusive) throws ParseException {
        if ("*".equals(part1)) {
            part1 = null;
        }
        if ("*".equals(part2)) {
            part2 = null;
        }
        currentMapper = null;
        if (parseContext.mapperService() != null) {
            MapperService.SmartNameFieldMappers fieldMappers = parseContext.mapperService().smartName(field);
            if (fieldMappers != null) {
                currentMapper = fieldMappers.fieldMappers().mapper();
                if (currentMapper != null) {
                    Query rangeQuery = currentMapper.rangeQuery(part1, part2, inclusive, inclusive);
                    return wrapSmartNameQuery(rangeQuery, fieldMappers, parseContext);
                }
            }
        }
        return newRangeQuery(field, part1, part2, inclusive);
    }

    @Override protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException {
        String indexedNameField = field;
        currentMapper = null;
        if (parseContext.mapperService() != null) {
            MapperService.SmartNameFieldMappers fieldMappers = parseContext.mapperService().smartName(field);
            if (fieldMappers != null) {
                currentMapper = fieldMappers.fieldMappers().mapper();
                if (currentMapper != null) {
                    indexedNameField = currentMapper.names().indexName();
                }
                return wrapSmartNameQuery(super.getFuzzyQuery(indexedNameField, termStr, minSimilarity), fieldMappers, parseContext);
            }
        }
        return super.getFuzzyQuery(indexedNameField, termStr, minSimilarity);
    }

    @Override protected Query getPrefixQuery(String field, String termStr) throws ParseException {
        String indexedNameField = field;
        currentMapper = null;
        if (parseContext.mapperService() != null) {
            MapperService.SmartNameFieldMappers fieldMappers = parseContext.mapperService().smartName(field);
            if (fieldMappers != null) {
                currentMapper = fieldMappers.fieldMappers().mapper();
                if (currentMapper != null) {
                    indexedNameField = currentMapper.names().indexName();
                }
                return wrapSmartNameQuery(getPossiblyAnalyzedPrefixQuery(indexedNameField, termStr), fieldMappers, parseContext);
            }
        }
        return getPossiblyAnalyzedPrefixQuery(indexedNameField, termStr);
    }

    private Query getPossiblyAnalyzedPrefixQuery(String field, String termStr) throws ParseException {
        if (!analyzeWildcard) {
            return super.getPrefixQuery(field, termStr);
        }
        // get Analyzer from superclass and tokenize the term
        TokenStream source;
        try {
            source = getAnalyzer().reusableTokenStream(field, new StringReader(termStr));
        } catch (IOException e) {
            return super.getPrefixQuery(field, termStr);
        }
        List<String> tlist = new ArrayList<String>();
        CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);

        while (true) {
            try {
                if (!source.incrementToken()) break;
            } catch (IOException e) {
                break;
            }
            tlist.add(termAtt.toString());
        }

        try {
            source.close();
        } catch (IOException e) {
            // ignore
        }

        if (tlist.size() == 1) {
            return super.getPrefixQuery(field, tlist.get(0));
        } else {
            return super.getPrefixQuery(field, termStr);
            /* this means that the analyzer used either added or consumed
* (common for a stemmer) tokens, and we can't build a PrefixQuery */
//            throw new ParseException("Cannot build PrefixQuery with analyzer "
//                    + getAnalyzer().getClass()
//                    + (tlist.size() > 1 ? " - token(s) added" : " - token consumed"));
        }

    }

    @Override protected Query getWildcardQuery(String field, String termStr) throws ParseException {
        if (AllFieldMapper.NAME.equals(field) && termStr.equals("*")) {
            return newMatchAllDocsQuery();
        }
        String indexedNameField = field;
        currentMapper = null;
        if (parseContext.mapperService() != null) {
            MapperService.SmartNameFieldMappers fieldMappers = parseContext.mapperService().smartName(field);
            if (fieldMappers != null) {
                currentMapper = fieldMappers.fieldMappers().mapper();
                if (currentMapper != null) {
                    indexedNameField = currentMapper.names().indexName();
                }
                return wrapSmartNameQuery(getPossiblyAnalyzedWildcardQuery(indexedNameField, termStr), fieldMappers, parseContext);
            }
        }
        return getPossiblyAnalyzedWildcardQuery(indexedNameField, termStr);
    }

    private Query getPossiblyAnalyzedWildcardQuery(String field, String termStr) throws ParseException {
        if (!analyzeWildcard) {
            return super.getWildcardQuery(field, termStr);
        }
        boolean isWithinToken = (!termStr.startsWith("?") && !termStr.startsWith("*"));
        StringBuilder aggStr = new StringBuilder();
        StringBuilder tmp = new StringBuilder();
        for (int i = 0; i < termStr.length(); i++) {
            char c = termStr.charAt(i);
            if (c == '?' || c == '*') {
                if (isWithinToken) {
                    try {
                        TokenStream source = getAnalyzer().reusableTokenStream(field, new FastStringReader(tmp.toString()));
                        CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
                        if (source.incrementToken()) {
                            String term = termAtt.toString();
                            if (term.length() == 0) {
                                // no tokens, just use what we have now
                                aggStr.append(tmp);
                            } else {
                                aggStr.append(term);
                            }
                        } else {
                            // no tokens, just use what we have now
                            aggStr.append(tmp);
                        }
                        source.close();
                    } catch (IOException e) {
                        aggStr.append(tmp);
                    }
                    tmp.setLength(0);
                }
                isWithinToken = false;
                aggStr.append(c);
            } else {
                tmp.append(c);
                isWithinToken = true;
            }
        }
        if (isWithinToken) {
            try {
                TokenStream source = getAnalyzer().reusableTokenStream(field, new FastStringReader(tmp.toString()));
                CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
                if (source.incrementToken()) {
                    String term = termAtt.toString();
                    if (term.length() == 0) {
                        // no tokens, just use what we have now
                        aggStr.append(tmp);
                    } else {
                        aggStr.append(term);
                    }
                } else {
                    // no tokens, just use what we have now
                    aggStr.append(tmp);
                }
                source.close();
            } catch (IOException e) {
                aggStr.append(tmp);
            }
        }

        return super.getWildcardQuery(field, aggStr.toString());
    }

    @Override protected Query getBooleanQuery(List<BooleanClause> clauses, boolean disableCoord) throws ParseException {
        Query q = super.getBooleanQuery(clauses, disableCoord);
        if (q == null) {
            return null;
        }
        return optimizeQuery(fixNegativeQueryIfNeeded(q));
    }

    protected FieldMapper fieldMapper(String smartName) {
        if (parseContext.mapperService() == null) {
            return null;
        }
        FieldMappers fieldMappers = parseContext.mapperService().smartNameFieldMappers(smartName);
        if (fieldMappers == null) {
            return null;
        }
        return fieldMappers.mapper();
    }
}
