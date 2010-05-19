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

package org.elasticsearch.index.query.support;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.xcontent.QueryParseContext;
import org.elasticsearch.util.lucene.Lucene;

import java.util.List;

import static org.elasticsearch.index.query.support.QueryParsers.*;
import static org.elasticsearch.util.lucene.search.Queries.*;

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

    private final QueryParseContext parseContext;

    private FieldMapper currentMapper;

    public MapperQueryParser(String defaultField, Analyzer analyzer,
                             QueryParseContext parseContext) {
        super(Lucene.QUERYPARSER_VERSION, defaultField, analyzer);
        this.parseContext = parseContext;
        setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);
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

    @Override public Query getFieldQuery(String field, String queryText) throws ParseException {
        currentMapper = null;
        if (parseContext.mapperService() != null) {
            MapperService.SmartNameFieldMappers fieldMappers = parseContext.mapperService().smartName(field);
            if (fieldMappers != null) {
                currentMapper = fieldMappers.fieldMappers().mapper();
                if (currentMapper != null) {
                    Query query = null;
                    if (currentMapper.useFieldQueryWithQueryString()) {
                        query = currentMapper.fieldQuery(queryText);
                    }
                    if (query == null) {
                        query = super.getFieldQuery(currentMapper.names().indexName(), queryText);
                    }
                    return wrapSmartNameQuery(query, fieldMappers, parseContext);
                }
            }
        }
        return super.getFieldQuery(field, queryText);
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
                return wrapSmartNameQuery(super.getPrefixQuery(indexedNameField, termStr), fieldMappers, parseContext);
            }
        }
        return super.getPrefixQuery(indexedNameField, termStr);
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

    @Override protected Query getWildcardQuery(String field, String termStr) throws ParseException {
        String indexedNameField = field;
        currentMapper = null;
        if (parseContext.mapperService() != null) {
            MapperService.SmartNameFieldMappers fieldMappers = parseContext.mapperService().smartName(field);
            if (fieldMappers != null) {
                currentMapper = fieldMappers.fieldMappers().mapper();
                if (currentMapper != null) {
                    indexedNameField = currentMapper.names().indexName();
                }
                return wrapSmartNameQuery(super.getWildcardQuery(indexedNameField, termStr), fieldMappers, parseContext);
            }
        }
        return super.getWildcardQuery(indexedNameField, termStr);
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
