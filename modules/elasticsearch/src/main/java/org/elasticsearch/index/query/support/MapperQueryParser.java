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
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.util.Nullable;

import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * A query parser that uses the {@link MapperService} in order to build smarter
 * queries based on the mapping information.
 *
 * <p>Maps a logic name of a field {@link org.elasticsearch.index.mapper.FieldMapper#name()}
 * into its {@link org.elasticsearch.index.mapper.FieldMapper#indexName()}.
 *
 * <p>Also breaks fields with [type].[name] into a boolean query that must include the type
 * as well as the query on the name.
 *
 * @author kimchy (Shay Banon)
 */
public class MapperQueryParser extends QueryParser {

    private final MapperService mapperService;

    private final FilterCache filterCache;

    public MapperQueryParser(String defaultField, Analyzer analyzer,
                             @Nullable MapperService mapperService,
                             @Nullable FilterCache filterCache) {
        super(Version.LUCENE_CURRENT, defaultField, analyzer);
        this.mapperService = mapperService;
        this.filterCache = filterCache;
        setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_AUTO_REWRITE_DEFAULT);
    }

    @Override protected Query getFieldQuery(String field, String queryText) throws ParseException {
        String indexedNameField = field;
        if (mapperService != null) {
            MapperService.SmartNameFieldMappers fieldMappers = mapperService.smartName(field);
            if (fieldMappers != null) {
                if (fieldMappers.fieldMappers().mapper() != null) {
                    Query query = fieldMappers.fieldMappers().mapper().fieldQuery(queryText);
                    return wrapSmartNameQuery(query, fieldMappers, filterCache);
                }
            }
        }
        return super.getFieldQuery(indexedNameField, queryText);
    }

    @Override protected Query getRangeQuery(String field, String part1, String part2, boolean inclusive) throws ParseException {
        if (mapperService != null) {
            MapperService.SmartNameFieldMappers fieldMappers = mapperService.smartName(field);
            if (fieldMappers != null) {
                if (fieldMappers.fieldMappers().mapper() != null) {
                    Query rangeQuery = fieldMappers.fieldMappers().mapper().rangeQuery(part1, part2, inclusive, inclusive);
                    return wrapSmartNameQuery(rangeQuery, fieldMappers, filterCache);
                }
            }
        }
        return super.getRangeQuery(field, part1, part2, inclusive);
    }

    @Override protected Query getPrefixQuery(String field, String termStr) throws ParseException {
        String indexedNameField = field;
        if (mapperService != null) {
            MapperService.SmartNameFieldMappers fieldMappers = mapperService.smartName(field);
            if (fieldMappers != null) {
                if (fieldMappers.fieldMappers().mapper() != null) {
                    indexedNameField = fieldMappers.fieldMappers().mapper().indexName();
                }
                return wrapSmartNameQuery(super.getPrefixQuery(indexedNameField, termStr), fieldMappers, filterCache);
            }
        }
        return super.getPrefixQuery(indexedNameField, termStr);
    }

    @Override protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException {
        String indexedNameField = field;
        if (mapperService != null) {
            MapperService.SmartNameFieldMappers fieldMappers = mapperService.smartName(field);
            if (fieldMappers != null) {
                if (fieldMappers.fieldMappers().mapper() != null) {
                    indexedNameField = fieldMappers.fieldMappers().mapper().indexName();
                }
                return wrapSmartNameQuery(super.getFuzzyQuery(indexedNameField, termStr, minSimilarity), fieldMappers, filterCache);
            }
        }
        return super.getFuzzyQuery(indexedNameField, termStr, minSimilarity);
    }

    @Override protected Query getWildcardQuery(String field, String termStr) throws ParseException {
        String indexedNameField = field;
        if (mapperService != null) {
            MapperService.SmartNameFieldMappers fieldMappers = mapperService.smartName(field);
            if (fieldMappers != null) {
                if (fieldMappers.fieldMappers().mapper() != null) {
                    indexedNameField = fieldMappers.fieldMappers().mapper().indexName();
                }
                return wrapSmartNameQuery(super.getWildcardQuery(indexedNameField, termStr), fieldMappers, filterCache);
            }
        }
        return super.getWildcardQuery(indexedNameField, termStr);
    }

    protected FieldMapper fieldMapper(String smartName) {
        if (mapperService == null) {
            return null;
        }
        FieldMappers fieldMappers = mapperService.smartNameFieldMappers(smartName);
        if (fieldMappers == null) {
            return null;
        }
        return fieldMappers.mapper();
    }
}
