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
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.terms.TermsByQueryRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.OrFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.cache.filter.terms.FieldTermsLookup;
import org.elasticsearch.indices.cache.filter.terms.IndicesTermsFilterCache;
import org.elasticsearch.indices.cache.filter.terms.QueryTermsLookup;
import org.elasticsearch.indices.cache.filter.terms.TermsLookup;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.query.support.QueryParsers.wrapSmartNameFilter;

/**
 *
 */
public class TermsFilterParser implements FilterParser {

    public static final String NAME = "terms";
    private IndicesTermsFilterCache termsFilterCache;

    @Inject
    public TermsFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME, "in"};
    }

    @Inject(optional = true)
    public void setIndicesTermsFilterCache(IndicesTermsFilterCache termsFilterCache) {
        this.termsFilterCache = termsFilterCache;
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        MapperService.SmartNameFieldMappers smartNameFieldMappers;
        Boolean cache = null;
        String filterName = null;
        String currentFieldName = null;

        String lookupId = null;
        String lookupPath = null;
        String lookupRouting = null;
        List<String> lookupIndices = Lists.newArrayList();
        List<String> lookupTypes = Lists.newArrayList();
        XContentBuilder lookupFilter = null;
        boolean lookupUseBloomFilter = false;
        Double lookupBloomFpp = null;
        Integer lookupBloomExpectedInsertions = null;
        Integer lookupBloomHashFunctions = null;
        Long lookupMaxTermsPerShard = null;
        boolean lookupCache = true;

        CacheKeyFilter.Key cacheKey = null;
        XContentParser.Token token;
        String execution = "plain";
        List<Object> terms = Lists.newArrayList();
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    Object value = parser.objectBytes();
                    if (value == null) {
                        throw new QueryParsingException(parseContext.index(), "No value specified for terms filter");
                    }
                    terms.add(value);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            String value = parser.text();
                            if ("indices".equals(currentFieldName) || "index".equals(currentFieldName)) {
                                if (value != null) {
                                    lookupIndices.add(value);
                                }
                            } else if ("types".equals(currentFieldName) || "type".equals(currentFieldName)) {
                                if (value != null) {
                                    lookupTypes.add(value);
                                }
                            }
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("filter".equals(currentFieldName)) {
                            lookupFilter = XContentFactory.contentBuilder(parser.contentType());
                            lookupFilter.copyCurrentStructure(parser);
                        } else if ("bloom_filter".equals(currentFieldName)) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                lookupUseBloomFilter = true;
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if ("fpp".equals(currentFieldName) ||
                                            "false_positive_probability".equals(currentFieldName) ||
                                            "falsePositiveProbability".equals(currentFieldName)) {
                                        lookupBloomFpp = parser.doubleValue();
                                        if (lookupBloomFpp <= 0 || lookupBloomFpp >= 1) {
                                            throw new QueryParsingException(parseContext.index(), "bloom fpp must be between 0 and 1");
                                        }
                                    } else if ("expected_insertions".equals(currentFieldName) ||
                                            "expectedInsertions".equals(currentFieldName)) {
                                        lookupBloomExpectedInsertions = parser.intValue();
                                        if (lookupBloomExpectedInsertions <= 0) {
                                            throw new QueryParsingException(parseContext.index(), "bloom expected insertions greater than 0");
                                        }
                                    } else if ("hash_functions".equals(currentFieldName) ||
                                            "hashFunctions".equals(currentFieldName)) {
                                        lookupBloomHashFunctions = parser.intValue();
                                        if (lookupBloomHashFunctions < 1 || lookupBloomHashFunctions > 255) {
                                            throw new QueryParsingException(parseContext.index(), "bloom hash functions must be between 1 and 255");
                                        }
                                    } else {
                                        throw new QueryParsingException(parseContext.index(),
                                                "[terms] filter does not support [" + currentFieldName + "] within bloom element");
                                    }
                                }
                            }
                        } else {
                            throw new QueryParsingException(parseContext.index(), "[terms] filter does not support [" + currentFieldName + "] within lookup element");
                        }
                    } else if (token.isValue()) {
                        if ("index".equals(currentFieldName) || "indices".equals(currentFieldName)) {
                            lookupIndices.clear();
                            lookupIndices.add(parser.text());
                        } else if ("type".equals(currentFieldName) || "types".equals(currentFieldName)) {
                            lookupTypes.clear();
                            lookupTypes.add(parser.text());
                        } else if ("id".equals(currentFieldName)) {
                            lookupId = parser.text();
                        } else if ("path".equals(currentFieldName)) {
                            lookupPath = parser.text();
                        } else if ("max_terms_per_shard".equals(currentFieldName) || "maxTermsPerShard".equals(currentFieldName)) {
                            lookupMaxTermsPerShard = parser.longValue();
                        } else if ("routing".equals(currentFieldName)) {
                            lookupRouting = parser.textOrNull();
                        } else if ("cache".equals(currentFieldName)) {
                            lookupCache = parser.booleanValue();
                        } else {
                            throw new QueryParsingException(parseContext.index(), "[terms] filter does not support [" + currentFieldName + "] within lookup element");
                        }
                    }
                }

                if (lookupFilter == null) {
                    if (lookupIndices.size() == 0) {
                        lookupIndices.add(parseContext.index().name());
                    }

                    if (lookupTypes == null || lookupTypes.size() == 0) {
                        throw new QueryParsingException(parseContext.index(), "[terms] filter lookup element requires specifying the type");
                    }

                    if (lookupId == null) {
                        throw new QueryParsingException(parseContext.index(), "[terms] filter lookup element requires specifying the id");
                    }
                }

                if (lookupPath == null) {
                    throw new QueryParsingException(parseContext.index(), "[terms] filter lookup element requires specifying the path");
                }

                if (lookupUseBloomFilter && lookupBloomExpectedInsertions == null) {
                    throw new QueryParsingException(parseContext.index(),
                            "[terms] filter lookup with bloom filter requires the expected number of insertions");
                }
            } else if (token.isValue()) {
                if ("execution".equals(currentFieldName)) {
                    execution = parser.text();
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new CacheKeyFilter.Key(parser.text());
                } else {
                    throw new QueryParsingException(parseContext.index(), "[terms] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if (fieldName == null) {
            throw new QueryParsingException(parseContext.index(), "terms filter requires a field name");
        }

        FieldMapper fieldMapper = null;
        smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        String[] previousTypes = null;
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                fieldMapper = smartNameFieldMappers.mapper();
                fieldName = fieldMapper.names().indexName();
            }
            // if we have a doc mapper, its explicit type, mark it
            if (smartNameFieldMappers.explicitTypeInNameWithDocMapper()) {
                previousTypes = QueryParseContext.setTypesWithPrevious(new String[]{smartNameFieldMappers.docMapper().type()});
            }
        }

        if (lookupId != null || lookupFilter != null) {
            // if there are no mappings, then nothing has been indexing yet against this shard, so we can return
            // no match (but not cached!), since the Terms Lookup relies on the fact that there are mappings...
            if (fieldMapper == null) {
                return Queries.MATCH_NO_FILTER;
            }

            // external lookup, use it
            TermsLookup termsLookup;
            if (lookupFilter != null) {
                final TermsByQueryRequest termsByQueryReq = new TermsByQueryRequest(lookupIndices.toArray(new String[lookupIndices.size()]))
                        .types(lookupTypes.toArray(new String[lookupTypes.size()]))
                        .field(lookupPath)
                        .routing(lookupRouting)
                        .filter(lookupFilter)
                        .useBloomFilter(lookupUseBloomFilter);

                if (lookupMaxTermsPerShard != null) {
                    termsByQueryReq.maxTermsPerShard(lookupMaxTermsPerShard);
                }

                if (lookupUseBloomFilter && lookupBloomFpp != null) {
                    termsByQueryReq.bloomFpp(lookupBloomFpp);
                }

                if (lookupUseBloomFilter && lookupBloomExpectedInsertions != null) {
                    termsByQueryReq.bloomExpectedInsertions(lookupBloomExpectedInsertions);
                }

                if (lookupUseBloomFilter && lookupBloomHashFunctions != null) {
                    termsByQueryReq.bloomHashFunctions(lookupBloomHashFunctions);
                }

                // default to no caching for query terms lookup
                if (cache == null) {
                    cache = false;
                }

                termsLookup = new QueryTermsLookup(termsByQueryReq, parseContext.fieldData().getForField(fieldMapper));
            } else {
                termsLookup = new FieldTermsLookup(fieldMapper, lookupIndices.get(0), lookupTypes.get(0),
                        lookupId, lookupRouting, lookupPath, parseContext);
            }

            Filter filter = termsFilterCache.termsFilter(termsLookup, lookupCache, cacheKey);
            if (filter == null) {
                return null;
            }

            // cache the whole filter by default, or if explicitly told to
            if (cache == null || cache) {
                filter = parseContext.cacheFilter(filter, cacheKey);
            }

            return filter;
        }

        if (terms.isEmpty()) {
            return Queries.MATCH_NO_FILTER;
        }

        try {
            Filter filter;
            if ("plain".equals(execution)) {
                if (fieldMapper != null) {
                    filter = fieldMapper.termsFilter(terms, parseContext);
                } else {
                    BytesRef[] filterValues = new BytesRef[terms.size()];
                    for (int i = 0; i < filterValues.length; i++) {
                        filterValues[i] = BytesRefs.toBytesRef(terms.get(i));
                    }
                    filter = new TermsFilter(fieldName, filterValues);
                }
                // cache the whole filter by default, or if explicitly told to
                if (cache == null || cache) {
                    filter = parseContext.cacheFilter(filter, cacheKey);
                }
            } else if ("fielddata".equals(execution)) {
                // if there are no mappings, then nothing has been indexing yet against this shard, so we can return
                // no match (but not cached!), since the FieldDataTermsFilter relies on a mapping...
                if (fieldMapper == null) {
                    return Queries.MATCH_NO_FILTER;
                }

                filter = fieldMapper.termsFilter(parseContext.fieldData(), terms, parseContext);
                if (cache != null && cache) {
                    filter = parseContext.cacheFilter(filter, cacheKey);
                }
            } else if ("bool".equals(execution)) {
                XBooleanFilter boolFiler = new XBooleanFilter();
                if (fieldMapper != null) {
                    for (Object term : terms) {
                        boolFiler.add(parseContext.cacheFilter(fieldMapper.termFilter(term, parseContext), null), BooleanClause.Occur.SHOULD);
                    }
                } else {
                    for (Object term : terms) {
                        boolFiler.add(parseContext.cacheFilter(new TermFilter(new Term(fieldName, BytesRefs.toBytesRef(term))), null), BooleanClause.Occur.SHOULD);
                    }
                }
                filter = boolFiler;
                // only cache if explicitly told to, since we cache inner filters
                if (cache != null && cache) {
                    filter = parseContext.cacheFilter(filter, cacheKey);
                }
            } else if ("bool_nocache".equals(execution)) {
                XBooleanFilter boolFiler = new XBooleanFilter();
                if (fieldMapper != null) {
                    for (Object term : terms) {
                        boolFiler.add(fieldMapper.termFilter(term, parseContext), BooleanClause.Occur.SHOULD);
                    }
                } else {
                    for (Object term : terms) {
                        boolFiler.add(new TermFilter(new Term(fieldName, BytesRefs.toBytesRef(term))), BooleanClause.Occur.SHOULD);
                    }
                }
                filter = boolFiler;
                // cache the whole filter by default, or if explicitly told to
                if (cache == null || cache) {
                    filter = parseContext.cacheFilter(filter, cacheKey);
                }
            } else if ("and".equals(execution)) {
                List<Filter> filters = Lists.newArrayList();
                if (fieldMapper != null) {
                    for (Object term : terms) {
                        filters.add(parseContext.cacheFilter(fieldMapper.termFilter(term, parseContext), null));
                    }
                } else {
                    for (Object term : terms) {
                        filters.add(parseContext.cacheFilter(new TermFilter(new Term(fieldName, BytesRefs.toBytesRef(term))), null));
                    }
                }
                filter = new AndFilter(filters);
                // only cache if explicitly told to, since we cache inner filters
                if (cache != null && cache) {
                    filter = parseContext.cacheFilter(filter, cacheKey);
                }
            } else if ("and_nocache".equals(execution)) {
                List<Filter> filters = Lists.newArrayList();
                if (fieldMapper != null) {
                    for (Object term : terms) {
                        filters.add(fieldMapper.termFilter(term, parseContext));
                    }
                } else {
                    for (Object term : terms) {
                        filters.add(new TermFilter(new Term(fieldName, BytesRefs.toBytesRef(term))));
                    }
                }
                filter = new AndFilter(filters);
                // cache the whole filter by default, or if explicitly told to
                if (cache == null || cache) {
                    filter = parseContext.cacheFilter(filter, cacheKey);
                }
            } else if ("or".equals(execution)) {
                List<Filter> filters = Lists.newArrayList();
                if (fieldMapper != null) {
                    for (Object term : terms) {
                        filters.add(parseContext.cacheFilter(fieldMapper.termFilter(term, parseContext), null));
                    }
                } else {
                    for (Object term : terms) {
                        filters.add(parseContext.cacheFilter(new TermFilter(new Term(fieldName, BytesRefs.toBytesRef(term))), null));
                    }
                }
                filter = new OrFilter(filters);
                // only cache if explicitly told to, since we cache inner filters
                if (cache != null && cache) {
                    filter = parseContext.cacheFilter(filter, cacheKey);
                }
            } else if ("or_nocache".equals(execution)) {
                List<Filter> filters = Lists.newArrayList();
                if (fieldMapper != null) {
                    for (Object term : terms) {
                        filters.add(fieldMapper.termFilter(term, parseContext));
                    }
                } else {
                    for (Object term : terms) {
                        filters.add(new TermFilter(new Term(fieldName, BytesRefs.toBytesRef(term))));
                    }
                }
                filter = new OrFilter(filters);
                // cache the whole filter by default, or if explicitly told to
                if (cache == null || cache) {
                    filter = parseContext.cacheFilter(filter, cacheKey);
                }
            } else {
                throw new QueryParsingException(parseContext.index(), "terms filter execution value [" + execution + "] not supported");
            }

            filter = wrapSmartNameFilter(filter, smartNameFieldMappers, parseContext);
            if (filterName != null) {
                parseContext.addNamedFilter(filterName, filter);
            }
            return filter;
        } finally {
            if (smartNameFieldMappers != null && smartNameFieldMappers.explicitTypeInNameWithDocMapper()) {
                QueryParseContext.setTypes(previousTypes);
            }
        }
    }
}
