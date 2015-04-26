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

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TermRangeQuery;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.joda.time.DateTimeZone;

import java.io.IOException;

/**
 *
 */
public class RangeFilterParser implements FilterParser {

    public static final String NAME = "range";

    @Inject
    public RangeFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        QueryCachingPolicy cache = parseContext.autoFilterCachePolicy();
        HashedBytesRef cacheKey = null;
        String fieldName = null;
        Object from = null;
        Object to = null;
        boolean includeLower = true;
        boolean includeUpper = true;
        DateTimeZone timeZone = null;
        DateMathParser forcedDateParser = null;
        String execution = "index";

        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("from".equals(currentFieldName)) {
                            from = parser.objectBytes();
                        } else if ("to".equals(currentFieldName)) {
                            to = parser.objectBytes();
                        } else if ("include_lower".equals(currentFieldName) || "includeLower".equals(currentFieldName)) {
                            includeLower = parser.booleanValue();
                        } else if ("include_upper".equals(currentFieldName) || "includeUpper".equals(currentFieldName)) {
                            includeUpper = parser.booleanValue();
                        } else if ("gt".equals(currentFieldName)) {
                            from = parser.objectBytes();
                            includeLower = false;
                        } else if ("gte".equals(currentFieldName) || "ge".equals(currentFieldName)) {
                            from = parser.objectBytes();
                            includeLower = true;
                        } else if ("lt".equals(currentFieldName)) {
                            to = parser.objectBytes();
                            includeUpper = false;
                        } else if ("lte".equals(currentFieldName) || "le".equals(currentFieldName)) {
                            to = parser.objectBytes();
                            includeUpper = true;
                        } else if ("time_zone".equals(currentFieldName) || "timeZone".equals(currentFieldName)) {
                            timeZone = DateTimeZone.forID(parser.text());
                        } else if ("format".equals(currentFieldName)) {
                            forcedDateParser = new DateMathParser(Joda.forPattern(parser.text()), DateFieldMapper.Defaults.TIME_UNIT);
                        } else {
                            throw new QueryParsingException(parseContext.index(), "[range] filter does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parseContext.parseFilterCachePolicy();
                } else if ("_cache_key".equals(currentFieldName) || "_cacheKey".equals(currentFieldName)) {
                    cacheKey = new HashedBytesRef(parser.text());
                } else if ("execution".equals(currentFieldName)) {
                    execution = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[range] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if (fieldName == null) {
            throw new QueryParsingException(parseContext.index(), "[range] filter no field specified for range filter");
        }

        Filter filter = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                if (execution.equals("index")) {
                    FieldMapper mapper = smartNameFieldMappers.mapper();
                    if (mapper instanceof DateFieldMapper) {
                        if ((from instanceof Number || to instanceof Number) && timeZone != null) {
                            throw new QueryParsingException(parseContext.index(), "[range] time_zone when using ms since epoch format as it's UTC based can not be applied to [" + fieldName + "]");
                        }
                        filter = ((DateFieldMapper) mapper).rangeFilter(from, to, includeLower, includeUpper, timeZone, forcedDateParser, parseContext);
                    } else  {
                        if (timeZone != null) {
                            throw new QueryParsingException(parseContext.index(), "[range] time_zone can not be applied to non date field [" + fieldName + "]");
                        }
                        filter = mapper.rangeFilter(from, to, includeLower, includeUpper, parseContext);
                    }
                } else if ("fielddata".equals(execution)) {
                    FieldMapper mapper = smartNameFieldMappers.mapper();
                    if (!(mapper instanceof NumberFieldMapper)) {
                        throw new QueryParsingException(parseContext.index(), "[range] filter field [" + fieldName + "] is not a numeric type");
                    }
                    if (mapper instanceof DateFieldMapper) {
                        if ((from instanceof Number || to instanceof Number) && timeZone != null) {
                            throw new QueryParsingException(parseContext.index(), "[range] time_zone when using ms since epoch format as it's UTC based can not be applied to [" + fieldName + "]");
                        }
                        filter = ((DateFieldMapper) mapper).rangeFilter(parseContext, from, to, includeLower, includeUpper, timeZone, forcedDateParser, parseContext);
                    } else {
                        if (timeZone != null) {
                            throw new QueryParsingException(parseContext.index(), "[range] time_zone can not be applied to non date field [" + fieldName + "]");
                        }
                        filter = ((NumberFieldMapper) mapper).rangeFilter(parseContext, from, to, includeLower, includeUpper, parseContext);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[range] filter doesn't support [" + execution + "] execution");
                }
            }
        }

        if (filter == null) {
            filter = Queries.wrap(new TermRangeQuery(fieldName, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to), includeLower, includeUpper));
        }

        if (cache != null) {
            filter = parseContext.cacheFilter(filter, cacheKey, cache);
        }

        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}
