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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.joda.time.DateTimeZone;

import java.io.IOException;

/**
 *
 */
public class RangeQueryParser implements QueryParser {

    public static final String NAME = "range";

    @Inject
    public RangeQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[range] query malformed, no field to indicate field name");
        }
        String fieldName = parser.currentName();
        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new QueryParsingException(parseContext.index(), "[range] query malformed, after field missing start object");
        }

        Object from = null;
        Object to = null;
        boolean includeLower = true;
        boolean includeUpper = true;
        DateTimeZone timeZone = null;
        DateMathParser forcedDateParser = null;
        float boost = 1.0f;
        String queryName = null;

        String currentFieldName = null;
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
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
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
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("format".equals(currentFieldName)) {
                    forcedDateParser = new DateMathParser(Joda.forPattern(parser.text()), DateFieldMapper.Defaults.TIME_UNIT);
                } else {
                    throw new QueryParsingException(parseContext.index(), "[range] query does not support [" + currentFieldName + "]");
                }
            }
        }

        // move to the next end object, to close the field name
        token = parser.nextToken();
        if (token != XContentParser.Token.END_OBJECT) {
            throw new QueryParsingException(parseContext.index(), "[range] query malformed, does not end with an object");
        }

        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                FieldMapper mapper = smartNameFieldMappers.mapper();
                if (mapper instanceof DateFieldMapper) {
                    if ((from instanceof Number || to instanceof Number) && timeZone != null) {
                        throw new QueryParsingException(parseContext.index(), "[range] time_zone when using ms since epoch format as it's UTC based can not be applied to [" + fieldName + "]");
                    }
                    query = ((DateFieldMapper) mapper).rangeQuery(from, to, includeLower, includeUpper, timeZone, forcedDateParser, parseContext);
                } else  {
                    if (timeZone != null) {
                        throw new QueryParsingException(parseContext.index(), "[range] time_zone can not be applied to non date field [" + fieldName + "]");
                    }
                    //LUCENE 4 UPGRADE Mapper#rangeQuery should use bytesref as well?
                    query = mapper.rangeQuery(from, to, includeLower, includeUpper, parseContext);
                }

            }
        }
        if (query == null) {
            query = new TermRangeQuery(fieldName, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to), includeLower, includeUpper);
        }
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
