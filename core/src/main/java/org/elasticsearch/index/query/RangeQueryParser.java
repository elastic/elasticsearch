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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.joda.time.DateTimeZone;

import java.io.IOException;

/**
 *
 */
public class RangeQueryParser implements QueryParser {

    public static final String NAME = "range";
    private static final ParseField FIELDDATA_FIELD = new ParseField("fielddata").withAllDeprecated("[no replacement]");
    private static final ParseField NAME_FIELD = new ParseField("_name").withAllDeprecated("query name is not supported in short version of range query");

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

        String fieldName = null;
        Object from = null;
        Object to = null;
        boolean includeLower = true;
        boolean includeUpper = true;
        DateTimeZone timeZone = null;
        DateMathParser forcedDateParser = null;
        float boost = 1.0f;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
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
                        } else if ("format".equals(currentFieldName)) {
                            forcedDateParser = new DateMathParser(Joda.forPattern(parser.text()));
                        } else {
                            throw new QueryParsingException(parseContext, "[range] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, FIELDDATA_FIELD)) {
                    // ignore
                } else {
                    throw new QueryParsingException(parseContext, "[range] query does not support [" + currentFieldName + "]");
                }
            }
        }

        Query query = null;
        MappedFieldType mapper = parseContext.fieldMapper(fieldName);
        if (mapper != null) {
            if (mapper instanceof DateFieldMapper.DateFieldType) {
                query = ((DateFieldMapper.DateFieldType) mapper).rangeQuery(from, to, includeLower, includeUpper, timeZone, forcedDateParser);
            } else  {
                if (timeZone != null) {
                    throw new QueryParsingException(parseContext, "[range] time_zone can not be applied to non date field ["
                            + fieldName + "]");
                }
                //LUCENE 4 UPGRADE Mapper#rangeQuery should use bytesref as well?
                query = mapper.rangeQuery(from, to, includeLower, includeUpper);
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
