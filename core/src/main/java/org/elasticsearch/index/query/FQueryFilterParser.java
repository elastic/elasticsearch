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

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * The "fquery" filter is the same as the {@link QueryFilterParser} except that it allows also to
 * associate a name with the query filter.
 */
@Deprecated
public class FQueryFilterParser implements QueryParser {

    public static final String NAME = "fquery";
    private final DeprecationLogger deprecationLogger;

    @Inject
    public FQueryFilterParser() {
        ESLogger logger = Loggers.getLogger(getClass());
        deprecationLogger = new DeprecationLogger(logger);
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        deprecationLogger.deprecated("The [fquery] filter is deprecated, you can now use queries as filters directly.");

        XContentParser parser = parseContext.parser();

        Query query = null;
        boolean queryFound = false;

        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("query".equals(currentFieldName)) {
                    queryFound = true;
                    query = parseContext.parseInnerQuery();
                } else {
                    throw new QueryParsingException(parseContext, "[fquery] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext, "[fquery] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (!queryFound) {
            throw new QueryParsingException(parseContext, "[fquery] requires 'query' element");
        }
        if (query == null) {
            return null;
        }
        query = new ConstantScoreQuery(query);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
