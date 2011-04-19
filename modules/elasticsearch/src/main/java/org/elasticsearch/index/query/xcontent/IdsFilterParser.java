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

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.search.UidFilter;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IdsFilterParser extends AbstractIndexComponent implements XContentFilterParser {

    public static final String NAME = "ids";

    @Inject public IdsFilterParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String[] names() {
        return new String[]{NAME};
    }

    @Override public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        List<String> ids = new ArrayList<String>();
        String type = null;
        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("values".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String value = parser.textOrNull();
                        if (value == null) {
                            throw new QueryParsingException(index, "No value specified for term filter");
                        }
                        ids.add(value);
                    }
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "_type".equals(currentFieldName)) {
                    type = parser.text();
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                }
            }
        }

        if (type == null) {
            throw new QueryParsingException(index, "[ids] filter, no type provided");
        }
        if (ids.size() == 0) {
            throw new QueryParsingException(index, "[ids] filter, no ids values provided");
        }

        UidFilter filter = new UidFilter(type, ids, parseContext.indexCache().bloomCache());
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}
