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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class IdsFilterParser implements FilterParser {

    public static final String NAME = "ids";

    @Inject
    public IdsFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        List<BytesRef> ids = new ArrayList<>();
        Collection<String> types = null;
        String filterName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        boolean idsProvided = false;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("values".equals(currentFieldName)) {
                    idsProvided = true;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        BytesRef value = parser.utf8BytesOrNull();
                        if (value == null) {
                            throw new QueryParsingException(parseContext.index(), "No value specified for term filter");
                        }
                        ids.add(value);
                    }
                } else if ("types".equals(currentFieldName) || "type".equals(currentFieldName)) {
                    types = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String value = parser.textOrNull();
                        if (value == null) {
                            throw new QueryParsingException(parseContext.index(), "No type specified for term filter");
                        }
                        types.add(value);
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[ids] filter does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if ("type".equals(currentFieldName) || "_type".equals(currentFieldName)) {
                    types = ImmutableList.of(parser.text());
                } else if ("_name".equals(currentFieldName)) {
                    filterName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[ids] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if (!idsProvided) {
            throw new QueryParsingException(parseContext.index(), "[ids] filter requires providing a values element");
        }

        if (ids.isEmpty()) {
            return Queries.newMatchNoDocsFilter();
        }

        if (types == null || types.isEmpty()) {
            types = parseContext.queryTypes();
        } else if (types.size() == 1 && Iterables.getFirst(types, null).equals("_all")) {
            types = parseContext.mapperService().types();
        }

        Filter filter = Queries.wrap(new TermsQuery(UidFieldMapper.NAME, Uid.createTypeUids(types, ids)));
        if (filterName != null) {
            parseContext.addNamedFilter(filterName, filter);
        }
        return filter;
    }
}
