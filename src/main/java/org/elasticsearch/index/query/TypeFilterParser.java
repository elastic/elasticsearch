/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;

import java.io.IOException;

public class TypeFilterParser implements FilterParser {

    public static final String NAME = "type";

    @Inject
    public TypeFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[type] filter should have a value field, and the type name");
        }
        String fieldName = parser.currentName();
        if (!fieldName.equals("value")) {
            throw new QueryParsingException(parseContext.index(), "[type] filter should have a value field, and the type name");
        }
        token = parser.nextToken();
        if (token != XContentParser.Token.VALUE_STRING) {
            throw new QueryParsingException(parseContext.index(), "[type] filter should have a value field, and the type name");
        }
        BytesRef type = parser.bytes();
        // move to the next token
        parser.nextToken();

        Filter filter;
        //LUCENE 4 UPGRADE document mapper should use bytesref aswell? 
        DocumentMapper documentMapper = parseContext.mapperService().documentMapper(type.utf8ToString());
        if (documentMapper == null) {
            filter = new TermFilter(new Term(TypeFieldMapper.NAME, type));
        } else {
            filter = documentMapper.typeFilter();
        }
        return parseContext.cacheFilter(filter, null);
    }
}