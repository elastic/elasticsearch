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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;

public class TypeFilterParser extends AbstractIndexComponent implements XContentFilterParser {

    public static final String NAME = "type";

    @Inject public TypeFilterParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String[] names() {
        return new String[]{NAME};
    }

    @Override public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(index, "type filter should have a value field, and the type name");
        }
        String fieldName = parser.currentName();
        if (!fieldName.equals("value")) {
            throw new QueryParsingException(index, "type filter should have a value field, and the type name");
        }
        token = parser.nextToken();
        if (token != XContentParser.Token.VALUE_STRING) {
            throw new QueryParsingException(index, "type filter should have a value field, and the type name");
        }
        String type = parser.text();
        // move to the next token
        parser.nextToken();

        Filter filter;
        DocumentMapper documentMapper = parseContext.mapperService().documentMapper(type);
        if (documentMapper == null) {
            filter = new TermFilter(new Term(TypeFieldMapper.NAME, type));
        } else {
            filter = documentMapper.typeFilter();
        }
        return parseContext.cacheFilter(filter);
    }
}