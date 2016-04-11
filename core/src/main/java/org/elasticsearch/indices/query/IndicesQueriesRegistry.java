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

package org.elasticsearch.indices.query;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.index.query.QueryParser;

import java.util.Map;

public class IndicesQueriesRegistry extends AbstractComponent {
    private Map<String, Tuple<ParseField, QueryParser<?>>> queryParsers;

    public IndicesQueriesRegistry(Settings settings, Map<String, Tuple<ParseField, QueryParser<?>>> queryParsers) {
        super(settings);
        this.queryParsers = queryParsers;
    }

    /**
     * Get the query parser for a specific type of query registered under its name.
     * Uses {@link ParseField} internally so that deprecation warnings/errors can be logged/thrown.
     * @param name the name of the parser to retrieve
     * @param parseFieldMatcher the {@link ParseFieldMatcher} to match the query name against
     * @param xContentLocation the current location of the {@link org.elasticsearch.common.xcontent.XContentParser}
     * @return the query parser
     * @throws IllegalArgumentException of there's no query or parser registered under the provided name
     */
    public QueryParser<?> getQueryParser(String name, ParseFieldMatcher parseFieldMatcher, XContentLocation xContentLocation) {
        Tuple<ParseField, QueryParser<?>> parseFieldQueryParserTuple = queryParsers.get(name);
        if (parseFieldQueryParserTuple == null) {
            throw new ParsingException(xContentLocation, "No query registered for [" + name + "]");
        }
        ParseField parseField = parseFieldQueryParserTuple.v1();
        QueryParser<?> queryParser = parseFieldQueryParserTuple.v2();
        boolean match = parseFieldMatcher.match(name, parseField);
        //this is always expected to match, ParseField is useful for deprecation warnings etc. here
        assert match : "registered ParseField did not match the query name it was registered for: [" + name + "]";
        return queryParser;
    }
}
