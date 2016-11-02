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

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class QueryParseContextTests extends ESTestCase {

    private static IndicesQueriesRegistry indicesQueriesRegistry;

    @BeforeClass
    public static void init() {
        indicesQueriesRegistry = new SearchModule(Settings.EMPTY, false, emptyList()).getQueryParserRegistry();
    }

    private ThreadContext threadContext;

    @Before
    public void beforeTest() throws IOException {
        this.threadContext = new ThreadContext(Settings.EMPTY);
        DeprecationLogger.setThreadContext(threadContext);
    }

    @After
    public void teardown() throws IOException {
        DeprecationLogger.removeThreadContext(this.threadContext);
        this.threadContext.close();
    }

    public void testParseTopLevelBuilder() throws IOException {
        QueryBuilder query = new MatchQueryBuilder("foo", "bar");
        String requestBody = "{ \"query\" : " + query.toString() + "}";
        try (XContentParser parser = XContentFactory.xContent(requestBody).createParser(requestBody)) {
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.STRICT);
            QueryBuilder actual = context.parseTopLevelQueryBuilder();
            assertEquals(query, actual);
        }
    }

    public void testParseTopLevelBuilderEmptyObject() throws IOException {
        String requestBody = "{}";
        try (XContentParser parser = XContentFactory.xContent(requestBody).createParser(requestBody)) {
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.STRICT);
            QueryBuilder query = context.parseTopLevelQueryBuilder();
            assertNull(query);
        }
    }

    public void testParseTopLevelBuilderUnknownParameter() throws IOException {
        String requestBody = "{ \"foo\" : \"bar\"}";
        try (XContentParser parser = XContentFactory.xContent(requestBody).createParser(requestBody)) {
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.STRICT);
            ParsingException exception = expectThrows(ParsingException.class, () ->  context.parseTopLevelQueryBuilder());
            assertEquals("request does not support [foo]", exception.getMessage());
        }
    }

    public void testParseInnerQueryBuilder() throws IOException {
        QueryBuilder query = new MatchQueryBuilder("foo", "bar");
        String source = query.toString();
        try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.STRICT);
            Optional<QueryBuilder> actual = context.parseInnerQueryBuilder();
            assertEquals(query, actual.get());
        }
    }

    public void testParseInnerQueryBuilderEmptyBody() throws IOException {
        String source = "{}";
        try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.EMPTY);
            Optional<QueryBuilder> emptyQuery = context.parseInnerQueryBuilder();
            assertFalse(emptyQuery.isPresent());
            final List<String> warnings = threadContext.getResponseHeaders().get(DeprecationLogger.DEPRECATION_HEADER);
            assertThat(warnings, hasSize(1));
            assertThat(warnings, hasItem(equalTo("query malformed, empty clause found at [1:2]")));
        }
    }

    public void testParseInnerQueryBuilderExceptions() throws IOException {
        String source = "{ \"foo\": \"bar\" }";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(source)) {
            parser.nextToken();
            parser.nextToken(); // don't start with START_OBJECT to provoke exception
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.STRICT);
            ParsingException exception = expectThrows(ParsingException.class, () ->  context.parseInnerQueryBuilder());
            assertEquals("[_na] query malformed, must start with start_object", exception.getMessage());
        }

        source = "{}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(source)) {
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.STRICT);
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->  context.parseInnerQueryBuilder());
            assertEquals("query malformed, empty clause found at [1:2]", exception.getMessage());
            final List<String> warnings = threadContext.getResponseHeaders().get(DeprecationLogger.DEPRECATION_HEADER);
            assertThat(warnings, hasSize(1));
            assertThat(warnings, hasItem(equalTo("query malformed, empty clause found at [1:2]")));
        }

        source = "{ \"foo\" : \"bar\" }";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(source)) {
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.STRICT);
            ParsingException exception = expectThrows(ParsingException.class, () ->  context.parseInnerQueryBuilder());
            assertEquals("[foo] query malformed, no start_object after query name", exception.getMessage());
        }

        source = "{ \"foo\" : {} }";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(source)) {
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.STRICT);
            ParsingException exception = expectThrows(ParsingException.class, () ->  context.parseInnerQueryBuilder());
            assertEquals("no [query] registered for [foo]", exception.getMessage());
        }
        final List<String> warnings = threadContext.getResponseHeaders().get(DeprecationLogger.DEPRECATION_HEADER);
        assertThat(warnings, hasSize(1));
        assertThat(warnings, hasItem(equalTo("query malformed, empty clause found at [1:2]")));
    }

}
