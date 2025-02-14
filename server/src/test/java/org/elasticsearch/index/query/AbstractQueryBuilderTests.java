/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.hamcrest.Matchers.containsString;

public class AbstractQueryBuilderTests extends ESTestCase {

    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        xContentRegistry = new NamedXContentRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents());
    }

    @AfterClass
    public static void cleanup() {
        xContentRegistry = null;
    }

    public void testParseInnerQueryBuilder() throws IOException {
        QueryBuilder query = new MatchQueryBuilder("foo", "bar");
        String source = query.toString();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            QueryBuilder actual = parseInnerQueryBuilder(parser);
            assertEquals(query, actual);
        }
    }

    public void testParseInnerQueryBuilderExceptions() throws IOException {
        String source = "{ \"foo\": \"bar\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            parser.nextToken();
            parser.nextToken(); // don't start with START_OBJECT to provoke exception
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("[_na] query malformed, must start with start_object", exception.getMessage());
        }

        source = "{}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("query malformed, empty clause found at [1:2]", exception.getMessage());
        }

        source = "{ \"foo\" : \"bar\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("[foo] query malformed, no start_object after query name", exception.getMessage());
        }

        source = "{ \"boool\" : {} }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("unknown query [boool] did you mean [bool]?", exception.getMessage());
        }
        source = "{ \"match_\" : {} }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("unknown query [match_] did you mean any of [match, match_all, match_none]?", exception.getMessage());
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public void testMaybeConvertToBytesRefLongTerm() {
        String longTerm = "a".repeat(IndexWriter.MAX_TERM_LENGTH + 1);
        Exception e = expectThrows(IllegalArgumentException.class, () -> AbstractQueryBuilder.maybeConvertToBytesRef(longTerm));
        assertThat(e.getMessage(), containsString("term starting with [aaaaa"));
    }

    public void testMaybeConvertToBytesRefStringCorrectSize() {
        int capacity = randomIntBetween(20, 40);
        StringBuilder termBuilder = new StringBuilder(capacity);
        int correctSize = 0;
        for (int i = 0; i < capacity; i++) {
            if (i < capacity / 3) {
                termBuilder.append((char) randomIntBetween(0, 127));
                ++correctSize; // use only one byte for char < 128
            } else if (i < 2 * capacity / 3) {
                termBuilder.append((char) randomIntBetween(128, 2047));
                correctSize += 2; // use two bytes for char < 2048
            } else {
                termBuilder.append((char) randomIntBetween(2048, 4092));
                correctSize += 3; // use three bytes for char >= 2048
            }
        }
        BytesRef bytesRef = (BytesRef) AbstractQueryBuilder.maybeConvertToBytesRef(termBuilder.toString());
        assertEquals(correctSize, bytesRef.bytes.length);
        assertEquals(correctSize, bytesRef.length);
    }

}
