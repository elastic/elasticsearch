/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.client;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.client.RemoteFailure;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Locale;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class RemoteFailureTests extends ESTestCase {
    public void testParseBasic() throws IOException {
        RemoteFailure failure = parse("basic.json");
        assertEquals("illegal_argument_exception", failure.type());
        assertEquals("[sql/query] unknown field [test], parser not found", failure.reason());
        assertThat(failure.remoteTrace(),
                containsString("at org.elasticsearch.common.xcontent.ObjectParser.getParser(ObjectParser.java:346)"));
        assertNull(failure.cause());
        assertEquals(emptyMap(), failure.headers());
    }

    public void testParseNested() throws IOException {
        RemoteFailure failure = parse("nested.json");
        assertEquals("parsing_exception", failure.type());
        assertEquals("line 1:1: no viable alternative at input 'test'", failure.reason());
        assertThat(failure.remoteTrace(),
                containsString("at org.elasticsearch.xpack.sql.parser.SqlParser$1.syntaxError(SqlParser.java:151)"));
        assertNotNull(failure.cause());

        failure = failure.cause();
        assertEquals("no_viable_alt_exception", failure.type());
        assertEquals(null, failure.reason());
        assertThat(failure.remoteTrace(),
                containsString("at org.antlr.v4.runtime.atn.ParserATNSimulator.noViableAlt(ParserATNSimulator.java:1886)"));
        assertNull(failure.cause());
        assertEquals(emptyMap(), failure.headers());
    }

    public void testParseMissingAuth() throws IOException {
        RemoteFailure failure = parse("missing_auth.json");
        assertEquals("security_exception", failure.type());
        assertEquals("missing authentication token for REST request [/?pretty&error_trace]", failure.reason());
        assertThat(failure.remoteTrace(),
                containsString("DefaultAuthenticationFailureHandler.missingToken"));
        assertNull(failure.cause());
        assertEquals(singletonMap("WWW-Authenticate", "Basic realm=\"security\" charset=\"UTF-8\""),
                failure.headers());
    }

    public void testNoError() {
        IOException e = expectThrows(IOException.class, () -> parse("no_error.json"));
        assertEquals(
                "Can't parse error from Elasticsearch [Expected [error] but didn't see it.] at [line 1 col 2]. Response:\n{}",
            e.getMessage());
    }

    public void testBogusError() {
        IOException e = expectThrows(IOException.class, () -> parse("bogus_error.json"));
        assertEquals(
            "Can't parse error from Elasticsearch [Expected [error] to be an object or string but was [START_ARRAY][[]] "
                + "at [line 1 col 12]. Response:\n"
                + "{ \"error\": [\"bogus\"] }",
            e.getMessage());
    }

    public void testNoStack() {
        IOException e = expectThrows(IOException.class, () -> parse("no_stack.json"));
        assertThat(e.getMessage(),
            startsWith("Can't parse error from Elasticsearch [expected [stack_trace] cannot but "
                + "didn't see it] at [line 5 col 3]. Response:\n{"));
    }

    public void testNoType() {
        IOException e = expectThrows(IOException.class, () -> parse("no_type.json"));
        assertThat(e.getMessage(),
            startsWith("Can't parse error from Elasticsearch [expected [type] but didn't see it] at [line 5 col 3]. Response:\n{"));
    }

    public void testInvalidJson() {
        IOException e = expectThrows(IOException.class, () -> parse("invalid_json.txt"));
        assertEquals(
            "Can't parse error from Elasticsearch [Unrecognized token 'I': "
                + "was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')] "
                + "at [line 1 col 1]. Response:\n"
                + "I'm not json at all",
            e.getMessage());
    }

    public void testExceptionBuildingParser() {
        IOException e = expectThrows(IOException.class, () -> RemoteFailure.parseFromResponse(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("Testing error");
            }
        }));
        assertEquals(
            "Can't parse error from Elasticsearch [Testing error]. Attempted to include response but failed because [Testing error].",
            e.getMessage());
    }

    public void testTotalGarbage() {
        IOException e = expectThrows(IOException.class, () ->
            RemoteFailure.parseFromResponse(new BytesArray(new byte[] {
                (byte) 0xEF, (byte) 0xBB, (byte) 0xBF, // The UTF-8 BOM
                (byte) 0xFF // An invalid UTF-8 character
            }).streamInput()));
        assertThat(e.getMessage(),
            startsWith("Can't parse error from Elasticsearch [Unrecognized token 'ÿ': "
                + "was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')] "
                + "at [line 1 col 4]. Response:\n"));
    }

    public void testTooBig() {
        StringBuilder tooBig = new StringBuilder(RemoteFailure.MAX_RAW_RESPONSE);
        tooBig.append("{\n");
        tooBig.append("\"error\" : {\n");
        tooBig.append("  \"type\" : \"illegal_argument_exception\",\n");
        tooBig.append("  \"reason\" : \"something\",\n");
        tooBig.append("  \"header\" : {\n");
        int i = 0;
        while (tooBig.length() < RemoteFailure.MAX_RAW_RESPONSE) {
            tooBig.append("    \"").append(String.format(Locale.ROOT, "%04d", i++))
                .append("\" : \"lots and lots and lots and lots and lots of words\",\n");
        }
        tooBig.append("    \"end\" : \"lots and lots and lots and lots and lots of words\"\n");
        tooBig.append("  }\n");
        tooBig.append("}\n");
        IOException e = expectThrows(IOException.class, () ->
            RemoteFailure.parseFromResponse(new BytesArray(tooBig.toString()).streamInput()));
        assertEquals(
            "Can't parse error from Elasticsearch [expected [stack_trace] cannot but didn't see it] "
                + "at [line 7951 col 1]. Attempted to include response but failed because [Response too large].",
            e.getMessage());
    }

    public void testFailureWithMetadata() throws IOException {
        final StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"error\":{");
        json.append("    \"root_cause\":[],");
        json.append("    \"type\":\"search_phase_execution_exception\",");
        json.append("    \"reason\":\"all shards failed\",");
        json.append("    \"phase\":\"query\",");
        json.append("    \"grouped\":true,");
        json.append("    \"failed_shards\":[],");
        json.append("    \"stack_trace\":\"Failed to execute phase [query], all shards failed at...\"");
        json.append("  },");
        json.append("  \"status\":503");
        json.append("}");

        RemoteFailure failure = RemoteFailure.parseFromResponse(new BytesArray(json.toString()).streamInput());
        assertEquals("search_phase_execution_exception", failure.type());
        assertEquals("all shards failed", failure.reason());
        assertThat(failure.remoteTrace(), containsString("Failed to execute phase [query], all shards failed"));
        assertNull(failure.cause());
        assertEquals(emptyMap(), failure.headers());
        assertNotNull(failure.metadata());
        assertEquals(1, failure.metadata().size());
        assertEquals(singletonList("query"), failure.metadata().get("phase"));
    }

    public void testFailureWithMetadataAndRootCause() throws IOException {
        final StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"error\":{");
        json.append("    \"caused_by\":{");
        json.append("        \"type\":\"invalid_index_name_exception\",");
        json.append("        \"reason\":\"Invalid index name [_root], must not start with '_'.\",");
        json.append("        \"index_uuid\":\"_na_root\",");
        json.append("        \"index\":[\"_root\",\"_foo\"],");
        json.append("        \"stack_trace\":\"[_root] InvalidIndexNameException[Invalid index name [_root], must not start with '_'.]\"");
        json.append("    },");
        json.append("    \"type\":\"invalid_index_name_exception\",");
        json.append("    \"reason\":\"Invalid index name [_foo], must not start with '_'.\",");
        json.append("    \"index_uuid\":\"_na_foo\",");
        json.append("    \"index\":[\"_foo\"],");
        json.append("    \"stack_trace\":\"[_foo] InvalidIndexNameException[Invalid index name [_foo], must not start with '_'.]\"");
        json.append("  },");
        json.append("  \"status\":400");
        json.append("}");

        RemoteFailure failure = RemoteFailure.parseFromResponse(new BytesArray(json.toString()).streamInput());
        assertEquals("invalid_index_name_exception", failure.type());
        assertEquals("Invalid index name [_foo], must not start with '_'.", failure.reason());
        assertThat(failure.remoteTrace(),
                containsString("[_foo] InvalidIndexNameException[Invalid index name [_foo], must not start with '_'.]"));
        assertEquals(emptyMap(), failure.headers());
        assertNotNull(failure.metadata());
        assertEquals(2, failure.metadata().size());
        assertEquals(singletonList("_na_foo"), failure.metadata().get("index_uuid"));
        assertEquals(singletonList("_foo"), failure.metadata().get("index"));

        RemoteFailure cause = failure.cause();
        assertEquals("invalid_index_name_exception", cause.type());
        assertEquals("Invalid index name [_root], must not start with '_'.", cause.reason());
        assertThat(cause.remoteTrace(),
                containsString("[_root] InvalidIndexNameException[Invalid index name [_root], must not start with '_'.]"));
        assertEquals(emptyMap(), failure.headers());
        assertNotNull(cause.metadata());
        assertEquals(2, cause.metadata().size());
        assertEquals(singletonList("_na_root"), cause.metadata().get("index_uuid"));
        assertEquals(Arrays.asList("_root", "_foo"), cause.metadata().get("index"));
    }

    private RemoteFailure parse(String fileName) throws IOException {
        try (InputStream in = Files.newInputStream(getDataPath("/remote_failure/" + fileName))) {
            return RemoteFailure.parseFromResponse(in);
        }
    }
}
