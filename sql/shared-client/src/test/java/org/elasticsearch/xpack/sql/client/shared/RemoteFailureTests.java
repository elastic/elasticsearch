/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client.shared;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyMap;
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

    public void testNoError() throws IOException {
        IOException e = expectThrows(IOException.class, () -> parse("no_error.json"));
        assertEquals(
            "Can't parse error from Elasticearch [Expected [error] but didn't see it.] at [line 2 col 1]. Response:\n"
                + "{\n}\n",
            e.getMessage());
    }

    public void testBogusError() throws IOException {
        IOException e = expectThrows(IOException.class, () -> parse("bogus_error.json"));
        assertEquals(
            "Can't parse error from Elasticearch [Expected [error] to be an object but was [VALUE_STRING][bogus]] "
                + "at [line 2 col 12]. Response:\n"
                + "{\n  \"error\": \"bogus\"\n}",
            e.getMessage());
    }

    public void testNoStack() throws IOException {
        IOException e = expectThrows(IOException.class, () -> parse("no_stack.json"));
        assertThat(e.getMessage(),
            startsWith("Can't parse error from Elasticearch [expected [stack_trace] cannot but "
                + "didn't see it] at [line 5 col 3]. Response:\n{"));
    }

    public void testNoType() throws IOException {
        IOException e = expectThrows(IOException.class, () -> parse("no_type.json"));
        assertThat(e.getMessage(),
            startsWith("Can't parse error from Elasticearch [expected [type] but didn't see it] at [line 5 col 3]. Response:\n{"));
    }

    public void testInvalidJson() throws IOException {
        IOException e = expectThrows(IOException.class, () -> parse("invalid_json.txt"));
        assertEquals(
            "Can't parse error from Elasticearch [Unrecognized token 'I': was expecting 'null', 'true', 'false' or NaN] "
                + "at [line 1 col 1]. Response:\n"
                + "I'm not json at all\n",
            e.getMessage());
    }

    public void testExceptionBuildingParser() throws IOException {
        IOException e = expectThrows(IOException.class, () -> RemoteFailure.parseFromResponse(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("Testing error");
            }
        }));
        assertEquals(
            "Can't parse error from Elasticearch [Testing error]. Attempted to include response but failed because [Testing error].",
            e.getMessage());
    }

    public void testTotalGarbage() throws IOException {
        IOException e = expectThrows(IOException.class, () ->
            RemoteFailure.parseFromResponse(new BytesArray(new byte[] {
                (byte) 0xEF, (byte) 0xBB, (byte) 0xBF, // The UTF-8 BOM
                (byte) 0xFF // An invalid UTF-8 character
            }).streamInput()));
        assertThat(e.getMessage(),
            startsWith("Can't parse error from Elasticearch [Unrecognized token 'ÿ': "
                + "was expecting ('true', 'false' or 'null')] at [line 1 col 1]. Response:\n"));
    }

    private RemoteFailure parse(String fileName) throws IOException {
        try (InputStream in = Files.newInputStream(getDataPath("/remote_failure/" + fileName))) {
            return RemoteFailure.parseFromResponse(in);
        }
    }
}
