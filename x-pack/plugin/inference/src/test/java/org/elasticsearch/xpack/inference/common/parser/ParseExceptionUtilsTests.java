/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.common.parser.ParseExceptionUtils.MAX_UNWRAP_DEPTH;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class ParseExceptionUtilsTests extends ESTestCase {

    public void testUnwrap_ReturnsSame_WhenXContentParseExceptionHasNullCause() {
        var parseException = new XContentParseException("parse failed");
        var result = ParseExceptionUtils.unwrapXContentParseException(parseException);
        assertThat(result, sameInstance(parseException));
    }

    public void testUnwrap_ReturnsCause_WhenXContentParseExceptionWrapsRuntimeException() {
        var rootCause = new IllegalArgumentException("root");
        var parseException = new XContentParseException(null, "parse failed", rootCause);
        var result = ParseExceptionUtils.unwrapXContentParseException(parseException);
        assertThat(result, sameInstance(rootCause));
    }

    public void testUnwrap_WrapsCause_WhenXContentParseExceptionWrapsCheckedException() {
        var rootCause = new IOException("root");
        var parseException = new XContentParseException(null, "parse failed", rootCause);
        var result = ParseExceptionUtils.unwrapXContentParseException(parseException);
        assertThat(result, instanceOf(RuntimeException.class));
        assertThat(result.getCause(), sameInstance(rootCause));
    }

    public void testUnwrap_UnwrapsChain_ToNonXContentParseException() {
        var rootCause = new IllegalStateException("root");
        var inner = new XContentParseException(null, "inner", rootCause);
        var outer = new XContentParseException(null, "outer", inner);
        var result = ParseExceptionUtils.unwrapXContentParseException(outer);
        assertThat(result, sameInstance(rootCause));
    }

    /**
     * Tests that unwrapping stops after 10 levels and returns the exception at that depth.
     * With {@code counter++ >= 10}, 10 getCause() steps are allowed; a chain of 11
     * XContentParseExceptions causes the 11th (index 10) to be returned when the limit is hit.
     */
    public void testUnwrap_StopsAtTenLevels_ReturnsExceptionAtLimit() {
        var innermost = new XContentParseException("level 10");
        var chain = new XContentParseException[MAX_UNWRAP_DEPTH + 1];

        var lastIndex = chain.length - 1;
        chain[lastIndex] = innermost;

        var secondToLastIndex = lastIndex - 1;
        for (var i = secondToLastIndex; i >= 0; i--) {
            chain[i] = new XContentParseException(null, "level " + i, chain[i + 1]);
        }
        var result = ParseExceptionUtils.unwrapXContentParseException(chain[0]);
        assertThat(result, sameInstance(innermost));
    }
}
