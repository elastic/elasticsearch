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

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ParseExceptionUtilsTests extends ESTestCase {

    public void testUnwrap_ReturnsSame_WhenThrowableIsRuntimeException() {
        var runtimeException = new IllegalStateException("runtime");
        var result = ParseExceptionUtils.unwrapXContentParseException(runtimeException);
        assertThat(result, sameInstance(runtimeException));
    }

    public void testUnwrap_WrapsInRuntimeException_WhenThrowableIsCheckedException() {
        var ioException = new IOException("io");
        var result = ParseExceptionUtils.unwrapXContentParseException(ioException);
        assertThat(result, instanceOf(RuntimeException.class));
        assertThat(result.getCause(), sameInstance(ioException));
        assertThat(result.getMessage(), is("java.io.IOException: io"));
    }

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
}
