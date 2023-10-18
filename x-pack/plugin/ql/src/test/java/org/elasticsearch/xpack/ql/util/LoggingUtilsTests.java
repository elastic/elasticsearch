/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.http.HttpHeadersValidationException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.junit.Before;
import org.mockito.Mockito;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.util.LoggingUtils.logQueryFailure;
import static org.mockito.Mockito.verify;

public class LoggingUtilsTests extends ESTestCase {
    Logger logger;
    String queryName;
    String exceptionText;
    String causeExceptionText;
    String suppressedExceptionText;
    String preamble;

    @Before
    public void init() {
        logger = Mockito.mock(Logger.class);
        queryName = "queryName_" + randomAlphaOfLength(10);
        exceptionText = "exceptionTest_" + randomAlphaOfLength(10);
        causeExceptionText = null;
        suppressedExceptionText = null;
        preamble = randomBoolean() ? ("preamble_" + randomAlphaOfLength(10)) : null;

    }

    public void testNonElasticsearchException() {
        Exception e = new IllegalArgumentException(exceptionText);
        logQueryFailure(logger, queryName, e, preamble);
        verifyInvocation(e, Level.INFO);
    }

    public void testElasticsearchUserException() {
        Exception e = new IndexNotFoundException(exceptionText);
        exceptionText = "no such index [" + exceptionText + "]";
        logQueryFailure(logger, queryName, e, preamble);
        verifyInvocation(e, Level.INFO);
    }

    public void testElasticsearchServerException() {
        Exception e = new QlIllegalArgumentException(exceptionText);
        logQueryFailure(logger, queryName, e, preamble);
        verifyInvocation(e, Level.ERROR);
    }

    public void testWithCause() {
        Exception cause = new IndexNotFoundException(exceptionText + "_cause");
        causeExceptionText = "no such index [" + exceptionText + "_cause]";
        Exception e = new QlIllegalArgumentException(cause, exceptionText);
        logQueryFailure(logger, queryName, e, preamble);
        verifyInvocation(e, Level.ERROR);
    }

    public void testWithNestedCause() {
        Exception e = new IndexNotFoundException(exceptionText + "_cause");
        e = new QlIllegalArgumentException(e, exceptionText);
        causeExceptionText = exceptionText; // QlIllegalArgumentException instanceof ElasticsearchWrapperException == false
        exceptionText = e.getClass().getCanonicalName() + ": " + exceptionText;

        final int levels = randomIntBetween(1, 9);
        for (int i = 0; i < levels; i++) {
            e = new HttpHeadersValidationException(e); // is instanceof ElasticsearchWrapperException
            if (i != 0) { // HttpHeadersValidationException stacks up it's message
                exceptionText = e.getClass().getCanonicalName() + ": " + exceptionText;
            }
        }
        logQueryFailure(logger, queryName, e, preamble);
        verifyInvocation(e, Level.ERROR);
    }

    public void testWithSuppressed() {
        Exception suppressed = new IndexNotFoundException(exceptionText + "_suppressed");
        Exception e = new IllegalArgumentException(exceptionText);
        e.addSuppressed(suppressed);
        suppressedExceptionText = "no such index [" + exceptionText + "_suppressed]";
        logQueryFailure(logger, queryName, e, preamble);
        verifyInvocation(e, Level.INFO);
    }

    public void testWithCauseAndSuppressed() {
        Exception suppressed = new IndexNotFoundException(exceptionText + "_suppressed");
        suppressedExceptionText = "no such index [" + exceptionText + "_suppressed]";
        Exception cause = new IndexNotFoundException(exceptionText + "_cause");
        causeExceptionText = "no such index [" + exceptionText + "_cause]";

        Exception e = new QlIllegalArgumentException(cause, exceptionText);
        e.addSuppressed(suppressed);

        logQueryFailure(logger, queryName, e, preamble);
        verifyInvocation(e, Level.ERROR);
    }

    private void verifyInvocation(Throwable t, Level level) {
        String expected = messageOnlyThrowable(t);
        if (t.getCause() != null) {
            expected += "\n" + messageOnlyCause(ExceptionsHelper.unwrapCause(t.getCause()));
        }
        for (Throwable s : t.getSuppressed()) {
            expected += "\n" + messageOnlySuppressed(s);
        }
        verify(logger).log(level, expected);
    }

    private String messageOnlyThrowable(Throwable t) {
        return format(
            null,
            "{} {}: {}: {}\n{}",
            queryName,
            preamble == null ? "failed" : preamble,
            t.getClass().getCanonicalName(),
            exceptionText,
            ExceptionsHelper.formatStackTrace(t.getStackTrace())
        );
    }

    private String messageOnlyCause(Throwable e) {
        return format(
            null,
            "caused by: {}: {}\n{}",
            e.getClass().getCanonicalName(),
            causeExceptionText,
            ExceptionsHelper.formatStackTrace(e.getStackTrace())
        );
    }

    private String messageOnlySuppressed(Throwable e) {
        return format(
            null,
            "suppressing: {}: {}\n{}",
            e.getClass().getCanonicalName(),
            suppressedExceptionText,
            ExceptionsHelper.formatStackTrace(e.getStackTrace())
        );
    }
}
