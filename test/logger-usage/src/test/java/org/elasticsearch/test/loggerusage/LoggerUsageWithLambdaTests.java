/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.loggerusage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.core.Strings.format;

public class LoggerUsageWithLambdaTests extends ESTestCase {
    Logger logger = LogManager.getLogger(LoggerUsageWithLambdaTests.class);
    public void testFailArraySize(String... arr) {
        logger.debug(format("text %s", (Object[]) arr));
    }

    public void testNumberOfArgumentsParameterizedMessage1() {
        logger.info(format("Hello %s, %s, %s", "world", 2, "third argument"));
    }

    public void testFailNumberOfArgumentsParameterizedMessage1() {
        logger.info(format("Hello %s, %s", "world", 2, "third argument"));
    }

    public void testNumberOfArgumentsParameterizedMessage2() {
        logger.info(format("Hello %s, %s", "world", 2));
    }

    public void testFailNumberOfArgumentsParameterizedMessage2() {
        logger.info(format("Hello %s, %s, %s", "world", 2));
    }

    public void testNumberOfArgumentsParameterizedMessage3() {
        logger.info(() -> format("Hello %s, %s, %s", "world", 2, "third argument"));
    }


    public void testOrderOfExceptionArgument1() {
        logger.info(() -> format("Hello %s", "world"), new Exception());
    }

// TODO additional parameters do no make format to fail
//
//    public void testFailNumberOfArgumentsParameterizedMessage3() {
//        logger.info(() -> format("Hello %s, %s", "world", 2, "third argument"));
//    }

// TODO additional parameters do no make format to fail
//
//    public void testFailNonConstantMessageWithArguments(boolean b) {
//        logger.info(() -> format(Boolean.toString(b), 42), new Exception());
//    }

}
