/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.Map;

/**
 * A JUnit rule that logs the profile information from an ESQL response if the test fails.
 * <p>
 *     Usage:
 * </p>
 * <pre>
 * {@code
 *     @Rule(order = Integer.MIN_VALUE)
 *     public ProfileLogger profileLogger = new ProfileLogger();
 *
 *     public void test() {
 *         var response = RestEsqlTestCase.runEsql(requestObject, assertWarnings, profileLogger, mode);
 *         // Or any of the other runEsql methods
 *     }
 * }
 * </pre>
 */
public class ProfileLogger extends TestWatcher {
    private static final Logger LOGGER = LogManager.getLogger(ProfileLogger.class);

    private Object profile;

    public void extractProfile(Map<String, Object> jsonResponse, Boolean originalProfileParameter) {
        if (jsonResponse.containsKey("profile") == false) {
            return;
        }

        profile = jsonResponse.get("profile");

        if (Boolean.TRUE.equals(originalProfileParameter) == false) {
            jsonResponse.remove("profile");
        }
    }

    public void clearProfile() {
        profile = null;
    }

    @Override
    protected void failed(Throwable e, Description description) {
        LOGGER.info("Profile: {}", profile);
    }
}
