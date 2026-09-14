/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.Map;

/**
 * A JUnit rule that logs the profile information for the last ESQL query of a test when the test fails.
 * <p>
 *     Queries are not run with {@code profile} enabled by default (a profile can be several megabytes for a
 *     wide query, which is pure overhead when the test passes). Instead, when a test fails, the query is
 *     re-issued with {@code profile:true} via {@link #setProfileFetcher} so the profile can be logged for
 *     debugging. Tests that explicitly request a profile keep it in-band, and that profile is logged directly.
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
    private CheckedSupplier<Object, Exception> profileFetcher;

    public void extractProfile(Map<String, Object> jsonResponse, Boolean originalProfileParameter) {
        if (jsonResponse.containsKey("profile") == false) {
            return;
        }

        profile = jsonResponse.get("profile");

        if (Boolean.TRUE.equals(originalProfileParameter) == false) {
            jsonResponse.remove("profile");
        }
    }

    /**
     * Registers how to obtain the profile of the last query on demand (by replaying it with {@code profile:true}),
     * used only if the test fails and the profile was not already captured in-band.
     */
    public void setProfileFetcher(CheckedSupplier<Object, Exception> profileFetcher) {
        this.profileFetcher = profileFetcher;
    }

    public void clearProfile() {
        profile = null;
        profileFetcher = null;
    }

    /**
     * Returns the profile of the last query, fetching it on demand (via {@link #setProfileFetcher}) if it was not
     * captured in-band. Returns {@code null} if no profile is available. Used on failure to log the profile, and
     * directly by tests that exercise the on-demand path.
     */
    public Object fetchProfileOnDemand() throws Exception {
        if (profile != null) {
            return profile;
        }
        return profileFetcher == null ? null : profileFetcher.get();
    }

    @Override
    protected void failed(Throwable e, Description description) {
        Object toLog = null;
        try {
            toLog = fetchProfileOnDemand();
        } catch (Exception ex) {
            LOGGER.info("Could not fetch profile via replay", ex);
        }
        LOGGER.info("Profile: {}", toLog);
    }
}
