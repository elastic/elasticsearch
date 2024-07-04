/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa.matchers;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.logsdb.qa.exceptions.MatcherException;
import org.elasticsearch.xcontent.XContentBuilder;

/**
 * A base class to be used for the matching logic when comparing query results.
 */
public abstract class ResponseMatcher<T> {

    public static class Builder<T> {

        protected final XContentBuilder oracleMappings;
        protected final Settings.Builder oracleSettings;
        protected final XContentBuilder challengeMappings;
        protected final Settings.Builder challengeSettings;
        private T response;

        public Builder(
            final XContentBuilder oracleMappings,
            final Settings.Builder oracleSettings,
            final XContentBuilder challengeMappings,
            final Settings.Builder challengeSettings
        ) {
            this.oracleMappings = oracleMappings;
            this.oracleSettings = oracleSettings;
            this.challengeMappings = challengeMappings;
            this.challengeSettings = challengeSettings;
        }

        public Builder<T> with(final T response) {
            this.response = response;
            return this;
        }

        public void equalTo(final T challenge, final ResponseMatcher<T> matcher) throws MatcherException {
            matcher.match(this.response, challenge);
        }
    }

    public abstract void match(T a, T b) throws MatcherException;
}
