/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.grok.MatcherWatchdog;

public class EsqlConfig {

    // versioning information
    private final boolean isDevVersion;
    private final MatcherWatchdog grokMatcherWatchdog;

    public EsqlConfig(boolean isDevVersion, MatcherWatchdog grokMatcherWatchdog) {
        this.isDevVersion = isDevVersion;
        this.grokMatcherWatchdog = grokMatcherWatchdog;
    }

    public EsqlConfig(boolean isDevVersion) {
        this(isDevVersion, MatcherWatchdog.newInstance(1000));
    }

    public EsqlConfig() {
        this(Build.current().isSnapshot());
    }

    public EsqlConfig(MatcherWatchdog grokMatcherWatchdog) {
        this(Build.current().isSnapshot(), grokMatcherWatchdog);
    }

    public boolean isDevVersion() {
        return isDevVersion;
    }

    public MatcherWatchdog grokMatcherWatchdog() {
        return grokMatcherWatchdog;
    }
}
