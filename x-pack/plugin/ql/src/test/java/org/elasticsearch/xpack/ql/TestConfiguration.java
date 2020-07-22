/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql;

import org.elasticsearch.xpack.ql.session.Configuration;

import java.time.ZoneId;


public final class TestConfiguration extends Configuration {

    private final boolean isCaseSensitive;

    TestConfiguration(ZoneId zi, String username, String clusterName) {
        super(zi, username, clusterName);
        this.isCaseSensitive = true;
    }

    TestConfiguration(ZoneId zi, String username, String clusterName, boolean isCaseSensitive) {
        super(zi, username, clusterName);
        this.isCaseSensitive = isCaseSensitive;
    }

    public boolean isCaseSensitive() {
        return isCaseSensitive;
    }
}
