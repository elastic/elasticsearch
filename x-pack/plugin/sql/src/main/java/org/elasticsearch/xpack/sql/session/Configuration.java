/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.proto.Mode;

import java.time.ZoneId;

// Typed object holding properties for a given query
public class Configuration extends org.elasticsearch.xpack.ql.session.Configuration {
    
    private final int pageSize;
    private final TimeValue requestTimeout;
    private final TimeValue pageTimeout;
    private final Mode mode;
    private final String clientId;
    private final boolean multiValueFieldLeniency;
    private final boolean includeFrozenIndices;

    @Nullable
    private QueryBuilder filter;

    public Configuration(ZoneId zi, int pageSize, TimeValue requestTimeout, TimeValue pageTimeout, QueryBuilder filter,
                         Mode mode, String clientId,
                         String username, String clusterName,
                         boolean multiValueFieldLeniency,
                         boolean includeFrozen) {

        super(zi, username, clusterName);

        this.pageSize = pageSize;
        this.requestTimeout = requestTimeout;
        this.pageTimeout = pageTimeout;
        this.filter = filter;
        this.mode = mode == null ? Mode.PLAIN : mode;
        this.clientId = clientId;
        this.multiValueFieldLeniency = multiValueFieldLeniency;
        this.includeFrozenIndices = includeFrozen;
    }

    public int pageSize() {
        return pageSize;
    }

    public TimeValue requestTimeout() {
        return requestTimeout;
    }

    public TimeValue pageTimeout() {
        return pageTimeout;
    }

    public QueryBuilder filter() {
        return filter;
    }
    public Mode mode() {
        return mode;
    }

    public String clientId() {
        return clientId;
    }

    public boolean multiValueFieldLeniency() {
        return multiValueFieldLeniency;
    }

    public boolean includeFrozen() {
        return includeFrozenIndices;
    }
}