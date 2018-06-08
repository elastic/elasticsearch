/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.plugin.AbstractSqlQueryRequest;
import org.elasticsearch.xpack.sql.proto.Protocol;

import java.util.TimeZone;

// Typed object holding properties for a given action
public class Configuration {
    public static final Configuration DEFAULT = new Configuration(TimeZone.getTimeZone("UTC"),
        Protocol.FETCH_SIZE, Protocol.REQUEST_TIMEOUT, Protocol.PAGE_TIMEOUT, null);

    private TimeZone timeZone;
    private int pageSize;
    private TimeValue requestTimeout;
    private TimeValue pageTimeout;

    @Nullable
    private QueryBuilder filter;

    public Configuration(TimeZone tz, int pageSize, TimeValue requestTimeout, TimeValue pageTimeout, QueryBuilder filter) {
        this.timeZone = tz;
        this.pageSize = pageSize;
        this.requestTimeout = requestTimeout;
        this.pageTimeout = pageTimeout;
        this.filter = filter;
    }

    public TimeZone timeZone() {
        return timeZone;
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
}
