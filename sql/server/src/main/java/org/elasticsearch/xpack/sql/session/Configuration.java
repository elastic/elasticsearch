/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.sql.plugin.sql.action.AbstractSqlRequest;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractQueryInitRequest;
import org.joda.time.DateTimeZone;

// Typed object holding properties for a given 
public class Configuration {
    public static final Configuration DEFAULT = new Configuration(DateTimeZone.UTC, 
                                AbstractQueryInitRequest.DEFAULT_FETCH_SIZE,
                                AbstractSqlRequest.DEFAULT_REQUEST_TIMEOUT,
                                AbstractSqlRequest.DEFAULT_PAGE_TIMEOUT);

    private DateTimeZone timeZone;
    private int pageSize;
    private TimeValue requestTimeout;
    private TimeValue pageTimeout;

    public Configuration(DateTimeZone tz, int pageSize, TimeValue requestTimeout, TimeValue pageTimeout) {
        this.timeZone = tz;
        this.pageSize = pageSize;
        this.requestTimeout = requestTimeout;
        this.pageTimeout = pageTimeout;
    }

    public DateTimeZone timeZone() {
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
}
