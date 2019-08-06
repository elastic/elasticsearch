/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;

import java.time.ZoneId;

public class Context {

    private static final ThreadLocal<Context> CURRENT_CONTEXT = new ThreadLocal<>();

    public static Context getCurrentContext() {
        Context current = CURRENT_CONTEXT.get();
        if (current == null) {
            throw new SqlIllegalArgumentException("Cannot determine current SQL context; this is likely a bug");
        }
        return current;
    }

    static void setCurrentContext(Context context) {
        Context current = CURRENT_CONTEXT.get();
        if (current != null) {
            throw new SqlIllegalArgumentException("Current SQL context already set; this is likely a bug");
        }
        CURRENT_CONTEXT.set(context);
    }

    static void cleanCurrentContext() {
        CURRENT_CONTEXT.remove();
    }

    /**
     * Per-request specific settings needed in some of the functions (timezone, username and clustername),
     * to which they are attached.
     */
    private final Configuration configuration;

    /**
     * Information about the indices against which the SQL is being analyzed.
     */
    private final IndexResolution resolution;

    public Context(Configuration configuration, IndexResolution resolution) {
        this.configuration = configuration;
        this.resolution = resolution;
    }

    public Configuration configuration() {
        return configuration;
    }

    public IndexResolution indexResolution() {
        return resolution;
    }

    public ZoneId zoneId() {
        return configuration.zoneId();
    }
}
