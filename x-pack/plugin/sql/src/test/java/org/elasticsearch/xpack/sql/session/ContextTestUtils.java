/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;

public class ContextTestUtils {

    public static void setContext(Configuration cfg, IndexResolution resolution) {
        Context.setCurrentContext(new Context(cfg, resolution));
    }

    public static void clearContext() {
        Context.cleanCurrentContext();
    }
}
