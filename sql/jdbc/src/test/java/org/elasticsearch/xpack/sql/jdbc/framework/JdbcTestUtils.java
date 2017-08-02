/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.sql.util.CollectionUtils;

import java.util.Map;
import java.util.Map.Entry;

public abstract class JdbcTestUtils {

    public static void sqlLogging() {
        String t = "TRACE";
        String d = "DEBUG";
        
        Map<String, String> of = CollectionUtils.of("org.elasticsearch.xpack.sql.parser", t, 
                "org.elasticsearch.xpack.sql.analysis.analyzer", t, 
                "org.elasticsearch.xpack.sql.optimizer", t, 
                "org.elasticsearch.xpack.sql.rule", t, 
                "org.elasticsearch.xpack.sql.planner", t,
                "org.elasticsearch.xpack.sql.execution.search", t);
        
        for (Entry<String, String> entry : of.entrySet()) {
            Loggers.setLevel(Loggers.getLogger(entry.getKey()), entry.getValue());
        }
    }
}
