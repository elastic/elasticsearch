/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.NodeNamePatternConverter;

public final class Utils {

    private Utils() {
        // utility class
    }

    static {
        LogConfigurator.setClusterName("elasticsearch-benchmark");
        LogConfigurator.setNodeName("test");
    }

    public static void configureBenchmarkLogging() {
        LogConfigurator.loadLog4jPlugins();
        NodeNamePatternConverter.setGlobalNodeName("benchmark");
        LogConfigurator.configureESLogging();
    }

}
