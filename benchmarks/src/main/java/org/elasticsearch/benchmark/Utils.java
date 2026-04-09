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
import org.openjdk.jmh.annotations.Param;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    public static List<String> possibleValues(Class<?> clazz, String field) {
        List<String> result = new ArrayList<>();
        try {
            Field f = clazz.getField(field);
            Param[] paramAnns = f.getAnnotationsByType(Param.class);
            if (paramAnns.length == 0) {
                throw new AssertionError("missing @Param on " + clazz.getName() + "#" + field);
            }
            for (Param param : paramAnns) {
                Collections.addAll(result, param.value());
            }
            for (ExtraParam param : f.getAnnotationsByType(ExtraParam.class)) {
                Collections.addAll(result, param.value());
            }
            return result;
        } catch (NoSuchFieldException e) {
            AssertionError assertionError = new AssertionError("unknown field " + clazz.getName() + "#" + field);
            assertionError.initCause(e);
            throw assertionError;
        }
    }
}
