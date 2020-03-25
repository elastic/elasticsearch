/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class ProcessBuilderUtils {

    private ProcessBuilderUtils() {}

    /**
     * Name of the model config file
     */
    public static final String ML_MODEL_CONF = "mlmodel.conf";

    public static <T> void addIfNotNull(T object, String argKey, List<String> command) {
        if (object != null) {
            String param = argKey + object;
            command.add(param);
        }
    }

    public static void addIfNotNull(TimeValue timeValue, String argKey, List<String> command) {
        addIfNotNull(timeValue == null ? null : timeValue.getSeconds(), argKey, command);
    }

    /**
     * Return true if there is a file ES_HOME/config/mlmodel.conf
     */
    public static boolean modelConfigFilePresent(Environment env) {
        Path modelConfPath = XPackPlugin.resolveConfigFile(env, ML_MODEL_CONF);

        return Files.isRegularFile(modelConfPath);
    }
}
