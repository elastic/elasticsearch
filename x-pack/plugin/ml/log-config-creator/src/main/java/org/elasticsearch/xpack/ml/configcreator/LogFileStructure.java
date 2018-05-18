/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import java.nio.file.Path;

public interface LogFileStructure {

    /**
     * Write config files suitable for ingesting the log file to the directory provided.
     * @param directory The directory to which the config files will be written.
     * @throws Exception if something goes wrong either during creation or writing of the config files.
     */
    void writeConfigs(Path directory) throws Exception;
}
