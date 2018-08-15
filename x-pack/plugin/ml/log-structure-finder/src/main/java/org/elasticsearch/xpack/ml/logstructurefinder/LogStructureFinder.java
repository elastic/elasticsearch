/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

import java.util.List;

public interface LogStructureFinder {

    /**
     * The (possibly multi-line) messages that the log sample was split into.
     * @return A list of messages.
     */
    List<String> getSampleMessages();

    /**
     * Retrieve the structure of the log file used to instantiate the finder.
     * @return The log file structure.
     */
    LogStructure getStructure();
}
