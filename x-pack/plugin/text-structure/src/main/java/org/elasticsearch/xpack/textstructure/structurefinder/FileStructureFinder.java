/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.List;

public interface FileStructureFinder {

    /**
     * The (possibly multi-line) messages that the sampled lines were combined into.
     * @return A list of messages.
     */
    List<String> getSampleMessages();

    /**
     * Retrieve the structure of the file used to instantiate the finder.
     * @return The file structure.
     */
    TextStructure getStructure();
}
