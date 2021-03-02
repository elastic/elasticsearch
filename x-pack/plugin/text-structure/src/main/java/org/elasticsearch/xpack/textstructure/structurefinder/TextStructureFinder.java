/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.List;

public interface TextStructureFinder {

    /**
     * The (possibly multi-line) messages that the sampled lines were combined into.
     * @return A list of messages.
     */
    List<String> getSampleMessages();

    /**
     * Retrieve the structure of the text used to instantiate the finder.
     * @return The text structure.
     */
    TextStructure getStructure();
}
