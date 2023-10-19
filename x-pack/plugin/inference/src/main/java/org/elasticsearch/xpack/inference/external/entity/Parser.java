/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.entity;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Parser {

    /**
     * It is expensive to construct the ObjectMapper so we'll do it once
     * <a href="https://github.com/FasterXML/jackson-docs/wiki/Presentation:-Jackson-Performance">See here for more details</a>
     *
     * ObjectMapper is threadsafe as long as it is not reconfigured across multiple threads.
     */
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Parser() {}
}
