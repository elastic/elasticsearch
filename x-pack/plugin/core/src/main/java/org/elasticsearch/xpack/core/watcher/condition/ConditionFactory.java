/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.condition;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Clock;

/**
 * Parses xcontent to a concrete condition of the same type.
 */
public interface ConditionFactory {

    /**
     * Parses the given xcontent and creates a concrete condition
     * @param watchId                   The id of the watch
     * @param parser                    The parsing that contains the condition content
     */
    ExecutableCondition parse(Clock clock, String watchId, XContentParser parser) throws IOException;

}
