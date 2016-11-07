/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Clock;

/**
 * Parses xcontent to a concrete condition of the same type.
 */
public interface ConditionFactory {

    /**
     * Parses the given xcontent and creates a concrete condition
     *  @param watchId                   The id of the watch
     * @param parser                    The parsing that contains the condition content
     * @param upgradeConditionSource    Whether to upgrade the source related to condition if in legacy format
     *                                  Note: depending on the version, only conditions implementations that have a
     */
    Condition parse(Clock clock, String watchId, XContentParser parser, boolean upgradeConditionSource) throws IOException;

}
