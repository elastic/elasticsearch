/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

/**
 * Contains utility methods for compiled scripts without impacting concrete script signatures
 */
public interface ScriptFactory {
    /** Returns {@code true} if the result of the script will be deterministic, {@code false} otherwise. */
    default boolean isResultDeterministic() { return false; }
}
