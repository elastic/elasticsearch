/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Map;
import java.util.function.Function;

/**
 * A mocked script used for testing purposes.  {@code deterministic} implies cacheability in query shard cache.
 */
public abstract class MockDeterministicScript implements Function<Map<String, Object>, Object>, ScriptFactory {
    public abstract Object apply(Map<String, Object> vars);
    public abstract boolean isResultDeterministic();

    public static MockDeterministicScript asDeterministic(Function<Map<String, Object>, Object> script) {
        return new MockDeterministicScript() {
            @Override public boolean isResultDeterministic() { return true; }
            @Override public Object apply(Map<String, Object> vars) { return script.apply(vars); }
        };
    }

    public static MockDeterministicScript asNonDeterministic(Function<Map<String, Object>, Object> script) {
        return new MockDeterministicScript() {
            @Override public boolean isResultDeterministic() { return false; }
            @Override public Object apply(Map<String, Object> vars) { return script.apply(vars); }
        };
    }
}
