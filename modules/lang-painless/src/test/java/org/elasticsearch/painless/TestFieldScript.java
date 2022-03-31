/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.List;

public abstract class TestFieldScript {
    private final List<Long> values = new ArrayList<>();

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory {
        TestFieldScript newInstance();
    }

    public static final ScriptContext<TestFieldScript.Factory> CONTEXT = new ScriptContext<>(
        "painless_test_fieldscript",
        TestFieldScript.Factory.class
    );

    public static class Emit {
        private final TestFieldScript script;

        public Emit(TestFieldScript script) {
            this.script = script;
        }

        public void emit(long v) {
            script.emit(v);
        }
    }

    public abstract void execute();

    public final void emit(long v) {
        values.add(v);
    }

    public long[] fetchValues() {
        return values.stream().mapToLong(i -> i).toArray();
    }
}
