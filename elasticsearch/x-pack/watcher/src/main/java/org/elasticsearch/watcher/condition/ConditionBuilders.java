/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition;

import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.condition.compare.CompareCondition;
import org.elasticsearch.watcher.condition.compare.array.ArrayCompareCondition;
import org.elasticsearch.watcher.condition.never.NeverCondition;
import org.elasticsearch.watcher.condition.script.ScriptCondition;
import org.elasticsearch.watcher.support.Script;

/**
 *
 */
public final class ConditionBuilders {

    private ConditionBuilders() {
    }

    public static AlwaysCondition.Builder alwaysCondition() {
        return AlwaysCondition.Builder.INSTANCE;
    }

    public static NeverCondition.Builder neverCondition() {
        return NeverCondition.Builder.INSTANCE;
    }

    public static ScriptCondition.Builder scriptCondition(String script) {
        return scriptCondition(Script.inline(script));
    }

    public static ScriptCondition.Builder scriptCondition(Script.Builder script) {
        return scriptCondition(script.build());
    }

    public static ScriptCondition.Builder scriptCondition(Script script) {
        return ScriptCondition.builder(script);
    }

    public static CompareCondition.Builder compareCondition(String path, CompareCondition.Op op, Object value) {
        return CompareCondition.builder(path, op, value);
    }

    public static ArrayCompareCondition.Builder arrayCompareCondition(String arrayPath, String path, ArrayCompareCondition.Op op,
                                                                      Object value, ArrayCompareCondition .Quantifier quantifier) {
        return ArrayCompareCondition.builder(arrayPath, path, op, value, quantifier);
    }
}
