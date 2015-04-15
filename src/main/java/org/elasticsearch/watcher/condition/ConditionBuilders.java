/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition;

import org.elasticsearch.watcher.condition.always.AlwaysCondition;
import org.elasticsearch.watcher.condition.never.NeverCondition;
import org.elasticsearch.watcher.condition.script.ExecutableScriptCondition;
import org.elasticsearch.watcher.condition.never.ExecutableNeverCondition;
import org.elasticsearch.watcher.condition.always.ExecutableAlwaysCondition;
import org.elasticsearch.watcher.condition.script.ScriptCondition;

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
        return ScriptCondition.builder(script);
    }
}
