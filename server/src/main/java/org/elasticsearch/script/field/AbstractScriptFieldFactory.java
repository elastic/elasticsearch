/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

/**
 * This class is acting as a placeholder where a {@link Field} is its own
 * {@link ScriptFieldFactory} as we continue to refactor each {@link Field}
 * for source fallback.
 */
public abstract class AbstractScriptFieldFactory<T> implements Field<T>, ScriptFieldFactory {

    public Field<?> toScriptField() {
        return this;
    }
}
