/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

/**
 * Converts between one scripting {@link Field} type and another, {@code FC}, with a different underlying
 * value type, {@code TC}.
 */
public interface Converter<TC, FC extends Field<TC>> {
    /**
     * Convert {@code sourceField} to a new field-type.  Conversions come from user scripts so {@code covert} may
     * be called on a {@link Field}'s own type.
     */
    FC convert(Field<?> sourceField);

    /**
     * The destination {@link Field} class.
     */
    Class<FC> getFieldClass();

    /**
     * The target value type.
     */
    Class<TC> getTargetClass();
}
