/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

/**
 * Converts between one scripting {@link Field} type and another,  {@code CF}, with a different underlying
 * value type, {@code CT}.
 */
public interface Converter<CT, CF extends Field<CT>> {
    /**
     * Convert {@code sourceField} to a new field-type.  Conversions come from user scripts so {@code covert} may
     * be called on a {@link Field}'s own type.
     */
    CF convert(Field<?> sourceField);

    /**
     * The destination {@link Field} class.
     */
    Class<CF> getFieldClass();

    /**
     * The target value type.
     */
    Class<CT> getTargetClass();
}
