/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.time.DateFormatter;

import static org.elasticsearch.index.mapper.ObjectMapper.Dynamic;

/**
 * Defines how runtime fields are dynamically created. Used when objects are mapped with dynamic:runtime.
 * Plugins that provide runtime field implementations can also plug in their implementation of this interface
 * to define how leaf fields of each supported type can be dynamically created in dynamic runtime mode.
 *
 * @see Dynamic
 */
public interface DynamicRuntimeFieldsBuilder {
    /**
     * Dynamically creates a runtime field from a parsed string value
     */
    RuntimeFieldType newDynamicStringField(String name);
    /**
     * Dynamically creates a runtime field from a parsed long value
     */
    RuntimeFieldType newDynamicLongField(String name);
    /**
     * Dynamically creates a runtime field from a parsed double value
     */
    RuntimeFieldType newDynamicDoubleField(String name);
    /**
     * Dynamically creates a runtime field from a parsed boolean value
     */
    RuntimeFieldType newDynamicBooleanField(String name);
    /**
     * Dynamically creates a runtime field from a parsed date value
     */
    RuntimeFieldType newDynamicDateField(String name, DateFormatter dateFormatter);
}
