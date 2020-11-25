/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.plugins.MapperPlugin;

/**
 * Defines how runtime fields are created dynamically. Used when objects are mapped with dynamic:runtime.
 * @see MapperPlugin#getDynamicRuntimeFieldsBuilder()
 */
public interface DynamicRuntimeFieldsBuilder {
    /**
     * Dynamically create a runtime field from a parsed string value
     */
    RuntimeFieldType newDynamicStringField(String name);
    /**
     * Dynamically create a runtime field from a parsed long value
     */
    RuntimeFieldType newDynamicLongField(String name);
    /**
     * Dynamically create a runtime field from a parsed double value
     */
    RuntimeFieldType newDynamicDoubleField(String name);
    /**
     * Dynamically create a runtime field from a parsed boolean value
     */
    RuntimeFieldType newDynamicBooleanField(String name);
    /**
     * Dynamically create a runtime field from a parsed date value
     */
    RuntimeFieldType newDynamicDateField(String name, DateFormatter dateFormatter);
}
