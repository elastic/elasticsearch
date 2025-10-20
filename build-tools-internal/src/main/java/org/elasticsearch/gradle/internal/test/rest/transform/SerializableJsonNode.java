/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.Serializable;

/**
 * A serializable wrapper for a JsonNode that can be used as Gradle task inputs.
 * This is necessary because JsonNode serialization is not supported by Gradle configuration cache
 * as it relies on DataInput.readFully which is unsupported by Gradle.
 *
 * @param <T> The type of JsonNode this wrapper will hold.
 */
public class SerializableJsonNode<T extends JsonNode> implements Serializable {

    private Object value;
    private Class<? extends JsonNode> type;

    SerializableJsonNode(Object value, Class<? extends JsonNode> type) {
        this.value = value;
        this.type = type;
    }

    public static SerializableJsonNode of(Object value, Class<? extends JsonNode> type) {
        return new SerializableJsonNode(value, type);
    }

    public T toJsonNode() {
        YAMLFactory YAML_FACTORY = new YAMLFactory();
        ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);
        return (T) MAPPER.convertValue(value, type);
    }
}
