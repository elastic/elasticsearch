/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration;

import java.util.Map;

public record MappingTemplate(Map<String, Entry> mapping) {
    public sealed interface Entry permits Entry.Leaf, Entry.Object {
        record Leaf(String name, FieldType type) implements Entry {}

        record Object(String name, boolean nested, Map<String, Entry> children) implements Entry {}
    }
}
