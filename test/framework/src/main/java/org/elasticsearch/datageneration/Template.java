/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration;

import java.util.Map;

/**
 * A template used to generate mapping and documents for a test.
 * Template encodes object structure, names of objects/fields and type of leaf fields.
 * Having such a template allow to create interchangeable random mappings with different parameters
 * but the same structure in order to f.e. test introduction of a new parameter.
 * @param template actual template data
 */
public record Template(Map<String, Entry> template) {
    public sealed interface Entry permits Leaf, Object {}

    public record Leaf(String name, String type) implements Entry {}

    public record Object(String name, boolean nested, Map<String, Entry> children) implements Entry {}
}
