/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import java.util.List;

/**
 * A column in the output schema of a query execution.
 * @param name the field name
 * @param type the field type
 * @param originalTypes the original types, in case of a union type.
 */
public record Column(String name, String type, List<String> originalTypes) {}
