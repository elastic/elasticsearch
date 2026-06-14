/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import java.util.List;

/**
 * Models a {@code @Struct}-annotated nested interface.
 *
 * @param simpleName the simple name of the struct interface (e.g. "RLimit")
 * @param isDynamic whether the struct uses a dynamic layout (offsets provided at runtime)
 * @param fields the list of fields in declaration order
 */
public record StructModel(String simpleName, boolean isDynamic, List<FieldModel> fields) {}
