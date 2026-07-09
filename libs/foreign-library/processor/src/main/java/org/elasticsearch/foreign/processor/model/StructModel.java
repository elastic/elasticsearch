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
 * Models a {@code @StructSpecification}-annotated type enclosed in a
 * {@code @LibrarySpecification} interface.
 *
 * <p>For records, {@link #fields()} contains one scalar {@link StructFieldModel} per record component.
 * For interfaces, {@link #fields()} contains all declared abstract methods in declaration order;
 * plain methods become scalar fields and {@code @ArrayField}-annotated methods become array
 * fields with type {@link NativeType#ADDRESS}.
 *
 * @param simpleName the simple name of the struct type
 * @param isRecord   {@code true} for records; {@code false} for interfaces
 * @param fields     field models in declaration order
 */
public record StructModel(String simpleName, boolean isRecord, List<StructFieldModel> fields) {}
