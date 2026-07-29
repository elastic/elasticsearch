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
 * A {@link StructModel} whose Java surface is an interface. Instances of the generated
 * implementation wrap a native {@link java.lang.foreign.MemorySegment} and expose VarHandle-backed
 * field access.
 *
 * <p>Layout is fully resolved: every field carries its absolute {@link StructFieldModel#offset()}
 * and {@code byteSize} is the total struct size. A single {@code StructInterfaceModel} describes the
 * layout for one platform; per-platform variation is represented by distinct instances (see
 * {@link StructVariants}).
 */
public record StructInterfaceModel(String simpleName, List<StructFieldModel> fields, long byteSize) implements StructModel {}
