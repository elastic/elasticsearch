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
 * A {@code @StructSpecification}-annotated type enclosed in a {@code @LibrarySpecification}
 * interface. The variant determines the Java-side surface: a
 * {@link StructRecordModel record} is a Java value type that packs into native memory, and an
 * {@link StructInterfaceModel interface} is a Java view backed by a native
 * {@link java.lang.foreign.MemorySegment}. Field shape (scalar vs array) is orthogonal and
 * modelled by {@link StructFieldModel}.
 */
public sealed interface StructModel permits StructRecordModel, StructInterfaceModel {
    /** The simple name of the struct type. */
    String simpleName();

    /** Field models in declaration order. */
    List<StructFieldModel> fields();
}
