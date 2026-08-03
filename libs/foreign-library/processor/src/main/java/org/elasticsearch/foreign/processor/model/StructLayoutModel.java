/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import java.lang.foreign.MemoryLayout;
import java.util.List;

/**
 * One distinct resolved memory layout of a {@code @StructSpecification} type, together with the
 * platforms that share it. Field offsets and total size are encoded in {@code layout} (queried by
 * name via {@link MemoryLayout#byteOffset} / {@link MemoryLayout#byteSize}); nothing here depends on
 * the position of {@code platforms} in a {@link StructModel#layouts()} list.
 *
 * @param platforms {@code Platform} enum constant names sharing this layout, in enum (ordinal) order
 * @param layout the {@code MemoryLayout.structLayout(...)} for those platforms
 */
public record StructLayoutModel(List<String> platforms, MemoryLayout layout) {}
