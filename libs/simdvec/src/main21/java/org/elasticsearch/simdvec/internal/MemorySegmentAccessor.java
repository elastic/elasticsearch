/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Provides access to vector data as {@link MemorySegment}s, abstracting over
 * whether vectors are stored in an IndexInput, on-heap arrays, or off-heap stores.
 */
public interface MemorySegmentAccessor {

    MemorySegment entireSegmentOrNull() throws IOException;

    MemorySegment segmentForEntryOrNull(int ordinal) throws IOException;

    MemorySegmentAccessor clone();
}
