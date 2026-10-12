/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.index.FieldInfo;

/**
 * Resolves the {@link ColumnarFieldType} of a field written to {@link ColumNARDocValuesFormat}. Injected at
 * construction so the type is supplied by the caller rather than read from a {@link FieldInfo} attribute,
 * mirroring {@link org.elasticsearch.columnar.numeric.NumericPipelineSelector}.
 */
@FunctionalInterface
public interface ColumnarFieldTypeSelector {

    /**
     * @param field the field being written
     * @return the {@link ColumnarFieldType} that field should be encoded as
     */
    ColumnarFieldType select(FieldInfo field);
}
