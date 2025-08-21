/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.sandbox.document.HalfFloatPoint;

public class HalfFloatOffsetDocValuesLoaderTests extends OffsetDocValuesLoaderTestCase {

    @Override
    public String getFieldTypeName() {
        return "half_float";
    }

    @Override
    public Object randomValue() {
        return HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(randomFloat()));
    }
}
