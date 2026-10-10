/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;

public class KnnIndexerTests extends ESTestCase {

    /** The mapping the tester hands its directory says what {@code on_disk_rescore} was asked for. */
    public void testVectorFieldMappingCarriesOnDiskRescore() {
        boolean onDiskRescore = randomBoolean();
        var fieldType = KnnIndexer.vectorFieldMapping(onDiskRescore).getFieldType(KnnIndexer.VECTOR_FIELD);
        assertThat(fieldType, instanceOf(DenseVectorFieldType.class));
        assertEquals(onDiskRescore, ((DenseVectorFieldType) fieldType).getIndexOptions().isOnDiskRescore());
    }
}
