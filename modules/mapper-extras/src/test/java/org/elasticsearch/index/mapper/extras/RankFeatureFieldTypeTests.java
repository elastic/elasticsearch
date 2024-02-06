/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class RankFeatureFieldTypeTests extends FieldTypeTestCase {

    public void testIsNotAggregatable() {
        MappedFieldType fieldType = getRankFeatureFieldType();
        assertFalse(fieldType.isAggregatable());
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType mapper = new RankFeatureFieldMapper.Builder("field").nullValue(2.0f)
            .build(MapperBuilderContext.root(false, false))
            .fieldType();

        assertEquals(List.of(3.14f), fetchSourceValue(mapper, 3.14));
        assertEquals(List.of(42.9f), fetchSourceValue(mapper, "42.9"));
        assertEquals(List.of(2.0f), fetchSourceValue(mapper, null));
    }

    public void testFieldHasValueIf_featureIsPresentInFieldInfos() {
        MappedFieldType fieldType = getRankFeatureFieldType();
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { getFieldInfoWithName("_feature") });
        assertTrue(fieldType.fieldHasValue(fieldInfos));
    }

    public void testFieldEmptyIfNameIsPresentInFieldInfos() {
        MappedFieldType fieldType = getRankFeatureFieldType();
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { getFieldInfoWithName("field") });
        assertFalse(fieldType.fieldHasValue(fieldInfos));
    }

    public void testFieldEmptyIfEmptyFieldInfos() {
        MappedFieldType fieldType = getRankFeatureFieldType();
        FieldInfos fieldInfos = FieldInfos.EMPTY;
        assertFalse(fieldType.fieldHasValue(fieldInfos));
    }

    private RankFeatureFieldMapper.RankFeatureFieldType getRankFeatureFieldType() {
        return new RankFeatureFieldMapper.RankFeatureFieldType("field", Collections.emptyMap(), true, null);
    }

    private FieldInfo getFieldInfoWithName(String name) {
        return new FieldInfo(
            name,
            1,
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IndexOptions.NONE,
            DocValuesType.NONE,
            -1,
            new HashMap<>(),
            1,
            1,
            1,
            1,
            VectorEncoding.BYTE,
            VectorSimilarityFunction.COSINE,
            randomBoolean()
        );
    }
}
