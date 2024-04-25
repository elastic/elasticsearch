/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;

public class ConstantFieldTypeTestCase extends FieldTypeTestCase {
    @Override
    public void testFieldHasValue() {
        assertTrue(getMappedFieldType().fieldHasValue(new FieldInfos(new FieldInfo[] { getFieldInfoWithName(randomAlphaOfLength(5)) })));
    }

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {
        assertTrue(getMappedFieldType().fieldHasValue(FieldInfos.EMPTY));
    }
}
