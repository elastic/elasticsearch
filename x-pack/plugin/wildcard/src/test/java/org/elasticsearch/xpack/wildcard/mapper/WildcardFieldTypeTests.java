/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.wildcard.mapper;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.wildcard.mapper.WildcardFieldMapper.WildcardFieldType;
import org.junit.Before;

public class WildcardFieldTypeTests extends FieldTypeTestCase {

    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new WildcardFieldMapper.WildcardFieldType();
    }
    
    @Before
    public void setupProperties() {
        addModifier(new Modifier("num_chars", false) {
            @Override
            public void modify(MappedFieldType ft) {
                WildcardFieldType fieldType = (WildcardFieldType) ft;
                fieldType.setNumChars(5);
            }
        });
    }    
}
