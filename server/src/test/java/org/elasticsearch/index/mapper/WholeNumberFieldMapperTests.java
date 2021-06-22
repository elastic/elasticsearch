/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

public abstract class WholeNumberFieldMapperTests extends NumberFieldMapperTests {

    protected void testDecimalCoerce() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "7.89")));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        IndexableField pointField = fields[0];
        assertEquals(7, pointField.numericValue().doubleValue(), 0d);
    }

    public void testDimension() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("dimension", true);
        }));

        MappedFieldType fieldType = mapperService.fieldType("field");
        assertThat(fieldType, instanceOf(NumberFieldMapper.NumberFieldType.class));
        NumberFieldMapper.NumberFieldType ft = (NumberFieldMapper.NumberFieldType) fieldType;
        assertTrue(ft.isDimension());
    }

}
