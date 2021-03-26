/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.murmur3;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugin.mapper.MapperMurmur3Plugin;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class Murmur3FieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return "value";
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperMurmur3Plugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "murmur3");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("store", b -> b.field("store", true));
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument parsedDoc = mapper.parse(source(b -> b.field("field", "value")));
        IndexableField[] fields = parsedDoc.rootDoc().getFields("field");
        assertNotNull(fields);
        assertEquals(Arrays.toString(fields), 1, fields.length);
        IndexableField field = fields[0];
        assertEquals(IndexOptions.NONE, field.fieldType().indexOptions());
        assertEquals(DocValuesType.SORTED_NUMERIC, field.fieldType().docValuesType());
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }
}
