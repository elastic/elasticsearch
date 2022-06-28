/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class FieldNamesFieldMapperTests extends MapperServiceTestCase {

    private static SortedSet<String> set(String... values) {
        return new TreeSet<>(Arrays.asList(values));
    }

    void assertFieldNames(Set<String> expected, ParsedDocument doc) {
        IndexableField[] fields = doc.rootDoc().getFields("_field_names");
        List<String> result = new ArrayList<>();
        for (IndexableField field : fields) {
            if (field.name().equals(FieldNamesFieldMapper.Defaults.NAME)) {
                result.add(field.stringValue());
            }
        }
        assertEquals(expected, set(result.toArray(new String[0])));
    }

    public void testFieldType() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().hasDocValues());

        assertEquals(IndexOptions.DOCS, FieldNamesFieldMapper.Defaults.FIELD_TYPE.indexOptions());
        assertFalse(FieldNamesFieldMapper.Defaults.FIELD_TYPE.tokenized());
        assertFalse(FieldNamesFieldMapper.Defaults.FIELD_TYPE.stored());
        assertTrue(FieldNamesFieldMapper.Defaults.FIELD_TYPE.omitNorms());
    }

    public void testInjectIntoDocDuringParsing() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(
            new SourceToParse(
                "test",
                "_doc",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder().startObject().field("a", "100").startObject("b").field("c", 42).endObject().endObject()
                ),
                XContentType.JSON
            )
        );

        assertFieldNames(Collections.emptySet(), doc);
    }

    /**
     * disabling the _field_names should still work for indices before 8.0
     */
    public void testUsingEnabledIssuesDeprecationWarning() throws Exception {

        DocumentMapper docMapper = createDocumentMapper(topMapping(b -> b.startObject("_field_names").field("enabled", false).endObject()));

        assertWarnings(FieldNamesFieldMapper.ENABLED_DEPRECATION_MESSAGE);
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().isEnabled());

        ParsedDocument doc = docMapper.parse(source(b -> b.field("field", "value")));
        assertNull(doc.rootDoc().get("_field_names"));
    }

}
