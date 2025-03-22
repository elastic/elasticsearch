/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class FieldNamesFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return FieldNamesFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return true;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {}

    private static void assertFieldNames(Set<String> expected, ParsedDocument doc) {
        assertThat(TermVectorsService.getValues(doc.rootDoc().getFields("_field_names")), containsInAnyOrder(expected.toArray()));
    }

    public void testFieldType() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().hasDocValues());
    }

    public void testInjectIntoDocDuringParsing() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder().startObject().field("a", "100").startObject("b").field("c", 42).endObject().endObject()
                ),
                XContentType.JSON
            )
        );

        assertFieldNames(Collections.emptySet(), doc);
    }

    public void testUsingEnabledSettingThrows() {
        MapperParsingException ex = expectThrows(MapperParsingException.class, () -> createMapperService(topMapping(b -> {
            b.startObject("_field_names").field("enabled", true).endObject();
            b.startObject("properties");
            {
                b.startObject("field").field("type", "keyword").endObject();
            }
            b.endObject();
        })));

        assertEquals(
            "Failed to parse mapping: "
                + "The `enabled` setting for the `_field_names` field has been deprecated and removed. "
                + "Please remove it from your mappings and templates.",
            ex.getMessage()
        );
    }

    /**
     * disabling the _field_names should still work for indices before 8.0
     */
    public void testUsingEnabledBefore8() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(
            IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.V_8_0_0),
            topMapping(b -> b.startObject("_field_names").field("enabled", false).endObject())
        );

        assertWarnings(FieldNamesFieldMapper.ENABLED_DEPRECATION_MESSAGE);
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().isEnabled());

        ParsedDocument doc = docMapper.parse(source(b -> b.field("field", "value")));
        assertNull(doc.rootDoc().get("_field_names"));
    }

    /**
     * Merging the "_field_names" enabled setting is forbidden in 8.0, but we still want to tests the behavior on pre-8 indices
     */
    public void testMergingMappingsBefore8() throws Exception {
        MapperService mapperService = createMapperService(
            IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.V_8_0_0),
            mapping(b -> {})
        );

        merge(mapperService, topMapping(b -> b.startObject("_field_names").field("enabled", false).endObject()));
        assertFalse(mapperService.documentMapper().metadataMapper(FieldNamesFieldMapper.class).fieldType().isEnabled());
        assertWarnings(FieldNamesFieldMapper.ENABLED_DEPRECATION_MESSAGE);

        merge(mapperService, topMapping(b -> b.startObject("_field_names").field("enabled", true).endObject()));
        assertTrue(mapperService.documentMapper().metadataMapper(FieldNamesFieldMapper.class).fieldType().isEnabled());
        assertWarnings(FieldNamesFieldMapper.ENABLED_DEPRECATION_MESSAGE);
    }
}
