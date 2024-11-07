/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class KqlNestedFieldQueryTests extends AbstractKqlParserTestCase {

    private static final String NESTED_FIELD_NAME = "mapped_nested";

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");

        mapping.startObject(TEXT_FIELD_NAME).field("type", "text").endObject();
        mapping.startObject(NESTED_FIELD_NAME);
        {
            mapping.field("type", "nested");
            mapping.startObject("properties");
            {
                mapping.startObject(TEXT_FIELD_NAME).field("type", "text").endObject();
                mapping.startObject(KEYWORD_FIELD_NAME).field("type", "keyword").endObject();
                mapping.startObject(INT_FIELD_NAME).field("type", "integer").endObject();
                mapping.startObject(NESTED_FIELD_NAME);
                {
                    mapping.field("type", "nested");
                    mapping.startObject("properties");
                    {
                        mapping.startObject(TEXT_FIELD_NAME).field("type", "text").endObject();
                        mapping.startObject(KEYWORD_FIELD_NAME).field("type", "keyword").endObject();
                        mapping.startObject(INT_FIELD_NAME).field("type", "integer").endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        mapping.endObject().endObject().endObject();

        mapperService.merge("_doc", new CompressedXContent(Strings.toString(mapping)), MapperService.MergeReason.MAPPING_UPDATE);
    }

    public void testInvalidNestedFieldName() {
        for (String invalidFieldName : List.of(TEXT_FIELD_NAME, "not_a_field", "mapped_nest*")) {
            KqlParsingException e = assertThrows(
                KqlParsingException.class,
                () -> parseKqlQuery(Strings.format("%s : { %s: foo AND %s < 10 } ", invalidFieldName, TEXT_FIELD_NAME, INT_FIELD_NAME))
            );
            assertThat(e.getMessage(), Matchers.containsString(invalidFieldName));
            assertThat(e.getMessage(), Matchers.containsString("is not a valid nested field name"));
        }
    }
}
