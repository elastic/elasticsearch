/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DocValuesParameterTests extends MapperServiceTestCase {

    // -----------------------------------------------------------------------
    // Null-fallback: omitting one sub-parameter falls back to the field-type default
    // -----------------------------------------------------------------------

    public void testMultiValueWithoutCardinalityUsesDefault() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", "sorted_set").endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(
                new FieldMapper.DocValuesParameter.Values(
                    true,
                    FieldMapper.DocValuesParameter.Values.Cardinality.LOW,
                    FieldMapper.DocValuesParameter.Values.MultiValue.SORTED_SET
                )
            )
        );
    }

    public void testCardinalityWithoutMultiValueUsesDefault() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("cardinality", "high").endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(
                new FieldMapper.DocValuesParameter.Values(
                    true,
                    FieldMapper.DocValuesParameter.Values.Cardinality.HIGH,
                    FieldMapper.DocValuesParameter.Values.MultiValue.SORTED_SET
                )
            )
        );
    }

    // -----------------------------------------------------------------------
    // Unknown-value rejection
    // -----------------------------------------------------------------------

    public void testInvalidCardinality() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        var e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("cardinality", "invalid").endObject())
            )
        );
        assertThat(e.getMessage(), containsString("Unknown value [invalid] for field [cardinality] - accepted values are [low, high]"));
    }

    public void testInvalidMultiValue() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        var e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", "invalid").endObject())
            )
        );
        assertThat(
            e.getMessage(),
            containsString("Unknown value [invalid] for field [multi_value] - accepted values are [no, sorted_set, arrays]")
        );
    }

    // -----------------------------------------------------------------------
    // Boolean shorthand: doc_values: true / false still works alongside map form
    // -----------------------------------------------------------------------

    public void testDocValuesTrueShorthand() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("doc_values", true)));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().enabled(), equalTo(true));
    }

    public void testDocValuesFalseShorthand() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword").field("doc_values", false)));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().enabled(), equalTo(false));
    }
}
