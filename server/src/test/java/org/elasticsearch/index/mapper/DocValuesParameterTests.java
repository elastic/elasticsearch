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
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("multi_value", false).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, false))
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
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.HIGH, true))
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

    // -----------------------------------------------------------------------
    // skippers sub-parameter
    // -----------------------------------------------------------------------

    public void testSkippersDefaultsToFalse() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "keyword")));
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().skippers(), equalTo(false));
    }

    public void testSkippersExplicitlyEnabledOnLowCardinality() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("skippers", true).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(
            mapper.docValuesParameters(),
            equalTo(new FieldMapper.DocValuesParameter.Values(true, FieldMapper.DocValuesParameter.Values.Cardinality.LOW, true, true))
        );
    }

    public void testSkippersExplicitlyDisabled() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("skippers", false).endObject())
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().skippers(), equalTo(false));
    }

    public void testSkippersRejectedWhenCardinalityHigh() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        var e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                fieldMapping(
                    b -> b.field("type", "keyword")
                        .startObject("doc_values")
                        .field("cardinality", "high")
                        .field("skippers", true)
                        .endObject()
                )
            )
        );
        assertThat(e.getMessage(), containsString("[doc_values.skippers] cannot be enabled when [doc_values.cardinality] is [high]"));
    }

    public void testSkippersAcceptedWhenCardinalityLow() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(
                b -> b.field("type", "keyword").startObject("doc_values").field("cardinality", "low").field("skippers", true).endObject()
            )
        );
        KeywordFieldMapper mapper = (KeywordFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().skippers(), equalTo(true));
        assertThat(mapper.docValuesParameters().cardinality(), equalTo(FieldMapper.DocValuesParameter.Values.Cardinality.LOW));
    }

    public void testSkippersOnNumericField() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "long").startObject("doc_values").field("skippers", true).endObject())
        );
        NumberFieldMapper mapper = (NumberFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.docValuesParameters().skippers(), equalTo(true));
    }

    public void testSkippersIsNonUpdateable() throws Exception {
        assumeTrue("feature under test must be enabled", FieldMapper.DocValuesParameter.EXTENDED_DOC_VALUES_PARAMS_FF.isEnabled());
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("skippers", true).endObject())
        );
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(
                mapperService,
                fieldMapping(b -> b.field("type", "keyword").startObject("doc_values").field("skippers", false).endObject())
            )
        );
        assertThat(e.getMessage(), containsString("skippers"));
    }
}
