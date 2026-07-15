/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.mapper.DocValuesFormatProvider;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Verifies that {@link PerFieldFormatSupplier#getDocValuesFormatForField} delegates to a mapper's own
 * {@link DocValuesFormatProvider} when the field's mapper implements it, and otherwise leaves every
 * other field on the plain default. This is the extension point that lets a module (e.g. serverless
 * metering) select a custom {@code DocValuesFormat} for one synthetic field natively, rather than by
 * decorating the codec from the outside.
 */
public class PerFieldFormatSupplierDocValuesFormatProviderTests extends MapperServiceTestCase {

    private static final String CUSTOM_FIELD_NAME = "_test_custom_docvalues_format";

    private static final class CustomFormatMetadataMapper extends MetadataFieldMapper implements DocValuesFormatProvider {
        private static final String CONTENT_TYPE = CUSTOM_FIELD_NAME;
        private static final TypeParser PARSER = new FixedTypeParser(c -> new CustomFormatMetadataMapper());

        private final DocValuesFormat customFormat = new Lucene90DocValuesFormat();

        private CustomFormatMetadataMapper() {
            super(new KeywordFieldMapper.KeywordFieldType(CUSTOM_FIELD_NAME));
        }

        @Override
        public DocValuesFormat getDocValuesFormatForField(DocValuesFormat defaultFormat) {
            return customFormat;
        }

        @Override
        protected String contentType() {
            return CONTENT_TYPE;
        }
    }

    private static final class CustomFormatMapperPlugin extends Plugin implements MapperPlugin {
        @Override
        public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
            return Collections.singletonMap(CustomFormatMetadataMapper.CONTENT_TYPE, CustomFormatMetadataMapper.PARSER);
        }
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new CustomFormatMapperPlugin());
    }

    public void testMapperProvidedFormatIsUsedOnlyForItsOwnField() throws IOException {
        MapperService mapperService = createMapperService("""
            { "properties": { "plain_field": { "type": "keyword" } } }""");
        PerFieldFormatSupplier supplier = new PerFieldFormatSupplier(mapperService, BigArrays.NON_RECYCLING_INSTANCE, null);

        var customMapper = (CustomFormatMetadataMapper) mapperService.mappingLookup().getMapper(CUSTOM_FIELD_NAME);
        DocValuesFormat customFieldFormat = supplier.getDocValuesFormatForField(CUSTOM_FIELD_NAME);
        assertThat(customFieldFormat, sameInstance(customMapper.customFormat));

        DocValuesFormat plainFieldFormat = supplier.getDocValuesFormatForField("plain_field");
        assertThat(plainFieldFormat, not(sameInstance(customFieldFormat)));
        // every other field still gets the exact same shared default instance, untouched
        assertThat(plainFieldFormat, sameInstance(supplier.getDocValuesFormatForField("another_plain_field")));
    }
}
