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
import org.apache.lucene.search.Query;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.MetadataDocValuesFieldMapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
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
 * {@link MetadataDocValuesFieldMapper#getDocValuesFormatForField} when the field's mapper extends
 * {@link MetadataDocValuesFieldMapper}, and otherwise leaves every other field on the plain default.
 * This is the extension point that lets a module (e.g. serverless metering) select a custom
 * {@code DocValuesFormat} for one synthetic field natively, rather than by decorating the codec
 * from the outside.
 */
public class PerFieldFormatSupplierDocValuesFormatProviderTests extends MapperServiceTestCase {
    private static final String NAME = "_somefield";

    private static final class CustomFormatMetadataMapper extends MetadataDocValuesFieldMapper {
        private static final CustomFormatMetadataMapper INSTANCE = new CustomFormatMetadataMapper();
        private static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

        private final DocValuesFormat format = new Lucene90DocValuesFormat();

        static final class MyType extends MappedFieldType {
            MyType() {
                super(NAME, IndexType.NONE, false, Map.of());
            }

            @Override
            public String typeName() {
                return name();
            }

            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                throw new UnsupportedOperationException();
            }
        }

        private CustomFormatMetadataMapper() {
            super(new MyType());
        }

        @Override
        public DocValuesFormat getDocValuesFormatForField(DocValuesFormat defaultFormat) {
            return format;
        }

        @Override
        protected String contentType() {
            return mappedFieldType.typeName();
        }
    }

    private static final class CustomFormatMapperPlugin extends Plugin implements MapperPlugin {
        @Override
        public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
            return Collections.singletonMap(NAME, CustomFormatMetadataMapper.PARSER);
        }
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new CustomFormatMapperPlugin());
    }

    public void testMapperProvidedFormatIsUsedOnlyForItsOwnField() throws IOException {
        PerFieldFormatSupplier supplier = new PerFieldFormatSupplier(createMapperService("""
            { "_doc": { "properties": { } } }"""), BigArrays.NON_RECYCLING_INSTANCE, null);

        DocValuesFormat customFieldFormat = supplier.getDocValuesFormatForField(NAME);
        assertThat(customFieldFormat, sameInstance(CustomFormatMetadataMapper.INSTANCE.format));

        DocValuesFormat plainFieldFormat = supplier.getDocValuesFormatForField("plain_field");
        assertThat(plainFieldFormat, not(sameInstance(customFieldFormat)));
        // every other field still gets the exact same shared default instance, untouched
        assertThat(plainFieldFormat, sameInstance(supplier.getDocValuesFormatForField("another_plain_field")));
    }
}
