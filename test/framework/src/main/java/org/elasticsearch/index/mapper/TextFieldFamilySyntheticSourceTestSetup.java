/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;

/**
 * Provides functionality needed to test synthetic source support in text and text-like fields (e.g. "text", "annotated_text").
 */
public final class TextFieldFamilySyntheticSourceTestSetup {
    public static MapperTestCase.SyntheticSourceSupport syntheticSourceSupport(String fieldType, boolean supportsCustomIndexConfiguration) {
        return new TextFieldFamilySyntheticSourceSupport(fieldType, supportsCustomIndexConfiguration);
    }

    public static MapperTestCase.BlockReaderSupport getSupportedReaders(MapperService mapper, String loaderFieldName) {
        MappedFieldType ft = mapper.fieldType(loaderFieldName);
        String parentName = mapper.mappingLookup().parentField(ft.name());
        if (parentName == null) {
            TextFieldMapper.TextFieldType text = (TextFieldMapper.TextFieldType) ft;
            boolean supportsColumnAtATimeReader = text.syntheticSourceDelegate() != null
                && text.syntheticSourceDelegate().hasDocValues()
                && text.canUseSyntheticSourceDelegateForQuerying();
            return new MapperTestCase.BlockReaderSupport(supportsColumnAtATimeReader, mapper, loaderFieldName);
        }
        MappedFieldType parent = mapper.fieldType(parentName);
        if (false == parent.typeName().equals(KeywordFieldMapper.CONTENT_TYPE)) {
            throw new UnsupportedOperationException();
        }
        KeywordFieldMapper.KeywordFieldType kwd = (KeywordFieldMapper.KeywordFieldType) parent;
        return new MapperTestCase.BlockReaderSupport(kwd.hasDocValues(), mapper, loaderFieldName);
    }

    public static Function<Object, Object> loadBlockExpected(MapperTestCase.BlockReaderSupport blockReaderSupport, boolean columnReader) {
        return v -> ((BytesRef) v).utf8ToString();
    }

    public static void validateRoundTripReader(String syntheticSource, DirectoryReader reader, DirectoryReader roundTripReader) {
        // `reader` here is reader of original document and `roundTripReader` reads document
        // created from synthetic source.
        // This check fails when synthetic source is constructed using keyword subfield
        // since in that case values are sorted (due to being read from doc values) but original document isn't.
        //
        // So it is disabled.
    }

    private static class TextFieldFamilySyntheticSourceSupport implements MapperTestCase.SyntheticSourceSupport {
        private final String fieldType;
        private final boolean store;
        private final boolean index;
        private final Integer ignoreAbove;
        private final KeywordFieldSyntheticSourceSupport keywordMultiFieldSyntheticSourceSupport;

        TextFieldFamilySyntheticSourceSupport(String fieldType, boolean supportsCustomIndexConfiguration) {
            this.fieldType = fieldType;
            this.store = randomBoolean();
            this.index = supportsCustomIndexConfiguration == false || randomBoolean();
            this.ignoreAbove = randomBoolean() ? null : between(10, 100);
            this.keywordMultiFieldSyntheticSourceSupport = new KeywordFieldSyntheticSourceSupport(
                ignoreAbove,
                randomBoolean(),
                null,
                false
            );
        }

        @Override
        public boolean ignoreAbove() {
            return keywordMultiFieldSyntheticSourceSupport.ignoreAbove();
        }

        @Override
        public MapperTestCase.SyntheticSourceExample example(int maxValues) {
            if (store) {
                CheckedConsumer<XContentBuilder, IOException> mapping = b -> {
                    b.field("type", fieldType);
                    b.field("store", true);
                    if (index == false) {
                        b.field("index", false);
                    }
                };

                return storedFieldExample(maxValues, mapping);
            }

            // Block loader will not use keyword multi-field if it has ignore_above configured.
            // And in this case it will use values from source.
            boolean loadingFromSource = ignoreAbove != null;
            MapperTestCase.SyntheticSourceExample delegate = keywordMultiFieldSyntheticSourceSupport.example(maxValues, loadingFromSource);

            return new MapperTestCase.SyntheticSourceExample(
                delegate.inputValue(),
                delegate.expectedForSyntheticSource(),
                delegate.expectedForBlockLoader(),
                b -> {
                    b.field("type", fieldType);
                    if (index == false) {
                        b.field("index", false);
                    }
                    b.startObject("fields");
                    {
                        b.startObject(randomAlphaOfLength(4));
                        delegate.mapping().accept(b);
                        b.endObject();
                    }
                    b.endObject();
                }
            );
        }

        private MapperTestCase.SyntheticSourceExample storedFieldExample(
            int maxValues,
            CheckedConsumer<XContentBuilder, IOException> mapping
        ) {
            if (randomBoolean()) {
                var randomString = randomString();
                return new MapperTestCase.SyntheticSourceExample(randomString, randomString, randomString, mapping);
            }

            var list = ESTestCase.randomList(1, maxValues, this::randomString);
            var output = list.size() == 1 ? list.get(0) : list;

            return new MapperTestCase.SyntheticSourceExample(list, output, output, mapping);
        }

        private String randomString() {
            return randomAlphaOfLengthBetween(0, 10);
        }

        @Override
        public List<MapperTestCase.SyntheticSourceInvalidExample> invalidExample() throws IOException {
            return List.of();
        }
    }
}
