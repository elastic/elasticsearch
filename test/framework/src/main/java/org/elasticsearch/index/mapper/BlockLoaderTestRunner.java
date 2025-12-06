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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.lucene.tests.util.LuceneTestCase.newDirectory;
import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.elasticsearch.index.mapper.BlockLoaderTestRunner.PrettyEqual.prettyEqualTo;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BlockLoaderTestRunner {
    private final BlockLoaderTestCase.Params params;

    public BlockLoaderTestRunner(BlockLoaderTestCase.Params params) {
        this.params = params;
    }

    public void runTest(MapperService mapperService, Map<String, Object> document, Object expected, String blockLoaderFieldName)
        throws IOException {
        var documentXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(document);

        Object blockLoaderResult = setupAndInvokeBlockLoader(mapperService, documentXContent, blockLoaderFieldName);
        assertThat(blockLoaderResult, prettyEqualTo(expected));
    }

    private Object setupAndInvokeBlockLoader(MapperService mapperService, XContentBuilder document, String fieldName) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);

            var source = new SourceToParse(
                "1",
                BytesReference.bytes(document),
                XContentType.JSON,
                null,
                Map.of(),
                Map.of(),
                true,
                XContentMeteringParserDecorator.NOOP,
                null
            );
            LuceneDocument doc = mapperService.documentMapper().parse(source).rootDoc();

            /*
             * Add three documents with doc id 0, 1, 2. The real document is 1.
             * The other two are empty documents.
             */
            iw.addDocuments(List.of(List.of(), doc, List.of()));
            iw.close();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext context = reader.leaves().get(0);
                return load(createBlockLoader(mapperService, fieldName), context, mapperService);
            }
        }
    }

    private Object load(BlockLoader blockLoader, LeafReaderContext context, MapperService mapperService) throws IOException {
        // `columnAtATimeReader` is tried first, we mimic `ValuesSourceReaderOperator`
        var columnAtATimeReader = blockLoader.columnAtATimeReader(context);
        if (columnAtATimeReader != null) {
            int[] docArray;
            int offset;
            if (randomBoolean()) {
                // Half the time we load a single document. Nice and simple.
                docArray = new int[] { 1 };
                offset = 0;
            } else {
                /*
                 * The other half the time we emulate loading a larger page,
                 * starting part way through the page.
                 */
                docArray = new int[between(2, 10)];
                offset = between(0, docArray.length - 1);
                for (int i = 0; i < docArray.length; i++) {
                    if (i < offset) {
                        docArray[i] = 0;
                    } else if (i == offset) {
                        docArray[i] = 1;
                    } else {
                        docArray[i] = 2;
                    }
                }
            }
            BlockLoader.Docs docs = TestBlock.docs(docArray);
            var block = (TestBlock) columnAtATimeReader.read(TestBlock.factory(), docs, offset, false);
            assertThat(block.size(), equalTo(docArray.length - offset));
            return block.get(0);
        }

        StoredFieldsSpec storedFieldsSpec = blockLoader.rowStrideStoredFieldSpec();
        SourceLoader.Leaf leafSourceLoader = null;
        if (storedFieldsSpec.requiresSource()) {
            var sourceLoader = mapperService.mappingLookup().newSourceLoader(null, SourceFieldMetrics.NOOP);
            leafSourceLoader = sourceLoader.leaf(context.reader(), null);
            storedFieldsSpec = storedFieldsSpec.merge(
                new StoredFieldsSpec(true, storedFieldsSpec.requiresMetadata(), sourceLoader.requiredStoredFields())
            );
        }
        BlockLoaderStoredFieldsFromLeafLoader storedFieldsLoader = new BlockLoaderStoredFieldsFromLeafLoader(
            StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(context, null),
            leafSourceLoader
        );
        storedFieldsLoader.advanceTo(1);

        BlockLoader.Builder builder = blockLoader.builder(TestBlock.factory(), 1);
        blockLoader.rowStrideReader(context).read(1, storedFieldsLoader, builder);
        var block = (TestBlock) builder.build();
        assertThat(block.size(), equalTo(1));

        return block.get(0);
    }

    private BlockLoader createBlockLoader(MapperService mapperService, String fieldName) {
        SearchLookup searchLookup = new SearchLookup(mapperService.mappingLookup().fieldTypesLookup()::get, null, null);

        return mapperService.fieldType(fieldName).blockLoader(new MappedFieldType.BlockLoaderContext() {
            @Override
            public String indexName() {
                return mapperService.getIndexSettings().getIndex().getName();
            }

            @Override
            public IndexSettings indexSettings() {
                return mapperService.getIndexSettings();
            }

            @Override
            public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                return params.preference();
            }

            @Override
            public SearchLookup lookup() {
                return searchLookup;
            }

            @Override
            public Set<String> sourcePaths(String name) {
                return mapperService.mappingLookup().sourcePaths(name);
            }

            @Override
            public String parentField(String field) {
                return mapperService.mappingLookup().parentField(field);
            }

            @Override
            public FieldNamesFieldMapper.FieldNamesFieldType fieldNames() {
                return (FieldNamesFieldMapper.FieldNamesFieldType) mapperService.fieldType(FieldNamesFieldMapper.NAME);
            }
        });
    }

    // Copied from org.hamcrest.core.IsEqual and modified to pretty print failure when bytesref
    static class PrettyEqual<T> extends BaseMatcher<T> {

        private final Object expectedValue;

        PrettyEqual(T equalArg) {
            expectedValue = equalArg;
        }

        @Override
        public boolean matches(Object actualValue) {
            return areEqual(actualValue, expectedValue);
        }

        @Override
        public void describeTo(Description description) {
            description.appendValue(attemptMakeReadable(expectedValue));
        }

        @Override
        public void describeMismatch(Object item, Description description) {
            super.describeMismatch(attemptMakeReadable(item), description);
        }

        private static boolean areEqual(Object actual, Object expected) {
            if (actual == null) {
                return expected == null;
            }

            if (expected != null && isArray(actual)) {
                return isArray(expected) && areArraysEqual(actual, expected);
            }

            return actual.equals(expected);
        }

        private static boolean areArraysEqual(Object actualArray, Object expectedArray) {
            return areArrayLengthsEqual(actualArray, expectedArray) && areArrayElementsEqual(actualArray, expectedArray);
        }

        private static boolean areArrayLengthsEqual(Object actualArray, Object expectedArray) {
            return Array.getLength(actualArray) == Array.getLength(expectedArray);
        }

        private static boolean areArrayElementsEqual(Object actualArray, Object expectedArray) {
            for (int i = 0; i < Array.getLength(actualArray); i++) {
                if (areEqual(Array.get(actualArray, i), Array.get(expectedArray, i)) == false) {
                    return false;
                }
            }
            return true;
        }

        private static boolean isArray(Object o) {
            return o.getClass().isArray();
        }

        // Attempt to make assertions readable:
        static Object attemptMakeReadable(Object expected) {
            try {
                if (expected instanceof BytesRef bytesRef) {
                    expected = bytesRef.utf8ToString();
                } else if (expected instanceof List<?> list && list.getFirst() instanceof BytesRef) {
                    List<String> expectedList = new ArrayList<>(list.size());
                    for (Object e : list) {
                        expectedList.add(((BytesRef) e).utf8ToString());
                    }
                    expected = expectedList;
                }
                return expected;
            } catch (Exception | AssertionError e) {
                // ip/geo fields can't be converted to strings:
                return expected;
            }
        }

        public static <T> Matcher<T> prettyEqualTo(T operand) {
            return new PrettyEqual<>(operand);
        }

    }
}
