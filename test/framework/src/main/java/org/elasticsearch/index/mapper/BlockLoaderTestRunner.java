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
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
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

import static org.apache.lucene.tests.util.LuceneTestCase.newDirectory;
import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test helper for {@link BlockLoader}. Run it like:
 * <pre>{@code
 *  BlockLoaderTestRunner runner = new BlockLoaderTestRunner(params);
 *  runner.mapperService(createMapperService...);
 *  runner.fieldName("field").document(Map.of("field", 1));
 *  runner.run(1);
 * }</pre>
 */
public class BlockLoaderTestRunner {
    public interface ResultMatcher {
        void match(Object expected, Object actual);
    }

    private final BlockLoaderTestCase.Params params;
    private boolean allowDummyDocs;
    private MapperService mapperService;
    private String fieldName;
    private ParsedDocument doc;
    private Map<String, Object> mapDoc;
    private ResultMatcher matcher = (expected, actual) -> assertThat(actual, PrettyEqual.prettyEqualTo(expected));

    public BlockLoaderTestRunner(BlockLoaderTestCase.Params params) {
        this.params = params;
    }

    /**
     * Allow dummy documents in test index. This defaults to {@code false} but many callers
     * are fine with these and call this.
     */
    public BlockLoaderTestRunner allowDummyDocs() {
        this.allowDummyDocs = true;
        return this;
    }

    /**
     * Set the {@link MapperService} to use for the test. This must be provided before
     * calling {@link #run}.
     */
    public BlockLoaderTestRunner mapperService(MapperService mapperService) {
        this.mapperService = mapperService;
        return this;
    }

    /**
     * The name of the field to load. The test sends this to {@link MapperService#fieldType}.
     */
    public String fieldName() {
        return fieldName;
    }

    /**
     * Set the name of the field to load. The test sends this to {@link MapperService#fieldType}.
     * This is required before calling {@link #run}
     */
    public BlockLoaderTestRunner fieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    /**
     * Configuration a non-standard matcher. The default matcher is usually fine, and
     * you often don't have to call this.
     */
    public BlockLoaderTestRunner matcher(ResultMatcher matcher) {
        this.matcher = matcher;
        return this;
    }

    /**
     * The document being parsed.
     */
    public ParsedDocument document() {
        if (doc != null) {
            return doc;
        }
        if (mapDoc != null) {
            if (mapperService == null) {
                throw new IllegalStateException("need to set mapperService");
            }
            try {
                var documentXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapDoc);
                var source = new SourceToParse(
                    "1",
                    BytesReference.bytes(documentXContent),
                    XContentType.JSON,
                    null,
                    Map.of(),
                    Map.of(),
                    true,
                    XContentMeteringParserDecorator.NOOP,
                    null
                );
                doc = mapperService.documentMapper().parse(source);
                return doc;
            } catch (IOException e) {
                throw new RuntimeException("shouldn't be possible", e);
            }
        }
        throw new IllegalStateException("need to set doc");
    }

    /**
     * The document to be parsed as a {@link Map}. This will only work if the document
     * was set with {@link #document(Map)}.
     */
    public Map<String, Object> mapDoc() {
        if (mapDoc == null) {
            throw new IllegalStateException("need to set doc to a map");
        }
        return mapDoc;
    }

    /**
     * Set the document to be parsed. A method with this name must be called before
     * calling {@link #run}.
     */
    public void document(Map<String, Object> doc) throws IOException {
        this.mapDoc = doc;
    }

    /**
     * Set the document to be parsed. A method with this name must be called before
     * calling {@link #run}.
     */
    public void document(ParsedDocument doc) throws IOException {
        this.doc = doc;
    }

    /**
     * Run the test and compare to {@code expected}.
     */
    public void run(Object expected) throws IOException {
        matcher.match(expected, setupAndInvokeBlockLoader());
    }

    private Object setupAndInvokeBlockLoader() throws IOException {
        if (mapperService == null) {
            throw new IllegalStateException("need to set mapperService");
        }
        if (fieldName == null) {
            throw new IllegalStateException("need to set fieldName");
        }
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);

            LuceneDocument doc = this.document().rootDoc();

            /*
             * Add three documents with doc id 0, 1, 2. The real document is 1.
             * The other two are empty documents.
             */
            iw.addDocuments(List.of(List.of(), doc, List.of()));
            iw.close();

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext context = reader.leaves().getFirst();
                return load(createBlockLoader(fieldName), context);
            }
        }
    }

    private Object load(BlockLoader blockLoader, LeafReaderContext context) throws IOException {
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
                        docArray[i] = allowDummyDocs ? 2 : 1;
                    }
                }
            }
            BlockLoader.Docs docs = TestBlock.docs(docArray);
            var block = (TestBlock) columnAtATimeReader.get().read(TestBlock.factory(), docs, offset, false);
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

    private BlockLoader createBlockLoader(String fieldName) {
        return mapperService.fieldType(fieldName).blockLoader(new DummyBlockLoaderContext.MapperServiceBlockLoaderContext(mapperService) {
            @Override
            public MappedFieldType.FieldExtractPreference fieldExtractPreference() {
                return params.preference();
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
