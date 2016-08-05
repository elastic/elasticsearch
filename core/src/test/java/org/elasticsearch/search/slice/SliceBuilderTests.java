/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.slice;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SliceBuilderTests extends ESTestCase {
    private static final int MAX_SLICE = 20;
    private static IndicesQueriesRegistry indicesQueriesRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        indicesQueriesRegistry = new IndicesQueriesRegistry();
        QueryParser<MatchAllQueryBuilder> parser = MatchAllQueryBuilder::fromXContent;
        indicesQueriesRegistry.register(parser, MatchAllQueryBuilder.NAME);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        indicesQueriesRegistry = null;
    }

    private SliceBuilder randomSliceBuilder() throws IOException {
        int max = randomIntBetween(2, MAX_SLICE);
        int id = randomInt(max - 1);
        String field = randomAsciiOfLengthBetween(5, 20);
        return new SliceBuilder(field, id, max);
    }

    private static SliceBuilder serializedCopy(SliceBuilder original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                return new SliceBuilder(in);
            }
        }
    }

    public void testSerialization() throws Exception {
        SliceBuilder original = randomSliceBuilder();
        SliceBuilder deserialized = serializedCopy(original);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHashcode() throws Exception {
        SliceBuilder firstBuilder = randomSliceBuilder();
        assertFalse("sliceBuilder is equal to null", firstBuilder.equals(null));
        assertFalse("sliceBuilder is equal to incompatible type", firstBuilder.equals(""));
        assertTrue("sliceBuilder is not equal to self", firstBuilder.equals(firstBuilder));
        assertThat("same searchFrom's hashcode returns different values if called multiple times",
            firstBuilder.hashCode(), equalTo(firstBuilder.hashCode()));

        SliceBuilder secondBuilder = serializedCopy(firstBuilder);
        assertTrue("sliceBuilder is not equal to self", secondBuilder.equals(secondBuilder));
        assertTrue("sliceBuilder is not equal to its copy", firstBuilder.equals(secondBuilder));
        assertTrue("equals is not symmetric", secondBuilder.equals(firstBuilder));
        assertThat("sliceBuilder copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
            equalTo(firstBuilder.hashCode()));
        SliceBuilder thirdBuilder = serializedCopy(secondBuilder);
        assertTrue("sliceBuilder is not equal to self", thirdBuilder.equals(thirdBuilder));
        assertTrue("sliceBuilder is not equal to its copy", secondBuilder.equals(thirdBuilder));
        assertThat("sliceBuilder copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
            equalTo(thirdBuilder.hashCode()));
        assertTrue("equals is not transitive", firstBuilder.equals(thirdBuilder));
        assertThat("sliceBuilder copy's hashcode is different from original hashcode", firstBuilder.hashCode(),
            equalTo(thirdBuilder.hashCode()));
        assertTrue("sliceBuilder is not symmetric", thirdBuilder.equals(secondBuilder));
        assertTrue("sliceBuilder is not symmetric", thirdBuilder.equals(firstBuilder));
    }

    public void testFromXContent() throws Exception {
        SliceBuilder sliceBuilder = randomSliceBuilder();
        XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        builder.startObject();
        sliceBuilder.innerToXContent(builder);
        builder.endObject();
        XContentParser parser = XContentHelper.createParser(shuffleXContent(builder).bytes());
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser,
            ParseFieldMatcher.STRICT);
        SliceBuilder secondSliceBuilder = SliceBuilder.fromXContent(context);
        assertNotSame(sliceBuilder, secondSliceBuilder);
        assertEquals(sliceBuilder, secondSliceBuilder);
        assertEquals(sliceBuilder.hashCode(), secondSliceBuilder.hashCode());
    }

    public void testInvalidArguments() throws Exception {
        Exception e = expectThrows(IllegalArgumentException.class, () -> new SliceBuilder("field", -1, 10));
        assertEquals(e.getMessage(), "id must be greater than or equal to 0");

        e = expectThrows(IllegalArgumentException.class, () -> new SliceBuilder("field", 10, -1));
        assertEquals(e.getMessage(), "max must be greater than 1");

        e = expectThrows(IllegalArgumentException.class, () -> new SliceBuilder("field", 10, 0));
        assertEquals(e.getMessage(), "max must be greater than 1");

        e = expectThrows(IllegalArgumentException.class, () -> new SliceBuilder("field", 10, 5));
        assertEquals(e.getMessage(), "max must be greater than id");

        e = expectThrows(IllegalArgumentException.class, () -> new SliceBuilder("field", 1000, 1000));
        assertEquals(e.getMessage(), "max must be greater than id");
        e = expectThrows(IllegalArgumentException.class, () -> new SliceBuilder("field", 1001, 1000));
        assertEquals(e.getMessage(), "max must be greater than id");
    }

    public void testToFilter() throws IOException {
        Directory dir = new RAMDirectory();
        try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())))) {
            writer.commit();
        }
        QueryShardContext context = mock(QueryShardContext.class);
        try (IndexReader reader = DirectoryReader.open(dir)) {
            MappedFieldType fieldType = new MappedFieldType() {
                @Override
                public MappedFieldType clone() {
                    return null;
                }

                @Override
                public String typeName() {
                    return null;
                }

                @Override
                public Query termQuery(Object value, @Nullable QueryShardContext context) {
                    return null;
                }
            };
            fieldType.setName(UidFieldMapper.NAME);
            fieldType.setHasDocValues(false);
            when(context.fieldMapper(UidFieldMapper.NAME)).thenReturn(fieldType);
            when(context.getIndexReader()).thenReturn(reader);
            SliceBuilder builder = new SliceBuilder(5, 10);
            Query query = builder.toFilter(context, 0, 1);
            assertThat(query, instanceOf(TermsSliceQuery.class));

            assertThat(builder.toFilter(context, 0, 1), equalTo(query));
            try (IndexReader newReader = DirectoryReader.open(dir)) {
                when(context.getIndexReader()).thenReturn(newReader);
                assertThat(builder.toFilter(context, 0, 1), equalTo(query));
            }
        }

        try (IndexReader reader = DirectoryReader.open(dir)) {
            MappedFieldType fieldType = new MappedFieldType() {
                @Override
                public MappedFieldType clone() {
                    return null;
                }

                @Override
                public String typeName() {
                    return null;
                }

                @Override
                public Query termQuery(Object value, @Nullable QueryShardContext context) {
                    return null;
                }
            };
            fieldType.setName("field_doc_values");
            fieldType.setHasDocValues(true);
            fieldType.setDocValuesType(DocValuesType.SORTED_NUMERIC);
            when(context.fieldMapper("field_doc_values")).thenReturn(fieldType);
            when(context.getIndexReader()).thenReturn(reader);
            IndexNumericFieldData fd = mock(IndexNumericFieldData.class);
            when(context.getForField(fieldType)).thenReturn(fd);
            SliceBuilder builder = new SliceBuilder("field_doc_values", 5, 10);
            Query query = builder.toFilter(context, 0, 1);
            assertThat(query, instanceOf(DocValuesSliceQuery.class));

            assertThat(builder.toFilter(context, 0, 1), equalTo(query));
            try (IndexReader newReader = DirectoryReader.open(dir)) {
                when(context.getIndexReader()).thenReturn(newReader);
                assertThat(builder.toFilter(context, 0, 1), equalTo(query));
            }

            // numSlices > numShards
            int numSlices = randomIntBetween(10, 100);
            int numShards = randomIntBetween(1, 9);
            Map<Integer, AtomicInteger> numSliceMap = new HashMap<>();
            for (int i = 0; i < numSlices; i++) {
                for (int j = 0; j < numShards; j++) {
                    SliceBuilder slice = new SliceBuilder("_uid", i, numSlices);
                    Query q = slice.toFilter(context, j, numShards);
                    if (q instanceof TermsSliceQuery || q instanceof MatchAllDocsQuery) {
                        AtomicInteger count = numSliceMap.get(j);
                        if (count == null) {
                            count = new AtomicInteger(0);
                            numSliceMap.put(j, count);
                        }
                        count.incrementAndGet();
                        if (q instanceof MatchAllDocsQuery) {
                            assertThat(count.get(), equalTo(1));
                        }
                    } else {
                        assertThat(q, instanceOf(MatchNoDocsQuery.class));
                    }
                }
            }
            int total = 0;
            for (Map.Entry<Integer, AtomicInteger> e : numSliceMap.entrySet()) {
                total += e.getValue().get();
            }
            assertThat(total, equalTo(numSlices));

            // numShards > numSlices
            numShards = randomIntBetween(4, 100);
            numSlices = randomIntBetween(2, numShards-1);
            List<Integer> targetShards = new ArrayList<>();
            for (int i = 0; i < numSlices; i++) {
                for (int j = 0; j < numShards; j++) {
                    SliceBuilder slice = new SliceBuilder("_uid", i, numSlices);
                    Query q = slice.toFilter(context, j, numShards);
                    if (q instanceof MatchNoDocsQuery == false) {
                        assertThat(q, instanceOf(MatchAllDocsQuery.class));
                        targetShards.add(j);
                    }
                }
            }
            assertThat(targetShards.size(), equalTo(numShards));
            assertThat(new HashSet<>(targetShards).size(), equalTo(numShards));

            // numShards == numSlices
            numShards = randomIntBetween(2, 10);
            numSlices = numShards;
            for (int i = 0; i < numSlices; i++) {
                for (int j = 0; j < numShards; j++) {
                    SliceBuilder slice = new SliceBuilder("_uid", i, numSlices);
                    Query q = slice.toFilter(context, j, numShards);
                    if (i == j) {
                        assertThat(q, instanceOf(MatchAllDocsQuery.class));
                    } else {
                        assertThat(q, instanceOf(MatchNoDocsQuery.class));
                    }
                }
            }
        }

        try (IndexReader reader = DirectoryReader.open(dir)) {
            MappedFieldType fieldType = new MappedFieldType() {
                @Override
                public MappedFieldType clone() {
                    return null;
                }

                @Override
                public String typeName() {
                    return null;
                }

                @Override
                public Query termQuery(Object value, @Nullable QueryShardContext context) {
                    return null;
                }
            };
            fieldType.setName("field_without_doc_values");
            when(context.fieldMapper("field_without_doc_values")).thenReturn(fieldType);
            when(context.getIndexReader()).thenReturn(reader);
            SliceBuilder builder = new SliceBuilder("field_without_doc_values", 5, 10);
            IllegalArgumentException exc =
                expectThrows(IllegalArgumentException.class, () -> builder.toFilter(context, 0, 1));
            assertThat(exc.getMessage(), containsString("cannot load numeric doc values"));
        }
    }
}
