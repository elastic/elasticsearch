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

package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.TypeFieldType;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.CoreMatchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.instanceOf;

public class TermsQueryBuilderTests extends AbstractQueryTestCase<TermsQueryBuilder> {
    private List<Object> randomTerms;
    private String termsPath;

    @Before
    public void randomTerms() {
        List<Object> randomTerms = new ArrayList<>();
        String[] strings = generateRandomStringArray(10, 10, false, true);
        for (String string : strings) {
            randomTerms.add(string);
            if (rarely()) {
                randomTerms.add(null);
            }
        }
        this.randomTerms = randomTerms;
        termsPath = randomAlphaOfLength(10).replace('.', '_');
    }

    @Override
    protected TermsQueryBuilder doCreateTestQueryBuilder() {
        TermsQueryBuilder query;
        // terms query or lookup query
        if (randomBoolean()) {
            // make between 0 and 5 different values of the same type
            String fieldName = randomValueOtherThanMany(choice ->
                    choice.equals(GEO_POINT_FIELD_NAME) ||
                    choice.equals(GEO_POINT_ALIAS_FIELD_NAME) ||
                    choice.equals(GEO_SHAPE_FIELD_NAME) ||
                    choice.equals(INT_RANGE_FIELD_NAME) ||
                    choice.equals(DATE_RANGE_FIELD_NAME) ||
                    choice.equals(DATE_NANOS_FIELD_NAME), // TODO: needs testing for date_nanos type
                () -> getRandomFieldName());
            Object[] values = new Object[randomInt(5)];
            for (int i = 0; i < values.length; i++) {
                values[i] = getRandomValueForFieldName(fieldName);
            }
            query = new TermsQueryBuilder(fieldName, values);
        } else {
            // right now the mock service returns us a list of strings
            query = new TermsQueryBuilder(randomBoolean() ? randomAlphaOfLengthBetween(1,10) : TEXT_FIELD_NAME, randomTermsLookup());
        }
        return query;
    }

    private TermsLookup randomTermsLookup() {
        TermsLookup lookup = new TermsLookup(randomAlphaOfLength(10), randomAlphaOfLength(10), termsPath);
        lookup.routing(randomBoolean() ? randomAlphaOfLength(10) : null);
        return lookup;
    }

    @Override
    protected void doAssertLuceneQuery(TermsQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (queryBuilder.termsLookup() == null && (queryBuilder.values() == null || queryBuilder.values().isEmpty())) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else if (queryBuilder.termsLookup() != null && randomTerms.size() == 0){
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, either(instanceOf(TermInSetQuery.class))
                    .or(instanceOf(PointInSetQuery.class))
                    .or(instanceOf(ConstantScoreQuery.class))
                    .or(instanceOf(MatchNoDocsQuery.class)));
            if (query instanceof ConstantScoreQuery) {
                assertThat(((ConstantScoreQuery) query).getQuery(), instanceOf(BooleanQuery.class));
            }

            // we only do the check below for string fields (otherwise we'd have to decode the values)
            if (queryBuilder.fieldName().equals(INT_FIELD_NAME) || queryBuilder.fieldName().equals(DOUBLE_FIELD_NAME)
                    || queryBuilder.fieldName().equals(BOOLEAN_FIELD_NAME) || queryBuilder.fieldName().equals(DATE_FIELD_NAME)) {
                return;
            }

            // expected returned terms depending on whether we have a terms query or a terms lookup query
            List<Object> terms;
            if (queryBuilder.termsLookup() != null) {
                terms = randomTerms;
            } else {
                terms = queryBuilder.values();
            }

            String fieldName = expectedFieldName(queryBuilder.fieldName());
            Query expected;
            if (context.getFieldType(fieldName) != null) {
                expected = new TermInSetQuery(fieldName,
                        terms.stream().filter(Objects::nonNull).map(Object::toString).map(BytesRef::new).collect(Collectors.toList()));
            } else {
                expected = new MatchNoDocsQuery();
            }
            assertEquals(expected, query);
        }
    }

    public void testEmtpyFieldName() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder(null, "term"));
        assertEquals("field name cannot be null.", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("", "term"));
        assertEquals("field name cannot be null.", e.getMessage());
    }

    public void testEmtpyTermsLookup() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (TermsLookup) null));
        assertEquals("No value or termsLookup specified for terms query", e.getMessage());
    }

    public void testNullValues() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (String[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (int[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (long[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (float[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (double[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (Object[]) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
        e = expectThrows(IllegalArgumentException.class, () -> new TermsQueryBuilder("field", (Iterable<?>) null));
        assertThat(e.getMessage(), containsString("No value specified for terms query"));
    }

    public void testBothValuesAndLookupSet() throws IOException {
        String query = "{\n" +
                "  \"terms\": {\n" +
                "    \"field\": [\n" +
                "      \"blue\",\n" +
                "      \"pill\"\n" +
                "    ],\n" +
                "    \"field_lookup\": {\n" +
                "      \"index\": \"pills\",\n" +
                "      \"type\": \"red\",\n" +
                "      \"id\": \"3\",\n" +
                "      \"path\": \"white rabbit\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertThat(e.getMessage(), containsString("[" + TermsQueryBuilder.NAME + "] query does not support more than one field."));
    }

    @Override
    public GetResponse executeGet(GetRequest getRequest) {
        String json;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            builder.array(termsPath, randomTerms.toArray(new Object[randomTerms.size()]));
            builder.endObject();
            json = Strings.toString(builder);
        } catch (IOException ex) {
            throw new ElasticsearchException("boom", ex);
        }
        return new GetResponse(new GetResult(getRequest.index(), getRequest.id(), 0, 1, 0, true,
            new BytesArray(json), null, null));
    }

    public void testNumeric() throws IOException {
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new int[]{1, 3, 4});
            TermsQueryBuilder copy = (TermsQueryBuilder) assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1, 3, 4), values);
            assertFalse(copy.isStale());
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new double[]{1, 3, 4});
            TermsQueryBuilder copy = (TermsQueryBuilder) assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1d, 3d, 4d), values);
            assertFalse(copy.isStale());
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new float[]{1, 3, 4});
            TermsQueryBuilder copy = (TermsQueryBuilder) assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1f, 3f, 4f), values);
            assertFalse(copy.isStale());
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new long[]{1, 3, 4});
            TermsQueryBuilder copy = (TermsQueryBuilder) assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1L, 3L, 4L), values);
            assertFalse(copy.isStale());
        }
    }

    public void testTermsQueryWithMultipleFields() throws IOException {
        String query = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("terms").array("foo", 123).array("bar", 456).endObject()
                .endObject());
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertEquals("[" + TermsQueryBuilder.NAME + "] query does not support multiple fields", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"terms\" : {\n" +
                "    \"user\" : [ \"kimchy\", \"elasticsearch\" ],\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        TermsQueryBuilder parsed = (TermsQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 2, parsed.values().size());
    }

    @Override
    public void testMustRewrite() throws IOException {
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder(TEXT_FIELD_NAME, randomTermsLookup());
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
                () -> termsQueryBuilder.toQuery(createShardContext()));
        assertEquals("query must be rewritten first", e.getMessage());

        // terms lookup removes null values
        List<Object> nonNullTerms = randomTerms.stream().filter(x -> x != null).collect(Collectors.toList());
        QueryBuilder expected;
        if (nonNullTerms.isEmpty()) {
            expected = new MatchNoneQueryBuilder();
        } else {
            expected = new TermsQueryBuilder(TEXT_FIELD_NAME, nonNullTerms);
        }
        assertEquals(expected, rewriteAndFetch(termsQueryBuilder, createShardContext()));
    }

    public void testGeo() throws Exception {
        TermsQueryBuilder query = new TermsQueryBuilder(GEO_POINT_FIELD_NAME, "2,3");
        QueryShardContext context = createShardContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertEquals("Geometry fields do not support exact searching, use dedicated geometry queries instead: "
                + "[mapped_geo_point]", e.getMessage());
    }

    public void testSerializationFailsUnlessFetched() throws IOException {
        QueryBuilder builder = new TermsQueryBuilder(TEXT_FIELD_NAME, randomTermsLookup());
        QueryBuilder termsQueryBuilder = Rewriteable.rewrite(builder, createShardContext());
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> termsQueryBuilder.writeTo(new BytesStreamOutput(10)));
        assertEquals(ise.getMessage(), "supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        builder = rewriteAndFetch(builder, createShardContext());
        builder.writeTo(new BytesStreamOutput(10));
    }

    public void testTypeField() throws IOException {
        TermsQueryBuilder builder = QueryBuilders.termsQuery("_type", "value1", "value2");
        builder.doToQuery(createShardContext());
        assertWarnings(TypeFieldType.TYPES_V7_DEPRECATION_MESSAGE);
    }

    public void testRewriteIndexQueryToMatchNone() throws IOException {
        TermsQueryBuilder query = new TermsQueryBuilder("_index", "does_not_exist", "also_does_not_exist");
        QueryShardContext queryShardContext = createShardContext();
        QueryBuilder rewritten = query.rewrite(queryShardContext);
        assertThat(rewritten, instanceOf(MatchNoneQueryBuilder.class));
    }

    public void testRewriteIndexQueryToNotMatchNone() throws IOException {
        // At least one name is good
        TermsQueryBuilder query = new TermsQueryBuilder("_index", "does_not_exist", getIndex().getName());
        QueryShardContext queryShardContext = createShardContext();
        QueryBuilder rewritten = query.rewrite(queryShardContext);
        assertThat(rewritten, instanceOf(MatchAllQueryBuilder.class));
    }

    @Override
    protected QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder query = super.parseQuery(parser);
        assertThat(query, CoreMatchers.instanceOf(TermsQueryBuilder.class));
        return query;
    }

    public void testSerializationFromLowerVersion() throws IOException {
        {
            TermsQueryBuilderBeforeV8 builder = new TermsQueryBuilderBeforeV8("foo", Arrays.asList(1, 3, 4), null);
            TermsQueryBuilder copy = (TermsQueryBuilder) serializeAndDeserialize(Version.V_7_12_0, builder, Version.V_8_0_0, TermsQueryBuilder::new);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1L, 3L, 4L), values);
            assertTrue(copy.isStale());
        }
        {
            TermsQueryBuilderBeforeV8 builder = new TermsQueryBuilderBeforeV8("foo", Arrays.asList(1L, 3L, 4L), null);
            TermsQueryBuilder copy = (TermsQueryBuilder) serializeAndDeserialize(Version.V_7_12_0, builder, Version.V_8_0_0, TermsQueryBuilder::new);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1L, 3L, 4L), values);
            assertTrue(copy.isStale());
        }
        {
            TermsQueryBuilderBeforeV8 builder = new TermsQueryBuilderBeforeV8("foo", Arrays.asList("a", "b", "c"), null);
            TermsQueryBuilder copy = (TermsQueryBuilder) serializeAndDeserialize(Version.V_7_12_0, builder, Version.V_8_0_0, TermsQueryBuilder::new);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList("a", "b", "c"), values);
            assertTrue(copy.isStale());
        }
        {
            TermsQueryBuilderBeforeV8 builder = new TermsQueryBuilderBeforeV8("foo", Arrays.asList(new BytesRef("a"), new BytesRef("b"), new BytesRef("c")), null);
            TermsQueryBuilder copy = (TermsQueryBuilder) serializeAndDeserialize(Version.V_7_12_0, builder, Version.V_8_0_0, TermsQueryBuilder::new);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList("a", "b", "c"), values);
            assertTrue(copy.isStale());
        }
    }

    public void testSerializationToLowerVersion() throws IOException {
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new int[]{1, 3, 4});
            TermsQueryBuilderBeforeV8 copy = (TermsQueryBuilderBeforeV8) serializeAndDeserialize(Version.V_8_0_0, builder, Version.V_7_12_0, TermsQueryBuilderBeforeV8::new);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1, 3, 4), values);
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new long[]{1L, 3L, 4L});
            TermsQueryBuilderBeforeV8 copy = (TermsQueryBuilderBeforeV8) serializeAndDeserialize(Version.V_8_0_0, builder, Version.V_7_12_0, TermsQueryBuilderBeforeV8::new);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1L, 3L, 4L), values);
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", "a", "b", "c");
            TermsQueryBuilderBeforeV8 copy = (TermsQueryBuilderBeforeV8) serializeAndDeserialize(Version.V_8_0_0, builder, Version.V_7_12_0, TermsQueryBuilderBeforeV8::new);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList("a", "b", "c"), values);
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new BytesRef("a"), new BytesRef("b"), new BytesRef("c"));
            TermsQueryBuilderBeforeV8 copy = (TermsQueryBuilderBeforeV8) serializeAndDeserialize(Version.V_8_0_0, builder, Version.V_7_12_0, TermsQueryBuilderBeforeV8::new);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList("a", "b", "c"), values);
        }
    }

    private QueryBuilder serializeAndDeserialize(Version srcVersion, QueryBuilder srcQueryBuilder,
                                   Version dstVersion, Writeable.Reader<QueryBuilder> dstReader) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(dstVersion);
            srcQueryBuilder.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(srcVersion);
                return dstReader.read(in);
            }
        }
    }

    /**
     * to test serialization compatible between different version
     */
    private static class TermsQueryBuilderBeforeV8 extends AbstractQueryBuilder<TermsQueryBuilderBeforeV8> {
        public static final String NAME = "terms_before_v8";

        private final String fieldName;
        private final List<?> values;
        private final TermsLookup termsLookup;
        private final Supplier<List<?>> supplier;

        TermsQueryBuilderBeforeV8(String fieldName, List<Object> values, TermsLookup termsLookup) {
            if (Strings.isEmpty(fieldName)) {
                throw new IllegalArgumentException("field name cannot be null.");
            }
            if (values == null && termsLookup == null) {
                throw new IllegalArgumentException("No value or termsLookup specified for terms query");
            }
            if (values != null && termsLookup != null) {
                throw new IllegalArgumentException("Both values and termsLookup specified for terms query");
            }
            this.fieldName = fieldName;
            this.values = values == null ? null : convert(values);
            this.termsLookup = termsLookup;
            this.supplier = null;
        }

        public TermsQueryBuilderBeforeV8(StreamInput in) throws IOException {
            super(in);
            fieldName = in.readString();
            termsLookup = in.readOptionalWriteable(TermsLookup::new);
            values = (List<?>) in.readGenericValue();
            this.supplier = null;
        }

        protected void doWriteTo(StreamOutput out) throws IOException {
            if (supplier != null) {
                throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
            }
            out.writeString(fieldName);
            out.writeOptionalWriteable(termsLookup);
            out.writeGenericValue(values);
        }

        static List<?> convert(List<?> list) {
            return TermsSetQueryBuilder.convert(list);
        }

        static List<Object> convertBack(List<?> list) {
            return TermsSetQueryBuilder.convertBack(list);
        }

        public List<Object> values() {
            return convertBack(this.values);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {

        }

        @Override
        protected Query doToQuery(QueryShardContext context) throws IOException {
            return null;
        }

        @Override
        protected boolean doEquals(TermsQueryBuilderBeforeV8 other) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }
    }
}
