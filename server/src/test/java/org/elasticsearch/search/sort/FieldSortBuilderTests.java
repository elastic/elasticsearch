/*
x * Licensed to Elasticsearch under one or more contributor
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

package org.elasticsearch.search.sort;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.search.sort.FieldSortBuilder.getMinMaxOrNull;
import static org.elasticsearch.search.sort.FieldSortBuilder.getPrimaryFieldSortOrNull;
import static org.elasticsearch.search.sort.NestedSortBuilderTests.createRandomNestedSort;
import static org.hamcrest.Matchers.instanceOf;

public class FieldSortBuilderTests extends AbstractSortTestCase<FieldSortBuilder> {

    /**
     * {@link #provideMappedFieldType(String)} will return a
     */
    private static String MAPPED_STRING_FIELDNAME = "_stringField";

    @Override
    protected FieldSortBuilder createTestItem() {
        return randomFieldSortBuilder();
    }

    private List<Object> missingContent = Arrays.asList(
            "_last",
            "_first",
            Integer.toString(randomInt()),
            randomInt());


    public FieldSortBuilder randomFieldSortBuilder() {
        String fieldName = rarely() ? FieldSortBuilder.DOC_FIELD_NAME : randomAlphaOfLengthBetween(1, 10);
        FieldSortBuilder builder = new FieldSortBuilder(fieldName);
        if (randomBoolean()) {
            builder.order(randomFrom(SortOrder.values()));
        }

        if (randomBoolean()) {
            builder.missing(randomFrom(missingContent));
        }

        if (randomBoolean()) {
            builder.unmappedType(randomAlphaOfLengthBetween(1, 10));
        }

        if (randomBoolean()) {
            builder.sortMode(randomFrom(SortMode.values()));
        }
        if (randomBoolean()) {
            builder.setNestedSort(createRandomNestedSort(3));
        }
        if (randomBoolean()) {
            builder.setNumericType(randomFrom(random(), "long", "double", "date", "date_nanos"));
        }
        return builder;
    }

    @Override
    protected FieldSortBuilder mutate(FieldSortBuilder original) throws IOException {
        FieldSortBuilder mutated = new FieldSortBuilder(original);
        int parameter = randomIntBetween(0, 5);
        switch (parameter) {
        case 0:
            mutated.setNestedSort(
                randomValueOtherThan(original.getNestedSort(), () -> NestedSortBuilderTests.createRandomNestedSort(3)));
            break;
        case 1:
            mutated.sortMode(randomValueOtherThan(original.sortMode(), () -> randomFrom(SortMode.values())));
            break;
        case 2:
            mutated.unmappedType(randomValueOtherThan(
                    original.unmappedType(),
                    () -> randomAlphaOfLengthBetween(1, 10)));
            break;
        case 3:
            mutated.missing(randomValueOtherThan(original.missing(), () -> randomFrom(missingContent)));
            break;
        case 4:
            mutated.order(randomValueOtherThan(original.order(), () -> randomFrom(SortOrder.values())));
            break;
        case 5:
            mutated.setNumericType(randomValueOtherThan(original.getNumericType(),
                () -> randomFrom("long", "double", "date", "date_nanos")));
            break;
        default:
            throw new IllegalStateException("Unsupported mutation.");
        }
        return mutated;
    }

    @Override
    protected void sortFieldAssertions(FieldSortBuilder builder, SortField sortField, DocValueFormat format) throws IOException {
        SortField.Type expectedType;
        if (builder.getFieldName().equals(FieldSortBuilder.DOC_FIELD_NAME)) {
            expectedType = SortField.Type.DOC;
        } else {
            expectedType = SortField.Type.CUSTOM;
        }
        assertEquals(expectedType, sortField.getType());
        assertEquals(builder.order() == SortOrder.ASC ? false : true, sortField.getReverse());
        if (expectedType == SortField.Type.CUSTOM) {
            assertEquals(builder.getFieldName(), sortField.getField());
        }
        assertEquals(DocValueFormat.RAW, format);
    }

    /**
     * Test that missing values get transferred correctly to the SortField
     */
    public void testBuildSortFieldMissingValue() throws IOException {
        QueryShardContext shardContextMock = createMockShardContext();
        FieldSortBuilder fieldSortBuilder = new FieldSortBuilder("value").missing("_first");
        SortField sortField = fieldSortBuilder.build(shardContextMock).field;
        SortedNumericSortField expectedSortField = new SortedNumericSortField("value", SortField.Type.DOUBLE);
        expectedSortField.setMissingValue(Double.NEGATIVE_INFINITY);
        assertEquals(expectedSortField, sortField);

        fieldSortBuilder = new FieldSortBuilder("value").missing("_last");
        sortField = fieldSortBuilder.build(shardContextMock).field;
        expectedSortField = new SortedNumericSortField("value", SortField.Type.DOUBLE);
        expectedSortField.setMissingValue(Double.POSITIVE_INFINITY);
        assertEquals(expectedSortField, sortField);

        Double randomDouble = randomDouble();
        fieldSortBuilder = new FieldSortBuilder("value").missing(randomDouble);
        sortField = fieldSortBuilder.build(shardContextMock).field;
        expectedSortField = new SortedNumericSortField("value", SortField.Type.DOUBLE);
        expectedSortField.setMissingValue(randomDouble);
        assertEquals(expectedSortField, sortField);

        fieldSortBuilder = new FieldSortBuilder("value").missing(randomDouble.toString());
        sortField = fieldSortBuilder.build(shardContextMock).field;
        expectedSortField = new SortedNumericSortField("value", SortField.Type.DOUBLE);
        expectedSortField.setMissingValue(randomDouble);
        assertEquals(expectedSortField, sortField);
    }

    /**
     * Test that the sort builder order gets transferred correctly to the SortField
     */
    public void testBuildSortFieldOrder() throws IOException {
        QueryShardContext shardContextMock = createMockShardContext();
        FieldSortBuilder fieldSortBuilder = new FieldSortBuilder("value");
        SortField sortField = fieldSortBuilder.build(shardContextMock).field;
        SortedNumericSortField expectedSortField = new SortedNumericSortField("value", SortField.Type.DOUBLE, false);
        expectedSortField.setMissingValue(Double.POSITIVE_INFINITY);
        assertEquals(expectedSortField, sortField);

        fieldSortBuilder = new FieldSortBuilder("value").order(SortOrder.ASC);
        sortField = fieldSortBuilder.build(shardContextMock).field;
        expectedSortField = new SortedNumericSortField("value", SortField.Type.DOUBLE, false);
        expectedSortField.setMissingValue(Double.POSITIVE_INFINITY);
        assertEquals(expectedSortField, sortField);

        fieldSortBuilder = new FieldSortBuilder("value").order(SortOrder.DESC);
        sortField = fieldSortBuilder.build(shardContextMock).field;
        expectedSortField = new SortedNumericSortField("value", SortField.Type.DOUBLE, true, SortedNumericSelector.Type.MAX);
        expectedSortField.setMissingValue(Double.NEGATIVE_INFINITY);
        assertEquals(expectedSortField, sortField);
    }

    /**
     * Test that the sort builder mode gets transferred correctly to the SortField
     */
    public void testMultiValueMode() throws IOException {
        QueryShardContext shardContextMock = createMockShardContext();

        FieldSortBuilder sortBuilder = new FieldSortBuilder("value").sortMode(SortMode.MIN);
        SortField sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField, instanceOf(SortedNumericSortField.class));
        SortedNumericSortField numericSortField = (SortedNumericSortField) sortField;
        assertEquals(SortedNumericSelector.Type.MIN, numericSortField.getSelector());

        sortBuilder = new FieldSortBuilder("value").sortMode(SortMode.MAX);
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField, instanceOf(SortedNumericSortField.class));
        numericSortField = (SortedNumericSortField) sortField;
        assertEquals(SortedNumericSelector.Type.MAX, numericSortField.getSelector());

        sortBuilder = new FieldSortBuilder("value").sortMode(SortMode.SUM);
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.SUM, comparatorSource.sortMode());

        sortBuilder = new FieldSortBuilder("value").sortMode(SortMode.AVG);
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.AVG, comparatorSource.sortMode());

        sortBuilder = new FieldSortBuilder("value").sortMode(SortMode.MEDIAN);
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.MEDIAN, comparatorSource.sortMode());

        // sort mode should also be set by build() implicitly to MIN or MAX if not set explicitly on builder
        sortBuilder = new FieldSortBuilder("value");
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField, instanceOf(SortedNumericSortField.class));
        numericSortField = (SortedNumericSortField) sortField;
        assertEquals(SortedNumericSelector.Type.MIN, numericSortField.getSelector());

        sortBuilder = new FieldSortBuilder("value").order(SortOrder.DESC);
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField, instanceOf(SortedNumericSortField.class));
        numericSortField = (SortedNumericSortField) sortField;
        assertEquals(SortedNumericSelector.Type.MAX, numericSortField.getSelector());
    }

    /**
     * Test that the sort builder nested object gets created in the SortField
     */
    public void testBuildNested() throws IOException {
        QueryShardContext shardContextMock = createMockShardContext();

        FieldSortBuilder sortBuilder = new FieldSortBuilder("fieldName")
                .setNestedSort(new NestedSortBuilder("path").setFilter(QueryBuilders.termQuery(MAPPED_STRING_FIELDNAME, "value")));
        SortField sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        Nested nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(new TermQuery(new Term(MAPPED_STRING_FIELDNAME, "value")), nested.getInnerQuery());

        NestedSortBuilder nestedSort = new NestedSortBuilder("path");
        sortBuilder = new FieldSortBuilder("fieldName").setNestedSort(nestedSort);
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(new TermQuery(new Term(NestedPathFieldMapper.NAME, "path")), nested.getInnerQuery());

        nestedSort.setFilter(QueryBuilders.termQuery(MAPPED_STRING_FIELDNAME, "value"));
        sortBuilder = new FieldSortBuilder("fieldName").setNestedSort(nestedSort);
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(new TermQuery(new Term(MAPPED_STRING_FIELDNAME, "value")), nested.getInnerQuery());
    }

    public void testUnknownOptionFails() throws IOException {
        String json = "{ \"post_date\" : {\"reverse\" : true} },\n";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            // need to skip until parser is located on second START_OBJECT
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();

            XContentParseException e = expectThrows(XContentParseException.class, () -> FieldSortBuilder.fromXContent(parser, ""));
            assertEquals("[1:18] [field_sort] unknown field [reverse]", e.getMessage());
        }
    }

    @Override
    protected MappedFieldType provideMappedFieldType(String name) {
        if (name.equals(MAPPED_STRING_FIELDNAME)) {
            KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType();
            fieldType.setName(name);
            fieldType.setHasDocValues(true);
            return fieldType;
        } else if (name.startsWith("custom-")) {
            final MappedFieldType fieldType;
            if (name.startsWith("custom-keyword")) {
                fieldType = new KeywordFieldMapper.KeywordFieldType();
            } else if (name.startsWith("custom-date")) {
                fieldType = new DateFieldMapper.DateFieldType();
            } else {
                String type = name.split("-")[1];
                if (type.equals("INT")) {
                    type = "integer";
                }
                NumberFieldMapper.NumberType numberType = NumberFieldMapper.NumberType.valueOf(type.toUpperCase(Locale.ENGLISH));
                if (numberType != null) {
                    fieldType = new NumberFieldMapper.NumberFieldType(numberType);
                } else {
                    fieldType = new KeywordFieldMapper.KeywordFieldType();
                }
            }
            fieldType.setName(name);
            fieldType.setHasDocValues(true);
            if (name.endsWith("-ni")) {
                fieldType.setIndexOptions(IndexOptions.NONE);
            } else {
                fieldType.setIndexOptions(IndexOptions.DOCS);
            }
            return fieldType;
        } else {
            return super.provideMappedFieldType(name);
        }
    }

    /**
     * Test that MIN, MAX mode work on non-numeric fields, but other modes throw exception
     */
    public void testModeNonNumericField() throws IOException {
        QueryShardContext shardContextMock = createMockShardContext();

        FieldSortBuilder sortBuilder = new FieldSortBuilder(MAPPED_STRING_FIELDNAME).sortMode(SortMode.MIN);
        SortField sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField, instanceOf(SortedSetSortField.class));
        assertEquals(SortedSetSelector.Type.MIN, ((SortedSetSortField) sortField).getSelector());

        sortBuilder = new FieldSortBuilder(MAPPED_STRING_FIELDNAME).sortMode(SortMode.MAX);
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField, instanceOf(SortedSetSortField.class));
        assertEquals(SortedSetSelector.Type.MAX, ((SortedSetSortField) sortField).getSelector());

        String expectedError = "we only support AVG, MEDIAN and SUM on number based fields";
        QueryShardException e = expectThrows(QueryShardException.class,
                () -> new FieldSortBuilder(MAPPED_STRING_FIELDNAME).sortMode(SortMode.AVG).build(shardContextMock));
        assertEquals(expectedError, e.getMessage());

        e = expectThrows(QueryShardException.class,
                () -> new FieldSortBuilder(MAPPED_STRING_FIELDNAME).sortMode(SortMode.SUM).build(shardContextMock));
        assertEquals(expectedError, e.getMessage());

        e = expectThrows(QueryShardException.class,
                () -> new FieldSortBuilder(MAPPED_STRING_FIELDNAME).sortMode(SortMode.MEDIAN).build(shardContextMock));
        assertEquals(expectedError, e.getMessage());
    }

    /**
     * Test the nested Filter gets rewritten
     */
    public void testNestedRewrites() throws IOException {
        FieldSortBuilder sortBuilder = new FieldSortBuilder(MAPPED_STRING_FIELDNAME);
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("fieldName") {
            @Override
            public QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
                return new MatchNoneQueryBuilder();
            }
        };
        NestedSortBuilder nestedSort = new NestedSortBuilder("path");
        nestedSort.setFilter(rangeQuery);
        sortBuilder.setNestedSort(nestedSort);
        FieldSortBuilder rewritten = sortBuilder
                .rewrite(createMockShardContext());
        assertNotSame(rangeQuery, rewritten.getNestedSort().getFilter());
    }

    /**
     * Test the nested sort gets rewritten
     */
    public void testNestedSortRewrites() throws IOException {
        FieldSortBuilder sortBuilder = new FieldSortBuilder(MAPPED_STRING_FIELDNAME);
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("fieldName") {
            @Override
            public QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
                return new MatchNoneQueryBuilder();
            }
        };
        sortBuilder.setNestedSort(new NestedSortBuilder("path").setFilter(rangeQuery));
        FieldSortBuilder rewritten = sortBuilder
                .rewrite(createMockShardContext());
        assertNotSame(rangeQuery, rewritten.getNestedSort().getFilter());
    }

    public void testGetPrimaryFieldSort() {
        assertNull(getPrimaryFieldSortOrNull(null));
        assertNull(getPrimaryFieldSortOrNull(new SearchSourceBuilder()));
        assertNull(getPrimaryFieldSortOrNull(new SearchSourceBuilder().sort(SortBuilders.scoreSort())));
        FieldSortBuilder sortBuilder = new FieldSortBuilder(MAPPED_STRING_FIELDNAME);
        assertEquals(sortBuilder, getPrimaryFieldSortOrNull(new SearchSourceBuilder().sort(sortBuilder)));
        assertNull(getPrimaryFieldSortOrNull(new SearchSourceBuilder()
            .sort(SortBuilders.scoreSort()).sort(sortBuilder)));
        assertNull(getPrimaryFieldSortOrNull(new SearchSourceBuilder()
            .sort(SortBuilders.geoDistanceSort("field", 0d, 0d)).sort(sortBuilder)));
    }

    public void testGetMaxNumericSortValue() throws IOException {
        QueryShardContext context = createMockShardContext();
        for (NumberFieldMapper.NumberType numberType : NumberFieldMapper.NumberType.values()) {
            String fieldName = "custom-" + numberType.numericType();
            assertNull(getMinMaxOrNull(context, SortBuilders.fieldSort(fieldName)));
            assertNull(getMinMaxOrNull(context, SortBuilders.fieldSort(fieldName + "-ni")));

            try (Directory dir = newDirectory()) {
                int numDocs = randomIntBetween(10, 30);
                final Comparable[] values = new Comparable[numDocs];
                try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                    for (int i = 0; i < numDocs; i++) {
                        Document doc = new Document();
                        switch (numberType) {
                            case LONG:
                                long v1 = randomLong();
                                values[i] = v1;
                                doc.add(new LongPoint(fieldName, v1));
                                break;

                            case INTEGER:
                                int v2 = randomInt();
                                values[i] = (long) v2;
                                doc.add(new IntPoint(fieldName, v2));
                                break;

                            case DOUBLE:
                                double v3 = randomDouble();
                                values[i] = v3;
                                doc.add(new DoublePoint(fieldName, v3));
                                break;

                            case FLOAT:
                                float v4 = randomFloat();
                                values[i] = v4;
                                doc.add(new FloatPoint(fieldName, v4));
                                break;

                            case HALF_FLOAT:
                                float v5 = randomFloat();
                                values[i] = (double) v5;
                                doc.add(new HalfFloatPoint(fieldName, v5));
                                break;

                            case BYTE:
                                byte v6 = randomByte();
                                values[i] = (long) v6;
                                doc.add(new IntPoint(fieldName, v6));
                                break;

                            case SHORT:
                                short v7 = randomShort();
                                values[i] = (long) v7;
                                doc.add(new IntPoint(fieldName, v7));
                                break;

                            default:
                                throw new AssertionError("unknown type " + numberType);
                        }
                        writer.addDocument(doc);
                    }
                    Arrays.sort(values);
                    try (DirectoryReader reader = writer.getReader()) {
                        QueryShardContext newContext = createMockShardContext(new AssertingIndexSearcher(random(), reader));
                        if (numberType == NumberFieldMapper.NumberType.HALF_FLOAT) {
                            assertNull(getMinMaxOrNull(newContext, SortBuilders.fieldSort(fieldName + "-ni")));
                            assertNull(getMinMaxOrNull(newContext, SortBuilders.fieldSort(fieldName)));
                        } else {
                            assertNull(getMinMaxOrNull(newContext, SortBuilders.fieldSort(fieldName + "-ni")));
                            assertEquals(values[numDocs - 1],
                                getMinMaxOrNull(newContext, SortBuilders.fieldSort(fieldName)).getMax());
                            assertEquals(values[0], getMinMaxOrNull(newContext, SortBuilders.fieldSort(fieldName)).getMin());
                        }
                    }
                }
            }
        }
    }

    public void testGetMaxNumericDateValue() throws IOException {
        QueryShardContext context = createMockShardContext();
        String fieldName = "custom-date";
        assertNull(getMinMaxOrNull(context, SortBuilders.fieldSort(fieldName)));
        assertNull(getMinMaxOrNull(context, SortBuilders.fieldSort(fieldName + "-ni")));
        try (Directory dir = newDirectory()) {
            int numDocs = randomIntBetween(10, 30);
            final long[] values = new long[numDocs];
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    values[i] = randomNonNegativeLong();
                    doc.add(new LongPoint(fieldName, values[i]));
                    writer.addDocument(doc);
                }
                Arrays.sort(values);
                try (DirectoryReader reader = writer.getReader()) {
                    QueryShardContext newContext = createMockShardContext(new AssertingIndexSearcher(random(), reader));
                    assertEquals(values[numDocs - 1], getMinMaxOrNull(newContext, SortBuilders.fieldSort(fieldName)).getMax());
                    assertEquals(values[0], getMinMaxOrNull(newContext, SortBuilders.fieldSort(fieldName)).getMin());
                }
            }
        }
    }

    public void testGetMaxKeywordValue() throws IOException {
        QueryShardContext context = createMockShardContext();
        String fieldName = "custom-keyword";
        assertNull(getMinMaxOrNull(context, SortBuilders.fieldSort(fieldName)));
        assertNull(getMinMaxOrNull(context, SortBuilders.fieldSort(fieldName + "-ni")));
        try (Directory dir = newDirectory()) {
            int numDocs = randomIntBetween(10, 30);
            final BytesRef[] values = new BytesRef[numDocs];
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, new KeywordAnalyzer())) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    values[i] = new BytesRef(randomAlphaOfLengthBetween(5, 10));
                    doc.add(new TextField(fieldName, values[i].utf8ToString(), Field.Store.NO));
                    writer.addDocument(doc);
                }
                Arrays.sort(values);
                try (DirectoryReader reader = writer.getReader()) {
                    QueryShardContext newContext = createMockShardContext(new AssertingIndexSearcher(random(), reader));
                    assertEquals(values[numDocs - 1], getMinMaxOrNull(newContext, SortBuilders.fieldSort(fieldName)).getMax());
                    assertEquals(values[0], getMinMaxOrNull(newContext, SortBuilders.fieldSort(fieldName)).getMin());
                }
            }
        }
    }

    @Override
    protected FieldSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        return FieldSortBuilder.fromXContent(parser, fieldName);
    }
}
