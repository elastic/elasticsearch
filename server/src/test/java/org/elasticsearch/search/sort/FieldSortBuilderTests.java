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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
        assertEquals(new TermQuery(new Term(TypeFieldMapper.NAME, "__path")), nested.getInnerQuery());

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
            assertEquals("[1:18] [field_sort] unknown field [reverse], parser not found", e.getMessage());
        }
    }

    @Override
    protected MappedFieldType provideMappedFieldType(String name) {
        if (name.equals(MAPPED_STRING_FIELDNAME)) {
            KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType();
            fieldType.setName(name);
            fieldType.setHasDocValues(true);
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

    @Override
    protected FieldSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        return FieldSortBuilder.fromXContent(parser, fieldName);
    }
}
