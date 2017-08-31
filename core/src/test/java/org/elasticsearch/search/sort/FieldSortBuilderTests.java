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

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
            builder.setNestedFilter(randomNestedFilter());
        }

        if (randomBoolean()) {
            builder.setNestedPath(randomAlphaOfLengthBetween(1, 10));
        }

        return builder;
    }

    @Override
    protected FieldSortBuilder mutate(FieldSortBuilder original) throws IOException {
        FieldSortBuilder mutated = new FieldSortBuilder(original);
        int parameter = randomIntBetween(0, 5);
        switch (parameter) {
        case 0:
            mutated.setNestedPath(randomValueOtherThan(
                    original.getNestedPath(),
                    () -> randomAlphaOfLengthBetween(1, 10)));
            break;
        case 1:
            mutated.setNestedFilter(randomValueOtherThan(
                    original.getNestedFilter(),
                    () -> randomNestedFilter()));
            break;
        case 2:
            mutated.sortMode(randomValueOtherThan(original.sortMode(), () -> randomFrom(SortMode.values())));
            break;
        case 3:
            mutated.unmappedType(randomValueOtherThan(
                    original.unmappedType(),
                    () -> randomAlphaOfLengthBetween(1, 10)));
            break;
        case 4:
            mutated.missing(randomValueOtherThan(original.missing(), () -> randomFrom(missingContent)));
            break;
        case 5:
            mutated.order(randomValueOtherThan(original.order(), () -> randomFrom(SortOrder.values())));
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
     * Test that missing values get transfered correctly to the SortField
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
     * Test that the sort builder order gets transfered correctly to the SortField
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
     * Test that the sort builder mode gets transfered correctly to the SortField
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

        // sort mode should also be set by build() implicitely to MIN or MAX if not set explicitely on builder
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

        FieldSortBuilder sortBuilder = new FieldSortBuilder("value").setNestedPath("path");
        SortField sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertNotNull(comparatorSource.nested());

        sortBuilder = new FieldSortBuilder("value").setNestedPath("path").setNestedFilter(QueryBuilders.termQuery("field", 10.0));
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertNotNull(comparatorSource.nested());

        // if nested path is missing, we omit any filter and return a SortedNumericSortField
        sortBuilder = new FieldSortBuilder("value").setNestedFilter(QueryBuilders.termQuery("field", 10.0));
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField, instanceOf(SortedNumericSortField.class));
    }

    public void testUnknownOptionFails() throws IOException {
        String json = "{ \"post_date\" : {\"reverse\" : true} },\n";

        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        // need to skip until parser is located on second START_OBJECT
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FieldSortBuilder.fromXContent(parser, ""));
        assertEquals("[field_sort] unknown field [reverse], parser not found", e.getMessage());
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

    @Override
    protected FieldSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        return FieldSortBuilder.fromXContent(parser, fieldName);
    }
}
