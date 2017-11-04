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

package org.elasticsearch.search.sort;


import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.search.sort.NestedSortBuilderTests.createRandomNestedSort;
import static org.hamcrest.Matchers.instanceOf;

public class ScriptSortBuilderTests extends AbstractSortTestCase<ScriptSortBuilder> {

    @Override
    protected ScriptSortBuilder createTestItem() {
        return randomScriptSortBuilder();
    }

    public static ScriptSortBuilder randomScriptSortBuilder() {
        ScriptSortType type = randomBoolean() ? ScriptSortType.NUMBER : ScriptSortType.STRING;
        ScriptSortBuilder builder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME),
                type);
        if (randomBoolean()) {
                builder.order(randomFrom(SortOrder.values()));
        }
        if (randomBoolean()) {
            if (type == ScriptSortType.NUMBER) {
                builder.sortMode(randomValueOtherThan(builder.sortMode(), () -> randomFrom(SortMode.values())));
            } else {
                Set<SortMode> exceptThis = new HashSet<>();
                exceptThis.add(SortMode.SUM);
                exceptThis.add(SortMode.AVG);
                exceptThis.add(SortMode.MEDIAN);
                builder.sortMode(randomValueOtherThanMany(exceptThis::contains, () -> randomFrom(SortMode.values())));
            }
        }
        if (randomBoolean()) {
            builder.setNestedSort(createRandomNestedSort(3));
        }
        return builder;
    }

    @Override
    protected ScriptSortBuilder mutate(ScriptSortBuilder original) throws IOException {
        ScriptSortBuilder result;
        if (randomBoolean()) {
            // change one of the constructor args, copy the rest over
            Script script = original.script();
            ScriptSortType type = original.type();
            if (randomBoolean()) {
                result = new ScriptSortBuilder(mockScript(script.getIdOrCode() + "_suffix"), type);
            } else {
                result = new ScriptSortBuilder(script, type.equals(ScriptSortType.NUMBER) ? ScriptSortType.STRING : ScriptSortType.NUMBER);
            }
            result.order(original.order());
            if (original.sortMode() != null && result.type() == ScriptSortType.NUMBER) {
                result.sortMode(original.sortMode());
            }
            result.setNestedSort(original.getNestedSort());
            return result;
        }
        result = new ScriptSortBuilder(original);
        switch (randomIntBetween(0, 2)) {
            case 0:
                if (original.order() == SortOrder.ASC) {
                    result.order(SortOrder.DESC);
                } else {
                    result.order(SortOrder.ASC);
                }
                break;
            case 1:
                if (original.type() == ScriptSortType.NUMBER) {
                    result.sortMode(randomValueOtherThan(result.sortMode(), () -> randomFrom(SortMode.values())));
                } else {
                    // script sort type String only allows MIN and MAX, so we only switch
                    if (original.sortMode() == SortMode.MIN) {
                        result.sortMode(SortMode.MAX);
                    } else {
                        result.sortMode(SortMode.MIN);
                    }
                }
                break;
            case 2:
                result.setNestedSort(randomValueOtherThan(original.getNestedSort(),
                        () -> NestedSortBuilderTests.createRandomNestedSort(3)));
                break;
        }
        return result;
    }

    @Override
    protected void sortFieldAssertions(ScriptSortBuilder builder, SortField sortField, DocValueFormat format) throws IOException {
        assertEquals(SortField.Type.CUSTOM, sortField.getType());
        assertEquals(builder.order() == SortOrder.ASC ? false : true, sortField.getReverse());
    }

    public void testScriptSortType() {
        // we rely on these ordinals in serialization, so changing them breaks bwc.
        assertEquals(0, ScriptSortType.STRING.ordinal());
        assertEquals(1, ScriptSortType.NUMBER.ordinal());

        assertEquals("string", ScriptSortType.STRING.toString());
        assertEquals("number", ScriptSortType.NUMBER.toString());

        assertEquals(ScriptSortType.STRING, ScriptSortType.fromString("string"));
        assertEquals(ScriptSortType.STRING, ScriptSortType.fromString("String"));
        assertEquals(ScriptSortType.STRING, ScriptSortType.fromString("STRING"));
        assertEquals(ScriptSortType.NUMBER, ScriptSortType.fromString("number"));
        assertEquals(ScriptSortType.NUMBER, ScriptSortType.fromString("Number"));
        assertEquals(ScriptSortType.NUMBER, ScriptSortType.fromString("NUMBER"));
    }

    public void testScriptSortTypeNull() {
        Exception e = expectThrows(NullPointerException.class, () -> ScriptSortType.fromString(null));
        assertEquals("input string is null", e.getMessage());
    }

    public void testScriptSortTypeIllegalArgument() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> ScriptSortType.fromString("xyz"));
        assertEquals("Unknown ScriptSortType [xyz]", e.getMessage());
    }

    public void testParseJson() throws IOException {
        String scriptSort = "{\n" +
                "\"_script\" : {\n" +
                    "\"type\" : \"number\",\n" +
                    "\"script\" : {\n" +
                        "\"source\": \"doc['field_name'].value * factor\",\n" +
                        "\"params\" : {\n" +
                            "\"factor\" : 1.1\n" +
                            "}\n" +
                    "},\n" +
                    "\"mode\" : \"max\",\n" +
                    "\"order\" : \"asc\"\n" +
                "} }\n";
        XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        ScriptSortBuilder builder = ScriptSortBuilder.fromXContent(parser, null);
        assertEquals("doc['field_name'].value * factor", builder.script().getIdOrCode());
        assertEquals(Script.DEFAULT_SCRIPT_LANG, builder.script().getLang());
        assertEquals(1.1, builder.script().getParams().get("factor"));
        assertEquals(ScriptType.INLINE, builder.script().getType());
        assertEquals(ScriptSortType.NUMBER, builder.type());
        assertEquals(SortOrder.ASC, builder.order());
        assertEquals(SortMode.MAX, builder.sortMode());
        assertNull(builder.getNestedSort());
    }

    public void testParseJson_simple() throws IOException {
        String scriptSort = "{\n" +
                "\"_script\" : {\n" +
                "\"type\" : \"number\",\n" +
                "\"script\" : \"doc['field_name'].value\",\n" +
                "\"mode\" : \"max\",\n" +
                "\"order\" : \"asc\"\n" +
                "} }\n";
        XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        ScriptSortBuilder builder = ScriptSortBuilder.fromXContent(parser, null);
        assertEquals("doc['field_name'].value", builder.script().getIdOrCode());
        assertEquals(Script.DEFAULT_SCRIPT_LANG, builder.script().getLang());
        assertEquals(builder.script().getParams(), Collections.emptyMap());
        assertEquals(ScriptType.INLINE, builder.script().getType());
        assertEquals(ScriptSortType.NUMBER, builder.type());
        assertEquals(SortOrder.ASC, builder.order());
        assertEquals(SortMode.MAX, builder.sortMode());
        assertNull(builder.getNestedSort());
    }

    public void testParseBadFieldNameExceptions() throws IOException {
        String scriptSort = "{\"_script\" : {" + "\"bad_field\" : \"number\"" + "} }";
        XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        Exception e = expectThrows(IllegalArgumentException.class, () -> ScriptSortBuilder.fromXContent(parser, null));
        assertEquals("[_script] unknown field [bad_field], parser not found", e.getMessage());
    }

    public void testParseBadFieldNameExceptionsOnStartObject() throws IOException {

        String scriptSort = "{\"_script\" : {" + "\"bad_field\" : { \"order\" : \"asc\" } } }";
        XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        Exception e = expectThrows(IllegalArgumentException.class, () -> ScriptSortBuilder.fromXContent(parser, null));
        assertEquals("[_script] unknown field [bad_field], parser not found", e.getMessage());
    }

    public void testParseUnexpectedToken() throws IOException {
        String scriptSort = "{\"_script\" : {" + "\"script\" : [ \"order\" : \"asc\" ] } }";
        XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        Exception e = expectThrows(ParsingException.class, () -> ScriptSortBuilder.fromXContent(parser, null));
        assertEquals("[_script] script doesn't support values of type: START_ARRAY", e.getMessage());
    }

    /**
     * script sort of type {@link ScriptSortType} does not work with {@link SortMode#AVG}, {@link SortMode#MEDIAN} or {@link SortMode#SUM}
     */
    public void testBadSortMode() throws IOException {
        ScriptSortBuilder builder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.STRING);
        String sortMode = randomFrom(new String[] { "avg", "median", "sum" });
        Exception e = expectThrows(IllegalArgumentException.class, () -> builder.sortMode(SortMode.fromString(sortMode)));
        assertEquals("script sort of type [string] doesn't support mode [" + sortMode + "]", e.getMessage());
    }

    /**
     * Test that the sort builder mode gets transfered correctly to the SortField
     */
    public void testMultiValueMode() throws IOException {
        QueryShardContext shardContextMock = createMockShardContext();
        for (SortMode mode : SortMode.values()) {
            ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER);
            sortBuilder.sortMode(mode);
            SortField sortField = sortBuilder.build(shardContextMock).field;
            assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
            XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
            assertEquals(MultiValueMode.fromString(mode.toString()), comparatorSource.sortMode());
        }

        // check that without mode set, order ASC sets mode to MIN, DESC to MAX
        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER);
        sortBuilder.order(SortOrder.ASC);
        SortField sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.MIN, comparatorSource.sortMode());

        sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER);
        sortBuilder.order(SortOrder.DESC);
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertEquals(MultiValueMode.MAX, comparatorSource.sortMode());
    }

    /**
     * Test that the correct comparator sort is returned, based on the script type
     */
    public void testBuildCorrectComparatorType() throws IOException {
        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.STRING);
        SortField sortField = sortBuilder.build(createMockShardContext()).field;
        assertThat(sortField.getComparatorSource(), instanceOf(BytesRefFieldComparatorSource.class));

        sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER);
        sortField = sortBuilder.build(createMockShardContext()).field;
        assertThat(sortField.getComparatorSource(), instanceOf(DoubleValuesComparatorSource.class));
    }

    /**
     * Test that the sort builder nested object gets created in the SortField
     */
    public void testBuildNested() throws IOException {
        QueryShardContext shardContextMock = createMockShardContext();

        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER)
                .setNestedSort(new NestedSortBuilder("path").setFilter(QueryBuilders.matchAllQuery()));
        SortField sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        XFieldComparatorSource comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        Nested nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(new MatchAllDocsQuery(), nested.getInnerQuery());

        sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER).setNestedPath("path");
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(new TermQuery(new Term(TypeFieldMapper.NAME, "__path")), nested.getInnerQuery());

        sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER).setNestedPath("path")
                .setNestedFilter(QueryBuilders.matchAllQuery());
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        nested = comparatorSource.nested();
        assertNotNull(nested);
        assertEquals(new MatchAllDocsQuery(), nested.getInnerQuery());

        // if nested path is missing, we omit nested element in the comparator
        sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER)
                .setNestedFilter(QueryBuilders.matchAllQuery());
        sortField = sortBuilder.build(shardContextMock).field;
        assertThat(sortField.getComparatorSource(), instanceOf(XFieldComparatorSource.class));
        comparatorSource = (XFieldComparatorSource) sortField.getComparatorSource();
        assertNull(comparatorSource.nested());
    }

    /**
     * Test we can either set nested sort via path/filter or via nested sort builder, not both
     */
    public void testNestedSortBothThrows() throws IOException {
        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript(MOCK_SCRIPT_NAME), ScriptSortType.NUMBER);
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class,
                () -> sortBuilder.setNestedPath("nestedPath").setNestedSort(new NestedSortBuilder("otherPath")));
        assertEquals("Setting both nested_path/nested_filter and nested not allowed", iae.getMessage());
        iae = expectThrows(IllegalArgumentException.class,
                () -> sortBuilder.setNestedSort(new NestedSortBuilder("otherPath")).setNestedPath("nestedPath"));
        assertEquals("Setting both nested_path/nested_filter and nested not allowed", iae.getMessage());
        iae = expectThrows(IllegalArgumentException.class,
                () -> sortBuilder.setNestedSort(new NestedSortBuilder("otherPath")).setNestedFilter(QueryBuilders.matchAllQuery()));
        assertEquals("Setting both nested_path/nested_filter and nested not allowed", iae.getMessage());
     }

    /**
     * Test the nested Filter gets rewritten
     */
    public void testNestedRewrites() throws IOException {
        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript("something"), ScriptSortType.STRING);
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("fieldName") {
            @Override
            public QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
                return new MatchNoneQueryBuilder();
            }
        };
        sortBuilder.setNestedPath("path").setNestedFilter(rangeQuery);
        ScriptSortBuilder rewritten = (ScriptSortBuilder) sortBuilder
                .rewrite(createMockShardContext());
        assertNotSame(rangeQuery, rewritten.getNestedFilter());
    }

    /**
     * Test the nested sort gets rewritten
     */
    public void testNestedSortRewrites() throws IOException {
        ScriptSortBuilder sortBuilder = new ScriptSortBuilder(mockScript("something"), ScriptSortType.STRING);
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder("fieldName") {
            @Override
            public QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
                return new MatchNoneQueryBuilder();
            }
        };
        sortBuilder.setNestedSort(new NestedSortBuilder("path").setFilter(rangeQuery));
        ScriptSortBuilder rewritten = (ScriptSortBuilder) sortBuilder
                .rewrite(createMockShardContext());
        assertNotSame(rangeQuery, rewritten.getNestedSort().getFilter());
    }

    @Override
    protected ScriptSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        return ScriptSortBuilder.fromXContent(parser, fieldName);
    }
}
