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


import org.apache.lucene.search.SortField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ScriptSortBuilderTests extends AbstractSortTestCase<ScriptSortBuilder> {

    @Override
    protected ScriptSortBuilder createTestItem() {
        return randomScriptSortBuilder();
    }

    public static ScriptSortBuilder randomScriptSortBuilder() {
        ScriptSortType type = randomBoolean() ? ScriptSortType.NUMBER : ScriptSortType.STRING;
        ScriptSortBuilder builder = new ScriptSortBuilder(new Script(randomAsciiOfLengthBetween(5, 10)),
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
            builder.setNestedFilter(randomNestedFilter());
        }
        if (randomBoolean()) {
            builder.setNestedPath(randomAsciiOfLengthBetween(1, 10));
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
                result = new ScriptSortBuilder(new Script(script.getIdOrCode() + "_suffix"), type);
            } else {
                result = new ScriptSortBuilder(script, type.equals(ScriptSortType.NUMBER) ? ScriptSortType.STRING : ScriptSortType.NUMBER);
            }
            result.order(original.order());
            if (original.sortMode() != null && result.type() == ScriptSortType.NUMBER) {
                result.sortMode(original.sortMode());
            }
            result.setNestedFilter(original.getNestedFilter());
            result.setNestedPath(original.getNestedPath());
            return result;
        }
        result = new ScriptSortBuilder(original);
        switch (randomIntBetween(0, 3)) {
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
                result.setNestedFilter(randomValueOtherThan(
                        original.getNestedFilter(),
                        () -> randomNestedFilter()));
                break;
            case 3:
                result.setNestedPath(original.getNestedPath() + "_some_suffix");
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
                        "\"inline\": \"doc['field_name'].value * factor\",\n" +
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

        QueryParseContext context = new QueryParseContext(parser);
        ScriptSortBuilder builder = ScriptSortBuilder.fromXContent(context, null);
        assertEquals("doc['field_name'].value * factor", builder.script().getIdOrCode());
        assertEquals(Script.DEFAULT_SCRIPT_LANG, builder.script().getLang());
        assertEquals(1.1, builder.script().getParams().get("factor"));
        assertEquals(ScriptType.INLINE, builder.script().getType());
        assertEquals(ScriptSortType.NUMBER, builder.type());
        assertEquals(SortOrder.ASC, builder.order());
        assertEquals(SortMode.MAX, builder.sortMode());
        assertNull(builder.getNestedFilter());
        assertNull(builder.getNestedPath());
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

        QueryParseContext context = new QueryParseContext(parser);
        ScriptSortBuilder builder = ScriptSortBuilder.fromXContent(context, null);
        assertEquals("doc['field_name'].value", builder.script().getIdOrCode());
        assertEquals(Script.DEFAULT_SCRIPT_LANG, builder.script().getLang());
        assertEquals(builder.script().getParams(), Collections.emptyMap());
        assertEquals(ScriptType.INLINE, builder.script().getType());
        assertEquals(ScriptSortType.NUMBER, builder.type());
        assertEquals(SortOrder.ASC, builder.order());
        assertEquals(SortMode.MAX, builder.sortMode());
        assertNull(builder.getNestedFilter());
        assertNull(builder.getNestedPath());
    }

    public void testParseBadFieldNameExceptions() throws IOException {
        String scriptSort = "{\"_script\" : {" + "\"bad_field\" : \"number\"" + "} }";
        XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        QueryParseContext context = new QueryParseContext(parser);
        Exception e = expectThrows(IllegalArgumentException.class, () -> ScriptSortBuilder.fromXContent(context, null));
        assertEquals("[_script] unknown field [bad_field], parser not found", e.getMessage());
    }

    public void testParseBadFieldNameExceptionsOnStartObject() throws IOException {

        String scriptSort = "{\"_script\" : {" + "\"bad_field\" : { \"order\" : \"asc\" } } }";
        XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        QueryParseContext context = new QueryParseContext(parser);
        Exception e = expectThrows(IllegalArgumentException.class, () -> ScriptSortBuilder.fromXContent(context, null));
        assertEquals("[_script] unknown field [bad_field], parser not found", e.getMessage());
    }

    public void testParseUnexpectedToken() throws IOException {
        String scriptSort = "{\"_script\" : {" + "\"script\" : [ \"order\" : \"asc\" ] } }";
        XContentParser parser = createParser(JsonXContent.jsonXContent, scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        QueryParseContext context = new QueryParseContext(parser);
        Exception e = expectThrows(IllegalArgumentException.class, () -> ScriptSortBuilder.fromXContent(context, null));
        assertEquals("[_script] script doesn't support values of type: START_ARRAY", e.getMessage());
    }

    /**
     * script sort of type {@link ScriptSortType} does not work with {@link SortMode#AVG}, {@link SortMode#MEDIAN} or {@link SortMode#SUM}
     */
    public void testBadSortMode() throws IOException {
        ScriptSortBuilder builder = new ScriptSortBuilder(new Script("something"), ScriptSortType.STRING);
        String sortMode = randomFrom(new String[] { "avg", "median", "sum" });
        Exception e = expectThrows(IllegalArgumentException.class, () -> builder.sortMode(SortMode.fromString(sortMode)));
        assertEquals("script sort of type [string] doesn't support mode [" + sortMode + "]", e.getMessage());
    }

    @Override
    protected ScriptSortBuilder fromXContent(QueryParseContext context, String fieldName) throws IOException {
        return ScriptSortBuilder.fromXContent(context, fieldName);
    }
}
