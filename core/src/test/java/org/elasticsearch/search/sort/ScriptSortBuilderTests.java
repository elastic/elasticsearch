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


import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.IOException;
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
                builder.order(RandomSortDataGenerator.order(null));
        }
        if (randomBoolean()) {
            if (type == ScriptSortType.NUMBER) {
                builder.sortMode(RandomSortDataGenerator.mode(builder.sortMode()));
            } else {
                Set<SortMode> exceptThis = new HashSet<>();
                exceptThis.add(SortMode.SUM);
                exceptThis.add(SortMode.AVG);
                exceptThis.add(SortMode.MEDIAN);
                builder.sortMode(RandomSortDataGenerator.mode(exceptThis));
            }
        }
        if (randomBoolean()) {
            builder.setNestedFilter(RandomSortDataGenerator.nestedFilter(builder.getNestedFilter()));
        }
        if (randomBoolean()) {
            builder.setNestedPath(RandomSortDataGenerator.randomAscii(builder.getNestedPath()));
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
                result = new ScriptSortBuilder(new Script(script.getScript() + "_suffix"), type);
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
                    result.sortMode(RandomSortDataGenerator.mode(original.sortMode()));
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
                result.setNestedFilter(RandomSortDataGenerator.nestedFilter(original.getNestedFilter()));
                break;
            case 3:
                result.setNestedPath(original.getNestedPath() + "_some_suffix");
                break;
        }
        return result;
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

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
        exceptionRule.expect(NullPointerException.class);
        exceptionRule.expectMessage("input string is null");
        ScriptSortType.fromString(null);
    }

    public void testScriptSortTypeIllegalArgument() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Unknown ScriptSortType [xyz]");
        ScriptSortType.fromString("xyz");
    }

    public void testParseJson() throws IOException {
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
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
        XContentParser parser = XContentFactory.xContent(scriptSort).createParser(scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        context.reset(parser);
        ScriptSortBuilder builder = ScriptSortBuilder.PROTOTYPE.fromXContent(context, null);
        assertEquals("doc['field_name'].value * factor", builder.script().getScript());
        assertNull(builder.script().getLang());
        assertEquals(1.1, builder.script().getParams().get("factor"));
        assertEquals(ScriptType.INLINE, builder.script().getType());
        assertEquals(ScriptSortType.NUMBER, builder.type());
        assertEquals(SortOrder.ASC, builder.order());
        assertEquals(SortMode.MAX, builder.sortMode());
        assertNull(builder.getNestedFilter());
        assertNull(builder.getNestedPath());
    }

    public void testParseJsonOldStyle() throws IOException {
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        String scriptSort = "{\n" +
                "\"_script\" : {\n" +
                    "\"type\" : \"number\",\n" +
                    "\"script\" : \"doc['field_name'].value * factor\",\n" +
                    "\"params\" : {\n" +
                        "\"factor\" : 1.1\n" +
                    "},\n" +
                    "\"mode\" : \"max\",\n" +
                    "\"order\" : \"asc\"\n" +
                "} }\n";
        XContentParser parser = XContentFactory.xContent(scriptSort).createParser(scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        context.reset(parser);
        ScriptSortBuilder builder = ScriptSortBuilder.PROTOTYPE.fromXContent(context, null);
        assertEquals("doc['field_name'].value * factor", builder.script().getScript());
        assertNull(builder.script().getLang());
        assertEquals(1.1, builder.script().getParams().get("factor"));
        assertEquals(ScriptType.INLINE, builder.script().getType());
        assertEquals(ScriptSortType.NUMBER, builder.type());
        assertEquals(SortOrder.ASC, builder.order());
        assertEquals(SortMode.MAX, builder.sortMode());
        assertNull(builder.getNestedFilter());
        assertNull(builder.getNestedPath());
    }

    public void testParseBadFieldNameExceptions() throws IOException {
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        String scriptSort = "{\"_script\" : {" + "\"bad_field\" : \"number\"" + "} }";
        XContentParser parser = XContentFactory.xContent(scriptSort).createParser(scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        context.reset(parser);
        exceptionRule.expect(ParsingException.class);
        exceptionRule.expectMessage("failed to parse field [bad_field]");
        ScriptSortBuilder.PROTOTYPE.fromXContent(context, null);
    }

    public void testParseBadFieldNameExceptionsOnStartObject() throws IOException {
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));

        String scriptSort = "{\"_script\" : {" + "\"bad_field\" : { \"order\" : \"asc\" } } }";
        XContentParser parser = XContentFactory.xContent(scriptSort).createParser(scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        context.reset(parser);
        exceptionRule.expect(ParsingException.class);
        exceptionRule.expectMessage("failed to parse field [bad_field]");
        ScriptSortBuilder.PROTOTYPE.fromXContent(context, null);
    }

    public void testParseUnexpectedToken() throws IOException {
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));

        String scriptSort = "{\"_script\" : {" + "\"script\" : [ \"order\" : \"asc\" ] } }";
        XContentParser parser = XContentFactory.xContent(scriptSort).createParser(scriptSort);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();

        context.reset(parser);
        exceptionRule.expect(ParsingException.class);
        exceptionRule.expectMessage("unexpected token [START_ARRAY]");
        ScriptSortBuilder.PROTOTYPE.fromXContent(context, null);
    }

    /**
     * script sort of type {@link ScriptSortType} does not work with {@link SortMode#AVG}, {@link SortMode#MEDIAN} or {@link SortMode#SUM}
     */
    public void testBadSortMode() throws IOException {
        ScriptSortBuilder builder = new ScriptSortBuilder(new Script("something"), ScriptSortType.STRING);
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("script sort of type [string] doesn't support mode");
        builder.sortMode(SortMode.fromString(randomFrom(new String[]{"avg", "median", "sum"})));
    }
}
