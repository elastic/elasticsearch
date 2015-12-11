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

package org.elasticsearch.search.highlight;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilders;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.IdsQueryParser;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermQueryParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder.Field;
import org.elasticsearch.search.highlight.SearchContextHighlight.FieldOptions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class HighlightBuilderTests extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;
    private static IndicesQueriesRegistry indicesQueriesRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        namedWriteableRegistry = new NamedWriteableRegistry();
        @SuppressWarnings("rawtypes")
        Set<QueryParser> injectedQueryParsers = new HashSet<>();
        injectedQueryParsers.add(new MatchAllQueryParser());
        injectedQueryParsers.add(new IdsQueryParser());
        injectedQueryParsers.add(new TermQueryParser());
        indicesQueriesRegistry = new IndicesQueriesRegistry(Settings.settingsBuilder().build(), injectedQueryParsers, namedWriteableRegistry);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
        indicesQueriesRegistry = null;
    }

    /**
     * Test serialization and deserialization of the highlighter builder
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            HighlightBuilder original = randomHighlighterBuilder();
            HighlightBuilder deserialized = serializedCopy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
        }
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            HighlightBuilder firstBuilder = randomHighlighterBuilder();
            assertFalse("highlighter is equal to null", firstBuilder.equals(null));
            assertFalse("highlighter is equal to incompatible type", firstBuilder.equals(""));
            assertTrue("highlighter is not equal to self", firstBuilder.equals(firstBuilder));
            assertThat("same highlighter's hashcode returns different values if called multiple times", firstBuilder.hashCode(),
                    equalTo(firstBuilder.hashCode()));
            assertThat("different highlighters should not be equal", mutate(firstBuilder), not(equalTo(firstBuilder)));

            HighlightBuilder secondBuilder = serializedCopy(firstBuilder);
            assertTrue("highlighter is not equal to self", secondBuilder.equals(secondBuilder));
            assertTrue("highlighter is not equal to its copy", firstBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", secondBuilder.equals(firstBuilder));
            assertThat("highlighter copy's hashcode is different from original hashcode", secondBuilder.hashCode(), equalTo(firstBuilder.hashCode()));

            HighlightBuilder thirdBuilder = serializedCopy(secondBuilder);
            assertTrue("highlighter is not equal to self", thirdBuilder.equals(thirdBuilder));
            assertTrue("highlighter is not equal to its copy", secondBuilder.equals(thirdBuilder));
            assertThat("highlighter copy's hashcode is different from original hashcode", secondBuilder.hashCode(), equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not transitive", firstBuilder.equals(thirdBuilder));
            assertThat("highlighter copy's hashcode is different from original hashcode", firstBuilder.hashCode(), equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not symmetric", thirdBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", thirdBuilder.equals(firstBuilder));
        }
    }

    /**
     *  creates random highlighter, renders it to xContent and back to new instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            HighlightBuilder highlightBuilder = randomHighlighterBuilder();
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            builder.startObject();
            highlightBuilder.innerXContent(builder);
            builder.endObject();

            XContentParser parser = XContentHelper.createParser(builder.bytes());
            context.reset(parser);
            HighlightBuilder secondHighlightBuilder = HighlightBuilder.fromXContent(context);
            assertNotSame(highlightBuilder, secondHighlightBuilder);
            assertEquals(highlightBuilder, secondHighlightBuilder);
            assertEquals(highlightBuilder.hashCode(), secondHighlightBuilder.hashCode());
        }
    }

    /**
     * test that unknown array fields cause exception
     */
    public void testUnknownArrayNameExpection() throws IOException {
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        String highlightElement = "{\n" +
                "    \"bad_fieldname\" : [ \"field1\" 1 \"field2\" ]\n" +
                "}\n";
        XContentParser parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context.reset(parser);
        try {
            HighlightBuilder.fromXContent(context);
            fail("expected a parsing exception");
        } catch (ParsingException e) {
            assertEquals("cannot parse array with name [bad_fieldname]", e.getMessage());
        }

        highlightElement = "{\n" +
                "  \"fields\" : {\n" +
                "     \"body\" : {\n" +
                "        \"bad_fieldname\" : [ \"field1\" , \"field2\" ]\n" +
                "     }\n" +
                "   }\n" +
                "}\n";
        parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context.reset(parser);
        try {
            HighlightBuilder.fromXContent(context);
            fail("expected a parsing exception");
        } catch (ParsingException e) {
            assertEquals("cannot parse array with name [bad_fieldname]", e.getMessage());
        }
    }

    /**
     * test that unknown field name cause exception
     */
    public void testUnknownFieldnameExpection() throws IOException {
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        String highlightElement = "{\n" +
                "    \"bad_fieldname\" : \"value\"\n" +
                "}\n";
        XContentParser parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context.reset(parser);
        try {
            HighlightBuilder.fromXContent(context);
            fail("expected a parsing exception");
        } catch (ParsingException e) {
            assertEquals("unexpected fieldname [bad_fieldname]", e.getMessage());
        }

        highlightElement = "{\n" +
                "  \"fields\" : {\n" +
                "     \"body\" : {\n" +
                "        \"bad_fieldname\" : \"value\"\n" +
                "     }\n" +
                "   }\n" +
                "}\n";
        parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context.reset(parser);
        try {
            HighlightBuilder.fromXContent(context);
            fail("expected a parsing exception");
        } catch (ParsingException e) {
            assertEquals("unexpected fieldname [bad_fieldname]", e.getMessage());
        }
    }

    /**
     * test that unknown field name cause exception
     */
    public void testUnknownObjectFieldnameExpection() throws IOException {
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        String highlightElement = "{\n" +
                "    \"bad_fieldname\" :  { \"field\" : \"value\" }\n \n" +
                "}\n";
        XContentParser parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context.reset(parser);
        try {
            HighlightBuilder.fromXContent(context);
            fail("expected a parsing exception");
        } catch (ParsingException e) {
            assertEquals("cannot parse object with name [bad_fieldname]", e.getMessage());
        }

        highlightElement = "{\n" +
                "  \"fields\" : {\n" +
                "     \"body\" : {\n" +
                "        \"bad_fieldname\" : { \"field\" : \"value\" }\n" +
                "     }\n" +
                "   }\n" +
                "}\n";
        parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context.reset(parser);
        try {
            HighlightBuilder.fromXContent(context);
            fail("expected a parsing exception");
        } catch (ParsingException e) {
            assertEquals("cannot parse object with name [bad_fieldname]", e.getMessage());
        }
     }

     /**
     * test that build() outputs a {@link SearchContextHighlight} that is similar to the one
     * we would get when parsing the xContent the test highlight builder is rendering out
     */
    public void testBuildSearchContextHighlight() throws IOException {
        Settings indexSettings = Settings.settingsBuilder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        Index index = new Index(randomAsciiOfLengthBetween(1, 10));
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(index, indexSettings);
        // shard context will only need indicesQueriesRegistry for building Query objects nested in highlighter
        QueryShardContext mockShardContext = new QueryShardContext(idxSettings, null, null, null, null, null, null, indicesQueriesRegistry) {
            @Override
            public MappedFieldType fieldMapper(String name) {
                StringFieldMapper.Builder builder = MapperBuilders.stringField(name);
                return builder.build(new Mapper.BuilderContext(idxSettings.getSettings(), new ContentPath(1))).fieldType();
            }
        };
        mockShardContext.setMapUnmappedFieldAsString(true);

        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            HighlightBuilder highlightBuilder = randomHighlighterBuilder();
            SearchContextHighlight highlight = highlightBuilder.build(mockShardContext);
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            builder.startObject();
            highlightBuilder.innerXContent(builder);
            builder.endObject();
            XContentParser parser = XContentHelper.createParser(builder.bytes());

            SearchContextHighlight parsedHighlight = new HighlighterParseElement().parse(parser, mockShardContext);
            assertNotSame(highlight, parsedHighlight);
            assertEquals(highlight.globalForceSource(), parsedHighlight.globalForceSource());
            assertEquals(highlight.fields().size(), parsedHighlight.fields().size());

            Iterator<org.elasticsearch.search.highlight.SearchContextHighlight.Field> iterator = parsedHighlight.fields().iterator();
            for (org.elasticsearch.search.highlight.SearchContextHighlight.Field field : highlight.fields()) {
                org.elasticsearch.search.highlight.SearchContextHighlight.Field otherField = iterator.next();
                assertEquals(field.field(), otherField.field());
                FieldOptions options = field.fieldOptions();
                FieldOptions otherOptions = otherField.fieldOptions();
                assertArrayEquals(options.boundaryChars(), options.boundaryChars());
                assertEquals(options.boundaryMaxScan(), otherOptions.boundaryMaxScan());
                assertEquals(options.encoder(), otherOptions.encoder());
                assertEquals(options.fragmentCharSize(), otherOptions.fragmentCharSize());
                assertEquals(options.fragmenter(), otherOptions.fragmenter());
                assertEquals(options.fragmentOffset(), otherOptions.fragmentOffset());
                assertEquals(options.highlighterType(), otherOptions.highlighterType());
                assertEquals(options.highlightFilter(), otherOptions.highlightFilter());
                assertEquals(options.highlightQuery(), otherOptions.highlightQuery());
                assertEquals(options.matchedFields(), otherOptions.matchedFields());
                assertEquals(options.noMatchSize(), otherOptions.noMatchSize());
                assertEquals(options.numberOfFragments(), otherOptions.numberOfFragments());
                assertEquals(options.options(), otherOptions.options());
                assertEquals(options.phraseLimit(), otherOptions.phraseLimit());
                assertArrayEquals(options.preTags(), otherOptions.preTags());
                assertArrayEquals(options.postTags(), otherOptions.postTags());
                assertEquals(options.requireFieldMatch(), otherOptions.requireFieldMatch());
                assertEquals(options.scoreOrdered(), otherOptions.scoreOrdered());
            }
        }
    }

    /**
     * `tags_schema` is not produced by toXContent in the builder but should be parseable, so this
     * adds a simple json test for this.
     */
    public void testParsingTagsSchema() throws IOException {
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        String highlightElement = "{\n" +
                "    \"tags_schema\" : \"styled\"\n" +
                "}\n";
        XContentParser parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context.reset(parser);
        HighlightBuilder highlightBuilder = HighlightBuilder.fromXContent(context);
        assertArrayEquals("setting tags_schema 'styled' should alter pre_tags", HighlightBuilder.DEFAULT_STYLED_PRE_TAG,
                highlightBuilder.preTags());
        assertArrayEquals("setting tags_schema 'styled' should alter post_tags", HighlightBuilder.DEFAULT_STYLED_POST_TAGS,
                highlightBuilder.postTags());

        highlightElement = "{\n" +
                "    \"tags_schema\" : \"default\"\n" +
                "}\n";
        parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context.reset(parser);
        highlightBuilder = HighlightBuilder.fromXContent(context);
        assertArrayEquals("setting tags_schema 'default' should alter pre_tags", HighlightBuilder.DEFAULT_PRE_TAGS,
                highlightBuilder.preTags());
        assertArrayEquals("setting tags_schema 'default' should alter post_tags", HighlightBuilder.DEFAULT_POST_TAGS,
                highlightBuilder.postTags());

        highlightElement = "{\n" +
                "    \"tags_schema\" : \"somthing_else\"\n" +
                "}\n";
        parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context.reset(parser);
        try {
            highlightBuilder = HighlightBuilder.fromXContent(context);
            fail("setting unknown tag schema should throw exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Unknown tag schema [somthing_else]", e.getMessage());
        }
    }

    protected static XContentBuilder toXContent(HighlightBuilder highlight, XContentType contentType) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(contentType);
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        highlight.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return builder;
    }

    /**
     * create random shape that is put under test
     */
    private static HighlightBuilder randomHighlighterBuilder() {
        HighlightBuilder testHighlighter = new HighlightBuilder();
        setRandomCommonOptions(testHighlighter);
        testHighlighter.useExplicitFieldOrder(randomBoolean());
        if (randomBoolean()) {
            testHighlighter.encoder(randomFrom(Arrays.asList(new String[]{"default", "html"})));
        }
        int numberOfFields = randomIntBetween(1,5);
        for (int i = 0; i < numberOfFields; i++) {
            Field field = new Field(randomAsciiOfLengthBetween(1, 10));
            setRandomCommonOptions(field);
            if (randomBoolean()) {
                field.fragmentOffset(randomIntBetween(1, 100));
            }
            if (randomBoolean()) {
                field.matchedFields(randomStringArray(0, 4));
            }
            testHighlighter.field(field);
        }
        return testHighlighter;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void setRandomCommonOptions(AbstractHighlighterBuilder highlightBuilder) {
        if (randomBoolean()) {
            // need to set this together, otherwise parsing will complain
            highlightBuilder.preTags(randomStringArray(0, 3));
            highlightBuilder.postTags(randomStringArray(0, 3));
        }
        if (randomBoolean()) {
            highlightBuilder.fragmentSize(randomIntBetween(0, 100));
        }
        if (randomBoolean()) {
            highlightBuilder.numOfFragments(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            highlightBuilder.highlighterType(randomAsciiOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            highlightBuilder.fragmenter(randomAsciiOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            QueryBuilder highlightQuery;
            switch (randomInt(2)) {
            case 0:
                highlightQuery = new MatchAllQueryBuilder();
                break;
            case 1:
                highlightQuery = new IdsQueryBuilder();
                break;
            default:
            case 2:
                highlightQuery = new TermQueryBuilder(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
                break;
            }
            highlightQuery.boost((float) randomDoubleBetween(0, 10, false));
            highlightBuilder.highlightQuery(highlightQuery);
        }
        if (randomBoolean()) {
            highlightBuilder.order(randomAsciiOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            highlightBuilder.highlightFilter(randomBoolean());
        }
        if (randomBoolean()) {
            highlightBuilder.forceSource(randomBoolean());
        }
        if (randomBoolean()) {
            highlightBuilder.boundaryMaxScan(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            highlightBuilder.boundaryChars(randomAsciiOfLengthBetween(1, 10).toCharArray());
        }
        if (randomBoolean()) {
            highlightBuilder.noMatchSize(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            highlightBuilder.phraseLimit(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            int items = randomIntBetween(0, 5);
            Map<String, Object> options = new HashMap<String, Object>(items);
            for (int i = 0; i < items; i++) {
                Object value = null;
                switch (randomInt(2)) {
                case 0:
                    value = randomAsciiOfLengthBetween(1, 10);
                    break;
                case 1:
                    value = new Integer(randomInt(1000));
                    break;
                case 2:
                    value = new Boolean(randomBoolean());
                    break;
                }
                options.put(randomAsciiOfLengthBetween(1, 10), value);
            }
        }
        if (randomBoolean()) {
            highlightBuilder.requireFieldMatch(randomBoolean());
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void mutateCommonOptions(AbstractHighlighterBuilder highlightBuilder) {
        switch (randomIntBetween(1, 16)) {
        case 1:
            highlightBuilder.preTags(randomStringArray(4, 6));
            break;
        case 2:
            highlightBuilder.postTags(randomStringArray(4, 6));
            break;
        case 3:
            highlightBuilder.fragmentSize(randomIntBetween(101, 200));
            break;
        case 4:
            highlightBuilder.numOfFragments(randomIntBetween(11, 20));
            break;
        case 5:
            highlightBuilder.highlighterType(randomAsciiOfLengthBetween(11, 20));
            break;
        case 6:
            highlightBuilder.fragmenter(randomAsciiOfLengthBetween(11, 20));
            break;
        case 7:
            highlightBuilder.highlightQuery(new TermQueryBuilder(randomAsciiOfLengthBetween(11, 20), randomAsciiOfLengthBetween(11, 20)));
            break;
        case 8:
            highlightBuilder.order(randomAsciiOfLengthBetween(11, 20));
            break;
        case 9:
            highlightBuilder.highlightFilter(toggleOrSet(highlightBuilder.highlightFilter()));
            break;
        case 10:
            highlightBuilder.forceSource(toggleOrSet(highlightBuilder.forceSource()));
            break;
        case 11:
            highlightBuilder.boundaryMaxScan(randomIntBetween(11, 20));
            break;
        case 12:
            highlightBuilder.boundaryChars(randomAsciiOfLengthBetween(11, 20).toCharArray());
            break;
        case 13:
            highlightBuilder.noMatchSize(randomIntBetween(11, 20));
            break;
        case 14:
            highlightBuilder.phraseLimit(randomIntBetween(11, 20));
            break;
        case 15:
            int items = 6;
            Map<String, Object> options = new HashMap<String, Object>(items);
            for (int i = 0; i < items; i++) {
                options.put(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
            }
            highlightBuilder.options(options);
            break;
        case 16:
            highlightBuilder.requireFieldMatch(toggleOrSet(highlightBuilder.requireFieldMatch()));
            break;
        }
    }

    private static Boolean toggleOrSet(Boolean flag) {
        if (flag == null) {
            return randomBoolean();
        } else {
            return !flag.booleanValue();
        }
    }

    private static String[] randomStringArray(int minSize, int maxSize) {
        int size = randomIntBetween(minSize, maxSize);
        String[] randomStrings = new String[size];
        for (int f = 0; f < size; f++) {
            randomStrings[f] = randomAsciiOfLengthBetween(1, 10);
        }
        return randomStrings;
    }

    /**
     * mutate the given highlighter builder so the returned one is different in one aspect
     */
    private static HighlightBuilder mutate(HighlightBuilder original) throws IOException {
        HighlightBuilder mutation = serializedCopy(original);
        if (randomBoolean()) {
            mutateCommonOptions(mutation);
        } else {
            switch (randomIntBetween(0, 2)) {
                // change settings that only exists on top level
                case 0:
                    mutation.useExplicitFieldOrder(!original.useExplicitFieldOrder()); break;
                case 1:
                    mutation.encoder(original.encoder() + randomAsciiOfLength(2)); break;
                case 2:
                    if (randomBoolean()) {
                        // add another field
                        mutation.field(new Field(randomAsciiOfLength(10)));
                    } else {
                        // change existing fields
                        List<Field> originalFields = original.fields();
                        Field fieldToChange = originalFields.get(randomInt(originalFields.size() - 1));
                        if (randomBoolean()) {
                            fieldToChange.fragmentOffset(randomIntBetween(101, 200));
                        } else {
                            fieldToChange.matchedFields(randomStringArray(5, 10));
                        }
                    }
                    break;
            }
        }
        return mutation;
    }

    private static HighlightBuilder serializedCopy(HighlightBuilder original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                return HighlightBuilder.PROTOTYPE.readFrom(in);
            }
        }
    }
}
