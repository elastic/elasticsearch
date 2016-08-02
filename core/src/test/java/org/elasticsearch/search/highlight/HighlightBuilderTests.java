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

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
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
import org.elasticsearch.index.mapper.core.TextFieldMapper;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.highlight.HighlightBuilder.Field;
import org.elasticsearch.search.highlight.HighlightBuilder.Order;
import org.elasticsearch.search.highlight.SearchContextHighlight.FieldOptions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Collections.emptyList;
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
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        indicesQueriesRegistry = searchModule.getQueryParserRegistry();
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
            assertThat("highlighter copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(firstBuilder.hashCode()));

            HighlightBuilder thirdBuilder = serializedCopy(secondBuilder);
            assertTrue("highlighter is not equal to self", thirdBuilder.equals(thirdBuilder));
            assertTrue("highlighter is not equal to its copy", secondBuilder.equals(thirdBuilder));
            assertThat("highlighter copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not transitive", firstBuilder.equals(thirdBuilder));
            assertThat("highlighter copy's hashcode is different from original hashcode", firstBuilder.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not symmetric", thirdBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", thirdBuilder.equals(firstBuilder));
        }
    }

    /**
     *  creates random highlighter, renders it to xContent and back to new instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            HighlightBuilder highlightBuilder = randomHighlighterBuilder();
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            highlightBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentBuilder shuffled = shuffleXContent(builder);

            XContentParser parser = XContentHelper.createParser(shuffled.bytes());
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.EMPTY);
            parser.nextToken();
            HighlightBuilder secondHighlightBuilder;
            try {
                secondHighlightBuilder = HighlightBuilder.fromXContent(context);
            } catch (RuntimeException e) {
                throw new RuntimeException("Error parsing " + highlightBuilder, e);
            }
            assertNotSame(highlightBuilder, secondHighlightBuilder);
            assertEquals(highlightBuilder, secondHighlightBuilder);
            assertEquals(highlightBuilder.hashCode(), secondHighlightBuilder.hashCode());
        }
    }

    /**
     * test that unknown array fields cause exception
     */
    public void testUnknownArrayNameExpection() throws IOException {
        {
            IllegalArgumentException e = expectParseThrows(IllegalArgumentException.class, "{\n" +
                    "    \"bad_fieldname\" : [ \"field1\" 1 \"field2\" ]\n" +
                    "}\n");
            assertEquals("[highlight] unknown field [bad_fieldname], parser not found", e.getMessage());
        }

        {
            ParsingException e = expectParseThrows(ParsingException.class, "{\n" +
                    "  \"fields\" : {\n" +
                    "     \"body\" : {\n" +
                    "        \"bad_fieldname\" : [ \"field1\" , \"field2\" ]\n" +
                    "     }\n" +
                    "   }\n" +
                    "}\n");
            assertEquals("[highlight] failed to parse field [fields]", e.getMessage());
            assertEquals("[fields] failed to parse field [body]", e.getCause().getMessage());
            assertEquals("[highlight_field] unknown field [bad_fieldname], parser not found", e.getCause().getCause().getMessage());
        }
    }

    private static <T extends Throwable> T expectParseThrows(Class<T> exceptionClass, String highlightElement) throws IOException {
        XContentParser parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.STRICT);
        return expectThrows(exceptionClass, () -> HighlightBuilder.fromXContent(context));
    }

    /**
     * test that unknown field name cause exception
     */
    public void testUnknownFieldnameExpection() throws IOException {
        {
            IllegalArgumentException e = expectParseThrows(IllegalArgumentException.class, "{\n" +
                    "    \"bad_fieldname\" : \"value\"\n" +
                    "}\n");
            assertEquals("[highlight] unknown field [bad_fieldname], parser not found", e.getMessage());
        }

        {
            ParsingException e = expectParseThrows(ParsingException.class, "{\n" +
                    "  \"fields\" : {\n" +
                    "     \"body\" : {\n" +
                    "        \"bad_fieldname\" : \"value\"\n" +
                    "     }\n" +
                    "   }\n" +
                    "}\n");
            assertEquals("[highlight] failed to parse field [fields]", e.getMessage());
            assertEquals("[fields] failed to parse field [body]", e.getCause().getMessage());
            assertEquals("[highlight_field] unknown field [bad_fieldname], parser not found", e.getCause().getCause().getMessage());
        }
    }

    /**
     * test that unknown field name cause exception
     */
    public void testUnknownObjectFieldnameExpection() throws IOException {
        {
            IllegalArgumentException e = expectParseThrows(IllegalArgumentException.class, "{\n" +
                    "    \"bad_fieldname\" :  { \"field\" : \"value\" }\n \n" +
                    "}\n");
            assertEquals("[highlight] unknown field [bad_fieldname], parser not found", e.getMessage());
        }

        {
            ParsingException e = expectParseThrows(ParsingException.class, "{\n" +
                    "  \"fields\" : {\n" +
                    "     \"body\" : {\n" +
                    "        \"bad_fieldname\" : { \"field\" : \"value\" }\n" +
                    "     }\n" +
                    "   }\n" +
                    "}\n");
            assertEquals("[highlight] failed to parse field [fields]", e.getMessage());
            assertEquals("[fields] failed to parse field [body]", e.getCause().getMessage());
            assertEquals("[highlight_field] unknown field [bad_fieldname], parser not found", e.getCause().getCause().getMessage());
        }
    }

    public void testStringInFieldsArray() throws IOException {
        ParsingException e = expectParseThrows(ParsingException.class, "{\"fields\" : [ \"junk\" ]}");
        assertEquals("[highlight] failed to parse field [fields]", e.getMessage());
        assertEquals(
                "[fields] can be a single object with any number of fields or an array where each entry is an object with a single field",
                e.getCause().getMessage());
    }

    public void testNoFieldsInObjectInFieldsArray() throws IOException {
        ParsingException e = expectParseThrows(ParsingException.class, "{\n" +
                "  \"fields\" : [ {\n" +
                "   }] \n" +
                "}\n");
        assertEquals("[highlight] failed to parse field [fields]", e.getMessage());
        assertEquals(
                "[fields] can be a single object with any number of fields or an array where each entry is an object with a single field",
                e.getCause().getMessage());
    }

    public void testTwoFieldsInObjectInFieldsArray() throws IOException {
        ParsingException e = expectParseThrows(ParsingException.class, "{\n" +
                "  \"fields\" : [ {\n" +
                "     \"body\" : {},\n" +
                "     \"nope\" : {}\n" +
                "   }] \n" +
                "}\n");
        assertEquals("[highlight] failed to parse field [fields]", e.getMessage());
        assertEquals(
                "[fields] can be a single object with any number of fields or an array where each entry is an object with a single field",
                e.getCause().getMessage());    }

     /**
     * test that build() outputs a {@link SearchContextHighlight} that is has similar parameters
     * than what we have in the random {@link HighlightBuilder}
     */
    public void testBuildSearchContextHighlight() throws IOException {
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        Index index = new Index(randomAsciiOfLengthBetween(1, 10), "_na_");
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(index, indexSettings);
        // shard context will only need indicesQueriesRegistry for building Query objects nested in highlighter
        QueryShardContext mockShardContext = new QueryShardContext(idxSettings, null, null, null, null, null, indicesQueriesRegistry,
                null, null, null) {
            @Override
            public MappedFieldType fieldMapper(String name) {
                TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name);
                return builder.build(new Mapper.BuilderContext(idxSettings.getSettings(), new ContentPath(1))).fieldType();
            }
        };
        mockShardContext.setMapUnmappedFieldAsString(true);

        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            HighlightBuilder highlightBuilder = randomHighlighterBuilder();
            SearchContextHighlight highlight = highlightBuilder.build(mockShardContext);
            for (SearchContextHighlight.Field field : highlight.fields()) {
                String encoder = highlightBuilder.encoder() != null ? highlightBuilder.encoder() : HighlightBuilder.DEFAULT_ENCODER;
                assertEquals(encoder, field.fieldOptions().encoder());
                final Field fieldBuilder = getFieldBuilderByName(highlightBuilder, field.field());
                assertNotNull("expected a highlight builder for field " + field.field(), fieldBuilder);
                FieldOptions fieldOptions = field.fieldOptions();

                BiConsumer<Function<AbstractHighlighterBuilder<?>, Object>, Function<FieldOptions, Object>> checkSame =
                        mergeBeforeChek(highlightBuilder, fieldBuilder, fieldOptions);

                checkSame.accept(AbstractHighlighterBuilder::boundaryChars, FieldOptions::boundaryChars);
                checkSame.accept(AbstractHighlighterBuilder::boundaryMaxScan, FieldOptions::boundaryMaxScan);
                checkSame.accept(AbstractHighlighterBuilder::fragmentSize, FieldOptions::fragmentCharSize);
                checkSame.accept(AbstractHighlighterBuilder::fragmenter, FieldOptions::fragmenter);
                checkSame.accept(AbstractHighlighterBuilder::requireFieldMatch, FieldOptions::requireFieldMatch);
                checkSame.accept(AbstractHighlighterBuilder::noMatchSize, FieldOptions::noMatchSize);
                checkSame.accept(AbstractHighlighterBuilder::numOfFragments, FieldOptions::numberOfFragments);
                checkSame.accept(AbstractHighlighterBuilder::phraseLimit, FieldOptions::phraseLimit);
                checkSame.accept(AbstractHighlighterBuilder::highlighterType, FieldOptions::highlighterType);
                checkSame.accept(AbstractHighlighterBuilder::highlightFilter, FieldOptions::highlightFilter);
                checkSame.accept(AbstractHighlighterBuilder::preTags, FieldOptions::preTags);
                checkSame.accept(AbstractHighlighterBuilder::postTags, FieldOptions::postTags);
                checkSame.accept(AbstractHighlighterBuilder::options, FieldOptions::options);
                checkSame.accept(AbstractHighlighterBuilder::order, op -> op.scoreOrdered() ? Order.SCORE : Order.NONE);
                assertEquals(fieldBuilder.fragmentOffset, fieldOptions.fragmentOffset());
                if (fieldBuilder.matchedFields != null) {
                    String[] copy = Arrays.copyOf(fieldBuilder.matchedFields, fieldBuilder.matchedFields.length);
                    Arrays.sort(copy);
                    assertArrayEquals(copy,
                            new TreeSet<String>(fieldOptions.matchedFields()).toArray(new String[fieldOptions.matchedFields().size()]));
                } else {
                    assertNull(fieldOptions.matchedFields());
                }
                Query expectedValue = null;
                if (fieldBuilder.highlightQuery != null) {
                    expectedValue = QueryBuilder.rewriteQuery(fieldBuilder.highlightQuery, mockShardContext).toQuery(mockShardContext);
                } else if (highlightBuilder.highlightQuery != null) {
                    expectedValue = QueryBuilder.rewriteQuery(highlightBuilder.highlightQuery, mockShardContext).toQuery(mockShardContext);
                }
                assertEquals(expectedValue, fieldOptions.highlightQuery());
            }
        }
    }

    /**
     * Create a generic helper function that performs all the work of merging the global highlight builder parameter,
     * the (potential) overwrite on the field level and the default value from {@link HighlightBuilder#defaultOptions}
     * before making the assertion that the value in the highlight builder and the actual value in the {@link FieldOptions}
     * passed in is the same.
     *
     * @param highlightBuilder provides the (optional) global builder parameter
     * @param fieldBuilder provides the (optional) field level parameter, if present this overwrites the global value
     * @param options the target field options that are checked
     */
    private static BiConsumer<Function<AbstractHighlighterBuilder<?>, Object>, Function<FieldOptions, Object>> mergeBeforeChek(
            HighlightBuilder highlightBuilder, Field fieldBuilder, FieldOptions options) {
        return (highlightBuilderParameterAccessor, fieldOptionsParameterAccessor) -> {
            Object expectedValue = null;
            Object globalLevelValue = highlightBuilderParameterAccessor.apply(highlightBuilder);
            Object fieldLevelValue = highlightBuilderParameterAccessor.apply(fieldBuilder);
            if (fieldLevelValue != null) {
                expectedValue = fieldLevelValue;
            } else if (globalLevelValue != null) {
                expectedValue = globalLevelValue;
            } else {
                expectedValue = fieldOptionsParameterAccessor.apply(HighlightBuilder.defaultOptions);
            }
            Object actualValue = fieldOptionsParameterAccessor.apply(options);
            if (actualValue instanceof String[]) {
                assertArrayEquals((String[]) expectedValue, (String[]) actualValue);
            } else if (actualValue instanceof Character[]) {
                if (expectedValue instanceof char[]) {
                    assertArrayEquals(HighlightBuilder.convertCharArray((char[]) expectedValue), (Character[]) actualValue);
                } else {
                    assertArrayEquals((Character[]) expectedValue, (Character[]) actualValue);
                }
            } else {
                assertEquals(expectedValue, actualValue);
            }
        };
    }

    private static Field getFieldBuilderByName(HighlightBuilder highlightBuilder, String fieldName) {
        for (Field hbfield : highlightBuilder.fields()) {
            if (hbfield.name().equals(fieldName)) {
                return hbfield;
            }
        }
        return null;
    }

    /**
     * `tags_schema` is not produced by toXContent in the builder but should be parseable, so this
     * adds a simple json test for this.
     */
    public void testParsingTagsSchema() throws IOException {

        String highlightElement = "{\n" +
                "    \"tags_schema\" : \"styled\"\n" +
                "}\n";
        XContentParser parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.EMPTY);
        HighlightBuilder highlightBuilder = HighlightBuilder.fromXContent(context);
        assertArrayEquals("setting tags_schema 'styled' should alter pre_tags", HighlightBuilder.DEFAULT_STYLED_PRE_TAG,
                highlightBuilder.preTags());
        assertArrayEquals("setting tags_schema 'styled' should alter post_tags", HighlightBuilder.DEFAULT_STYLED_POST_TAGS,
                highlightBuilder.postTags());

        highlightElement = "{\n" +
                "    \"tags_schema\" : \"default\"\n" +
                "}\n";
        parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.EMPTY);
        highlightBuilder = HighlightBuilder.fromXContent(context);
        assertArrayEquals("setting tags_schema 'default' should alter pre_tags", HighlightBuilder.DEFAULT_PRE_TAGS,
                highlightBuilder.preTags());
        assertArrayEquals("setting tags_schema 'default' should alter post_tags", HighlightBuilder.DEFAULT_POST_TAGS,
                highlightBuilder.postTags());

        ParsingException e = expectParseThrows(ParsingException.class, "{\n" +
                "    \"tags_schema\" : \"somthing_else\"\n" +
                "}\n");
        assertEquals("[highlight] failed to parse field [tags_schema]", e.getMessage());
        assertEquals("Unknown tag schema [somthing_else]", e.getCause().getMessage());
    }

    /**
     * test parsing empty highlight or empty fields blocks
     */
    public void testParsingEmptyStructure() throws IOException {
        String highlightElement = "{ }";
        XContentParser parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.EMPTY);
        HighlightBuilder highlightBuilder = HighlightBuilder.fromXContent(context);
        assertEquals("expected plain HighlightBuilder", new HighlightBuilder(), highlightBuilder);

        highlightElement = "{ \"fields\" : { } }";
        parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.EMPTY);
        highlightBuilder = HighlightBuilder.fromXContent(context);
        assertEquals("defining no field should return plain HighlightBuilder", new HighlightBuilder(), highlightBuilder);

        highlightElement = "{ \"fields\" : { \"foo\" : { } } }";
        parser = XContentFactory.xContent(highlightElement).createParser(highlightElement);

        context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.EMPTY);
        highlightBuilder = HighlightBuilder.fromXContent(context);
        assertEquals("expected HighlightBuilder with field", new HighlightBuilder().field(new Field("foo")), highlightBuilder);
    }

    public void testPreTagsWithoutPostTags() throws IOException {
        ParsingException e = expectParseThrows(ParsingException.class, "{\n" +
                "    \"pre_tags\" : [\"<a>\"]\n" +
                "}\n");
        assertEquals("pre_tags are set but post_tags are not set", e.getMessage());

        e = expectParseThrows(ParsingException.class, "{\n" +
                "  \"fields\" : {\n" +
                "     \"body\" : {\n" +
                "        \"pre_tags\" : [\"<a>\"]\n" +
                "     }\n" +
                "   }\n" +
                "}\n");
        assertEquals("[highlight] failed to parse field [fields]", e.getMessage());
        assertEquals("[fields] failed to parse field [body]", e.getCause().getMessage());
        assertEquals("pre_tags are set but post_tags are not set", e.getCause().getCause().getMessage());
    }

    /**
     * test ordinals of {@link Order}, since serialization depends on it
     */
    public void testValidOrderOrdinals() {
        assertThat(Order.NONE.ordinal(), equalTo(0));
        assertThat(Order.SCORE.ordinal(), equalTo(1));
    }

    public void testOrderSerialization() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Order.NONE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Order.SCORE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
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
     * create random highlight builder that is put under test
     */
    public static HighlightBuilder randomHighlighterBuilder() {
        HighlightBuilder testHighlighter = new HighlightBuilder();
        setRandomCommonOptions(testHighlighter);
        testHighlighter.useExplicitFieldOrder(randomBoolean());
        if (randomBoolean()) {
            testHighlighter.encoder(randomFrom(Arrays.asList(new String[]{"default", "html"})));
        }
        int numberOfFields = randomIntBetween(1,5);
        for (int i = 0; i < numberOfFields; i++) {
            Field field = new Field(i + "_" + randomAsciiOfLengthBetween(1, 10));
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
            if (randomBoolean()) {
                highlightBuilder.order(randomFrom(Order.values()));
            } else {
                // also test the string setter
                highlightBuilder.order(randomFrom(Order.values()).toString());
            }
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
            if (highlightBuilder.order() == Order.NONE) {
                highlightBuilder.order(Order.SCORE);
            } else {
                highlightBuilder.order(Order.NONE);
            }
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

    /**
     * Create array of unique Strings. If not unique, e.g. duplicates field names
     * would be dropped in {@link FieldOptions.Builder#matchedFields(Set)}, resulting in test glitches
     */
    private static String[] randomStringArray(int minSize, int maxSize) {
        int size = randomIntBetween(minSize, maxSize);
        Set<String> randomStrings = new HashSet<String>(size);
        for (int f = 0; f < size; f++) {
            randomStrings.add(randomAsciiOfLengthBetween(3, 10));
        }
        return randomStrings.toArray(new String[randomStrings.size()]);
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
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                return new HighlightBuilder(in);
            }
        }
    }
}
