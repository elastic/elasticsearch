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

package org.elasticsearch.search.suggest;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.collect.Tuple;
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
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper.StringFieldType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptServiceTests.TestEngineService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public abstract class AbstractSuggestionBuilderTestCase<SB extends SuggestionBuilder<SB>> extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    protected static NamedWriteableRegistry namedWriteableRegistry;
    private static Suggesters suggesters;
    private static ScriptService scriptService;
    private static SuggestParseElement parseElement;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() throws IOException {
        Path genericConfigFolder = createTempDir();
        Settings baseSettings = settingsBuilder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put(Environment.PATH_CONF_SETTING.getKey(), genericConfigFolder)
                .build();
        Environment environment = new Environment(baseSettings);
        ScriptContextRegistry scriptContextRegistry = new ScriptContextRegistry(Collections.emptyList());
        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(Collections.singletonList(new ScriptEngineRegistry
                .ScriptEngineRegistration(TestEngineService.class, TestEngineService.TYPES)));
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, scriptContextRegistry);
        scriptService = new ScriptService(baseSettings, environment, Collections.singleton(new TestEngineService()),
                new ResourceWatcherService(baseSettings, null), scriptEngineRegistry, scriptContextRegistry, scriptSettings) {
            @Override
            public CompiledScript compile(Script script, ScriptContext scriptContext, Map<String, String> params) {
                return new CompiledScript(ScriptType.INLINE, "mockName", "mocklang", script);
            }
        };
        suggesters = new Suggesters(Collections.emptyMap());
        parseElement = new SuggestParseElement(suggesters);

        namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.registerPrototype(SuggestionBuilder.class, TermSuggestionBuilder.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(SuggestionBuilder.class, PhraseSuggestionBuilder.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(SuggestionBuilder.class, CompletionSuggestionBuilder.PROTOTYPE);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
    }

    /**
     * Test serialization and deserialization of the suggestion builder
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB original = randomTestBuilder();
            SB deserialized = serializedCopy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
        }
    }

    /**
     * returns a random suggestion builder, setting the common options randomly
     */
    protected SB randomTestBuilder() {
        SB randomSuggestion = randomSuggestionBuilder();
        maybeSet(randomSuggestion::text, randomAsciiOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::prefix, randomAsciiOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::regex, randomAsciiOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::analyzer, randomAsciiOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::size, randomIntBetween(1, 20));
        maybeSet(randomSuggestion::shardSize, randomIntBetween(1, 20));
        return randomSuggestion;
    }

    /**
     * create a randomized {@link SuggestBuilder} that is used in further tests
     */
    protected abstract SB randomSuggestionBuilder();

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB firstBuilder = randomTestBuilder();
            assertFalse("suggestion builder is equal to null", firstBuilder.equals(null));
            assertFalse("suggestion builder is equal to incompatible type", firstBuilder.equals(""));
            assertTrue("suggestion builder is not equal to self", firstBuilder.equals(firstBuilder));
            assertThat("same suggestion builder's hashcode returns different values if called multiple times", firstBuilder.hashCode(),
                    equalTo(firstBuilder.hashCode()));
        final SB mutate = mutate(firstBuilder);
        assertThat("different suggestion builders should not be equal", mutate, not(equalTo(firstBuilder)));

            SB secondBuilder = serializedCopy(firstBuilder);
            assertTrue("suggestion builder is not equal to self", secondBuilder.equals(secondBuilder));
            assertTrue("suggestion builder is not equal to its copy", firstBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", secondBuilder.equals(firstBuilder));
            assertThat("suggestion builder copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(firstBuilder.hashCode()));

            SB thirdBuilder = serializedCopy(secondBuilder);
            assertTrue("suggestion builder is not equal to self", thirdBuilder.equals(thirdBuilder));
            assertTrue("suggestion builder is not equal to its copy", secondBuilder.equals(thirdBuilder));
            assertThat("suggestion builder copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not transitive", firstBuilder.equals(thirdBuilder));
            assertThat("suggestion builder copy's hashcode is different from original hashcode", firstBuilder.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not symmetric", thirdBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", thirdBuilder.equals(firstBuilder));
        }
    }

    /**
     * creates random suggestion builder, renders it to xContent and back to new
     * instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        QueryParseContext context = new QueryParseContext(null);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB suggestionBuilder = randomTestBuilder();
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                xContentBuilder.prettyPrint();
            }
            xContentBuilder.startObject();
            suggestionBuilder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            xContentBuilder.endObject();

            XContentParser parser = XContentHelper.createParser(xContentBuilder.bytes());
            context.reset(parser);
            // we need to skip the start object and the name, those will be parsed by outer SuggestBuilder
            parser.nextToken();

            SuggestionBuilder<?> secondSuggestionBuilder = SuggestionBuilder.fromXContent(context, suggesters);
            assertNotSame(suggestionBuilder, secondSuggestionBuilder);
            assertEquals(suggestionBuilder, secondSuggestionBuilder);
            assertEquals(suggestionBuilder.hashCode(), secondSuggestionBuilder.hashCode());
        }
    }

    protected Tuple<MapperService, SB> mockMapperServiceAndSuggestionBuilder(
        IndexSettings idxSettings, AnalysisService mockAnalysisService, SB suggestBuilder) {
        final MapperService mapperService = new MapperService(idxSettings, mockAnalysisService, null,
            new IndicesModule().getMapperRegistry(), null) {
            @Override
            public MappedFieldType fullName(String fullName) {
                StringFieldType type = new StringFieldType();
                if (randomBoolean()) {
                    type.setSearchAnalyzer(new NamedAnalyzer("foo", new WhitespaceAnalyzer()));
                }
                return type;
            }
        };
        return new Tuple<>(mapperService, suggestBuilder);
    }

    /**
     * parses random suggestion builder via old parseElement method and via
     * build, comparing the results for equality
     */
    public void testBuild() throws IOException {
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAsciiOfLengthBetween(1, 10), Settings.EMPTY);

        AnalysisService mockAnalysisService = new AnalysisService(idxSettings, Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap()) {
            @Override
            public NamedAnalyzer analyzer(String name) {
                return new NamedAnalyzer(name, new WhitespaceAnalyzer());
            }
        };

        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SuggestBuilder suggestBuilder = new SuggestBuilder();
            SB suggestionBuilder = randomTestBuilder();
            Tuple<MapperService, SB> mapperServiceSBTuple =
                mockMapperServiceAndSuggestionBuilder(idxSettings, mockAnalysisService, suggestionBuilder);
            suggestionBuilder = mapperServiceSBTuple.v2();
            QueryShardContext mockShardContext = new QueryShardContext(idxSettings,
                null, null, mapperServiceSBTuple.v1(), null, scriptService, null) {
                @Override
                public MappedFieldType fieldMapper(String name) {
                    StringFieldMapper.Builder builder = new StringFieldMapper.Builder(name);
                    return builder.build(new Mapper.BuilderContext(idxSettings.getSettings(), new ContentPath(1))).fieldType();
                }
            };
            mockShardContext.setMapUnmappedFieldAsString(true);
            suggestBuilder.addSuggestion(randomAsciiOfLength(10), suggestionBuilder);

            if (suggestionBuilder.text() == null) {
                // we either need suggestion text or global text
                suggestBuilder.setGlobalText(randomAsciiOfLengthBetween(5, 50));
            }
            if (suggestionBuilder.text() != null && suggestionBuilder.prefix() != null) {
                suggestionBuilder.prefix(null);
            }

            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                xContentBuilder.prettyPrint();
            }
            suggestBuilder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            XContentParser parser = XContentHelper.createParser(xContentBuilder.bytes());
            parser.nextToken(); // set cursor to START_OBJECT
            SuggestionSearchContext parsedSuggestionSearchContext = parseElement.parseInternal(parser, mockShardContext);

            SuggestionSearchContext buildSuggestSearchContext = suggestBuilder.build(mockShardContext);
            assertEquals(parsedSuggestionSearchContext.suggestions().size(), buildSuggestSearchContext.suggestions().size());
            Iterator<Map.Entry<String, SuggestionContext>> iterator =  buildSuggestSearchContext.suggestions().entrySet().iterator();
            for (Map.Entry<String, SuggestionContext> entry : parsedSuggestionSearchContext.suggestions().entrySet()) {
                Map.Entry<String, SuggestionContext> other = iterator.next();
                assertEquals(entry.getKey(), other.getKey());

                SuggestionContext oldSchoolContext = entry.getValue();
                SuggestionContext newSchoolContext = other.getValue();
                assertNotSame(oldSchoolContext, newSchoolContext);
                // deep comparison of analyzers is difficult here, but we check they are set or not set
                if (oldSchoolContext.getAnalyzer() != null) {
                    assertNotNull(newSchoolContext.getAnalyzer());
                } else {
                    assertNull(newSchoolContext.getAnalyzer());
                }
                assertEquals(oldSchoolContext.getField(), newSchoolContext.getField());
                assertEquals(oldSchoolContext.getPrefix(), newSchoolContext.getPrefix());
                assertEquals(oldSchoolContext.getRegex(), newSchoolContext.getRegex());
                assertEquals(oldSchoolContext.getShardSize(), newSchoolContext.getShardSize());
                assertEquals(oldSchoolContext.getSize(), newSchoolContext.getSize());
                assertEquals(oldSchoolContext.getSuggester().getClass(), newSchoolContext.getSuggester().getClass());
                assertEquals(oldSchoolContext.getText(), newSchoolContext.getText());
                assertEquals(oldSchoolContext.getClass(), newSchoolContext.getClass());

                assertSuggestionContext(oldSchoolContext, newSchoolContext);
            }
        }
    }

    /**
     * compare two SuggestionContexte implementations for the special suggestion type under test
     */
    protected abstract void assertSuggestionContext(SuggestionContext oldSuggestion, SuggestionContext newSuggestion);

    private SB mutate(SB firstBuilder) throws IOException {
        SB mutation = serializedCopy(firstBuilder);
        assertNotSame(mutation, firstBuilder);
        // change ither one of the shared SuggestionBuilder parameters, or delegate to the specific tests mutate method
        if (randomBoolean()) {
            switch (randomIntBetween(0, 5)) {
            case 0:
                mutation.text(randomValueOtherThan(mutation.text(), () -> randomAsciiOfLengthBetween(2, 20)));
                break;
            case 1:
                mutation.prefix(randomValueOtherThan(mutation.prefix(), () -> randomAsciiOfLengthBetween(2, 20)));
                break;
            case 2:
                mutation.regex(randomValueOtherThan(mutation.regex(), () -> randomAsciiOfLengthBetween(2, 20)));
                break;
            case 3:
                mutation.analyzer(randomValueOtherThan(mutation.analyzer(), () -> randomAsciiOfLengthBetween(2, 20)));
                break;
            case 4:
                mutation.size(randomValueOtherThan(mutation.size(), () -> randomIntBetween(1, 20)));
                break;
            case 5:
                mutation.shardSize(randomValueOtherThan(mutation.shardSize(), () -> randomIntBetween(1, 20)));
                break;
            }
        } else {
            mutateSpecificParameters(firstBuilder);
        }
        return mutation;
    }

    /**
     * take and input {@link SuggestBuilder} and return another one that is
     * different in one aspect (to test non-equality)
     */
    protected abstract void mutateSpecificParameters(SB firstBuilder) throws IOException;

    @SuppressWarnings("unchecked")
    protected SB serializedCopy(SB original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeSuggestion(original);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                return (SB) in.readSuggestion();
            }
        }
    }
}
