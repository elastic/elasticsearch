/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MockFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.lucene.BytesRefs.toBytesRef;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractSuggestionBuilderTestCase<SB extends SuggestionBuilder<SB>> extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    protected static NamedWriteableRegistry namedWriteableRegistry;
    protected static NamedXContentRegistry xContentRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @AfterClass
    public static void afterClass() {
        namedWriteableRegistry = null;
        xContentRegistry = null;
    }

    /**
     * Test serialization and deserialization of the suggestion builder
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB original = randomTestBuilder();
            SB deserialized = copy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
        }
    }

    /**
     * returns a random suggestion builder, setting the common options randomly
     */
    protected SB randomTestBuilder() {
        return randomSuggestionBuilder();
    }

    public static void setCommonPropertiesOnRandomBuilder(SuggestionBuilder<?> randomSuggestion) {
        randomSuggestion.text(randomAlphaOfLengthBetween(2, 20)); // have to set the text because we don't know if the global text was set
        maybeSet(randomSuggestion::prefix, randomAlphaOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::regex, randomAlphaOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::analyzer, randomAlphaOfLengthBetween(2, 20));
        maybeSet(randomSuggestion::size, randomIntBetween(1, 20));
        maybeSet(randomSuggestion::shardSize, randomIntBetween(1, 20));
    }

    /**
     * create a randomized {@link SuggestBuilder} that is used in further tests
     */
    protected abstract SB randomSuggestionBuilder();

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            checkEqualsAndHashCode(randomTestBuilder(), this::copy, this::mutate);
        }
    }

    /**
     * creates random suggestion builder, renders it to xContent and back to new
     * instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB suggestionBuilder = randomTestBuilder();
            XContentBuilder xContentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                xContentBuilder.prettyPrint();
            }
            xContentBuilder.startObject();
            suggestionBuilder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            xContentBuilder.endObject();

            XContentBuilder shuffled = shuffleXContent(xContentBuilder, shuffleProtectedFields());
            try (XContentParser parser = createParser(shuffled)) {
                // we need to skip the start object and the name, those will be parsed by outer SuggestBuilder
                parser.nextToken();

                SuggestionBuilder<?> secondSuggestionBuilder = SuggestionBuilder.fromXContent(parser);
                assertNotSame(suggestionBuilder, secondSuggestionBuilder);
                assertEquals(suggestionBuilder, secondSuggestionBuilder);
                assertEquals(suggestionBuilder.hashCode(), secondSuggestionBuilder.hashCode());
            }
        }
    }

    public void testBuild() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            SB suggestionBuilder = randomTestBuilder();
            Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(
                new Index(randomAlphaOfLengthBetween(1, 10), "_na_"),
                indexSettings
            );
            ScriptService scriptService = mock(ScriptService.class);
            MappedFieldType fieldType = mockFieldType(suggestionBuilder.field());
            IndexAnalyzers indexAnalyzers = (type, name) -> new NamedAnalyzer(name, AnalyzerScope.INDEX, new SimpleAnalyzer());
            ;
            MapperService mapperService = mock(MapperService.class);
            when(mapperService.getIndexAnalyzers()).thenReturn(indexAnalyzers);
            when(scriptService.compile(any(Script.class), any())).then(
                invocation -> new TestTemplateService.MockTemplateScript.Factory(((Script) invocation.getArguments()[0]).getIdOrCode())
            );
            List<FieldMapper> mappers = Collections.singletonList(new MockFieldMapper(fieldType));
            MappingLookup lookup = MappingLookup.fromMappers(Mapping.EMPTY, mappers, emptyList(), emptyList());
            SearchExecutionContext mockContext = new SearchExecutionContext(
                0,
                0,
                idxSettings,
                ClusterSettings.createBuiltInClusterSettings(),
                null,
                null,
                mapperService,
                lookup,
                null,
                scriptService,
                parserConfig(),
                namedWriteableRegistry,
                null,
                null,
                System::currentTimeMillis,
                null,
                null,
                () -> true,
                null,
                emptyMap()
            );

            SuggestionContext suggestionContext = suggestionBuilder.build(mockContext);
            assertEquals(toBytesRef(suggestionBuilder.text()), suggestionContext.getText());
            if (suggestionBuilder.text() != null && suggestionBuilder.prefix() == null) {
                assertEquals(toBytesRef(suggestionBuilder.text()), suggestionContext.getPrefix());
            } else {
                assertEquals(toBytesRef(suggestionBuilder.prefix()), suggestionContext.getPrefix());
            }
            assertEquals(toBytesRef(suggestionBuilder.regex()), suggestionContext.getRegex());
            assertEquals(suggestionBuilder.field(), suggestionContext.getField());
            int expectedSize = suggestionBuilder.size() != null ? suggestionBuilder.size : 5;
            assertEquals(expectedSize, suggestionContext.getSize());
            Integer expectedShardSize = suggestionBuilder.shardSize != null ? suggestionBuilder.shardSize : Math.max(expectedSize, 5);
            assertEquals(expectedShardSize, suggestionContext.getShardSize());
            assertSame(mockContext, suggestionContext.getSearchExecutionContext());
            if (suggestionBuilder.analyzer() != null) {
                assertEquals(suggestionBuilder.analyzer(), ((NamedAnalyzer) suggestionContext.getAnalyzer()).name());
            } else {
                assertEquals("fieldSearchAnalyzer", ((NamedAnalyzer) suggestionContext.getAnalyzer()).name());
            }
            assertSuggestionContext(suggestionBuilder, suggestionContext);
        }
    }

    public void testBuildWithUnmappedField() {
        Settings.Builder builder = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        if (randomBoolean()) {
            builder.put(IndexSettings.ALLOW_UNMAPPED.getKey(), randomBoolean());
        }
        Settings indexSettings = builder.build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(
            new Index(randomAlphaOfLengthBetween(1, 10), "_na_"),
            indexSettings
        );

        SearchExecutionContext mockContext = new SearchExecutionContext(
            0,
            0,
            idxSettings,
            ClusterSettings.createBuiltInClusterSettings(),
            null,
            null,
            mock(MapperService.class),
            MappingLookup.EMPTY,
            null,
            null,
            parserConfig(),
            namedWriteableRegistry,
            null,
            null,
            System::currentTimeMillis,
            null,
            null,
            () -> true,
            null,
            emptyMap()
        );
        if (randomBoolean()) {
            mockContext.setAllowUnmappedFields(randomBoolean());
        }

        SB suggestionBuilder = randomTestBuilder();
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> suggestionBuilder.build(mockContext));
        assertEquals("no mapping found for field [" + suggestionBuilder.field + "]", iae.getMessage());
    }

    /**
     * put implementation dependent assertions in the sub-type test
     */
    protected abstract void assertSuggestionContext(SB builder, SuggestionContext context) throws IOException;

    protected MappedFieldType mockFieldType(String fieldName) {
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.name()).thenReturn(fieldName.intern()); // intern field name to not trip assertions that ensure all field names are
                                                               // interned
        NamedAnalyzer searchAnalyzer = new NamedAnalyzer("fieldSearchAnalyzer", AnalyzerScope.INDEX, new SimpleAnalyzer());
        TextSearchInfo tsi = new TextSearchInfo(TextFieldMapper.Defaults.FIELD_TYPE, null, searchAnalyzer, searchAnalyzer);
        when(fieldType.getTextSearchInfo()).thenReturn(tsi);
        return fieldType;
    }

    /**
     * Subclasses can override this method and return a set of fields which should be protected from
     * recursive random shuffling in the {@link #testFromXContent()} test case
     */
    protected String[] shuffleProtectedFields() {
        return new String[0];
    }

    private SB mutate(SB firstBuilder) throws IOException {
        SB mutation = copy(firstBuilder);
        assertNotSame(mutation, firstBuilder);
        // change ither one of the shared SuggestionBuilder parameters, or delegate to the specific tests mutate method
        if (randomBoolean()) {
            switch (randomIntBetween(0, 5)) {
                case 0 -> mutation.text(randomValueOtherThan(mutation.text(), () -> randomAlphaOfLengthBetween(2, 20)));
                case 1 -> mutation.prefix(randomValueOtherThan(mutation.prefix(), () -> randomAlphaOfLengthBetween(2, 20)));
                case 2 -> mutation.regex(randomValueOtherThan(mutation.regex(), () -> randomAlphaOfLengthBetween(2, 20)));
                case 3 -> mutation.analyzer(randomValueOtherThan(mutation.analyzer(), () -> randomAlphaOfLengthBetween(2, 20)));
                case 4 -> mutation.size(randomValueOtherThan(mutation.size(), () -> randomIntBetween(1, 20)));
                case 5 -> mutation.shardSize(randomValueOtherThan(mutation.shardSize(), () -> randomIntBetween(1, 20)));
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
    protected SB copy(SB original) throws IOException {
        return copyWriteable(
            original,
            namedWriteableRegistry,
            (Writeable.Reader<SB>) namedWriteableRegistry.getReader(SuggestionBuilder.class, original.getWriteableName())
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }
}
