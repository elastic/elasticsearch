/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilderTests;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilderTests;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.TestRankBuilder;
import org.elasticsearch.search.rescore.QueryRescorerBuilderTests;
import org.elasticsearch.search.suggest.SuggestBuilderTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractSearchTestCase extends ESTestCase {

    protected NamedWriteableRegistry namedWriteableRegistry;
    private TestSearchExtPlugin searchExtPlugin;
    private NamedXContentRegistry xContentRegistry;

    public void setUp() throws Exception {
        super.setUp();
        searchExtPlugin = new TestSearchExtPlugin();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.singletonList(searchExtPlugin));
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.addAll(IndicesModule.getNamedWriteables());
        namedWriteables.addAll(searchModule.getNamedWriteables());
        namedWriteables.add(new NamedWriteableRegistry.Entry(RankBuilder.class, TestRankBuilder.NAME, TestRankBuilder::new));
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();
        namedXContents.addAll(searchModule.getNamedXContents());
        namedXContents.add(
            new NamedXContentRegistry.Entry(RankBuilder.class, new ParseField(TestRankBuilder.NAME), TestRankBuilder::fromXContent)
        );
        xContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    protected SearchSourceBuilder createSearchSourceBuilder() {
        Supplier<List<SearchExtBuilder>> randomExtBuilders = () -> {
            Set<String> elementNames = new HashSet<>(searchExtPlugin.getSupportedElements().keySet());
            int numSearchExts = randomIntBetween(1, elementNames.size());
            while (elementNames.size() > numSearchExts) {
                elementNames.remove(randomFrom(elementNames));
            }
            List<SearchExtBuilder> searchExtBuilders = new ArrayList<>();
            for (String elementName : elementNames) {
                searchExtBuilders.add(searchExtPlugin.getSupportedElements().get(elementName).apply(randomAlphaOfLengthBetween(3, 10)));
            }
            return searchExtBuilders;
        };
        return RandomSearchRequestGenerator.randomSearchSourceBuilder(
            HighlightBuilderTests::randomHighlighterBuilder,
            SuggestBuilderTests::randomSuggestBuilder,
            TestRankBuilder::randomRankBuilder,
            QueryRescorerBuilderTests::randomRescoreBuilder,
            randomExtBuilders,
            CollapseBuilderTests::randomCollapseBuilder,
            AbstractSearchTestCase::randomRuntimeMappings
        );
    }

    public static Map<String, Object> randomRuntimeMappings() {
        int count = between(1, 100);
        Map<String, Object> runtimeFields = Maps.newMapWithExpectedSize(count);
        while (runtimeFields.size() < count) {
            int size = between(1, 10);
            Map<String, Object> config = Maps.newMapWithExpectedSize(size);
            while (config.size() < size) {
                config.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
            runtimeFields.put(randomAlphaOfLength(5), config);
        }
        return runtimeFields;
    }

    protected SearchRequest createSearchRequest() throws IOException {
        return RandomSearchRequestGenerator.randomSearchRequest(this::createSearchSourceBuilder);
    }

    private static class TestSearchExtPlugin extends Plugin implements SearchPlugin {
        private final List<SearchExtSpec<? extends SearchExtBuilder>> searchExtSpecs;
        private final Map<String, Function<String, ? extends SearchExtBuilder>> supportedElements;

        private TestSearchExtPlugin() {
            int numSearchExts = randomIntBetween(1, 3);
            this.searchExtSpecs = new ArrayList<>(numSearchExts);
            this.supportedElements = new HashMap<>();
            for (int i = 0; i < numSearchExts; i++) {
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        if (this.supportedElements.put(TestSearchExtBuilder1.NAME, TestSearchExtBuilder1::new) == null) {
                            this.searchExtSpecs.add(
                                new SearchExtSpec<>(
                                    TestSearchExtBuilder1.NAME,
                                    TestSearchExtBuilder1::new,
                                    new TestSearchExtParser<>(TestSearchExtBuilder1::new)
                                )
                            );
                        }
                        break;
                    case 1:
                        if (this.supportedElements.put(TestSearchExtBuilder2.NAME, TestSearchExtBuilder2::new) == null) {
                            this.searchExtSpecs.add(
                                new SearchExtSpec<>(
                                    TestSearchExtBuilder2.NAME,
                                    TestSearchExtBuilder2::new,
                                    new TestSearchExtParser<>(TestSearchExtBuilder2::new)
                                )
                            );
                        }
                        break;
                    case 2:
                        if (this.supportedElements.put(TestSearchExtBuilder3.NAME, TestSearchExtBuilder3::new) == null) {
                            this.searchExtSpecs.add(
                                new SearchExtSpec<>(
                                    TestSearchExtBuilder3.NAME,
                                    TestSearchExtBuilder3::new,
                                    new TestSearchExtParser<>(TestSearchExtBuilder3::new)
                                )
                            );
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
        }

        Map<String, Function<String, ? extends SearchExtBuilder>> getSupportedElements() {
            return supportedElements;
        }

        @Override
        public List<SearchExtSpec<?>> getSearchExts() {
            return searchExtSpecs;
        }
    }

    private static class TestSearchExtParser<T extends SearchExtBuilder> implements CheckedFunction<XContentParser, T, IOException> {
        private final Function<String, T> searchExtBuilderFunction;

        TestSearchExtParser(Function<String, T> searchExtBuilderFunction) {
            this.searchExtBuilderFunction = searchExtBuilderFunction;
        }

        @Override
        public T apply(XContentParser parser) throws IOException {
            return searchExtBuilderFunction.apply(parseField(parser));
        }

        String parseField(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "start_object expected, found " + parser.currentToken());
            }
            if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(), "field_name expected, found " + parser.currentToken());
            }
            String field = parser.currentName();
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "start_object expected, found " + parser.currentToken());
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "end_object expected, found " + parser.currentToken());
            }
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "end_object expected, found " + parser.currentToken());
            }
            return field;
        }
    }

    // Would be nice to have a single builder that gets its name as a parameter, but the name wouldn't get a value when the object
    // is created reading from the stream (constructor that takes a StreamInput) which is a problem as we check that after reading
    // a named writeable its name is the expected one. That's why we go for the following less dynamic approach.
    private static class TestSearchExtBuilder1 extends TestSearchExtBuilder {
        private static final String NAME = "name1";

        TestSearchExtBuilder1(String field) {
            super(NAME, field);
        }

        TestSearchExtBuilder1(StreamInput in) throws IOException {
            super(NAME, in);
        }
    }

    private static class TestSearchExtBuilder2 extends TestSearchExtBuilder {
        private static final String NAME = "name2";

        TestSearchExtBuilder2(String field) {
            super(NAME, field);
        }

        TestSearchExtBuilder2(StreamInput in) throws IOException {
            super(NAME, in);
        }
    }

    private static class TestSearchExtBuilder3 extends TestSearchExtBuilder {
        private static final String NAME = "name3";

        TestSearchExtBuilder3(String field) {
            super(NAME, field);
        }

        TestSearchExtBuilder3(StreamInput in) throws IOException {
            super(NAME, in);
        }
    }

    private abstract static class TestSearchExtBuilder extends SearchExtBuilder {
        final String objectName;
        protected final String name;

        TestSearchExtBuilder(String name, String objectName) {
            this.name = name;
            this.objectName = objectName;
        }

        TestSearchExtBuilder(String name, StreamInput in) throws IOException {
            this.name = name;
            this.objectName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(objectName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestSearchExtBuilder that = (TestSearchExtBuilder) o;
            return Objects.equals(objectName, that.objectName) && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(objectName, name);
        }

        @Override
        public String getWriteableName() {
            return name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            builder.startObject(objectName);
            builder.endObject();
            builder.endObject();
            return builder;
        }
    }
}
