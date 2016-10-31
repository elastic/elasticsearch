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

package org.elasticsearch.search;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilderTests;
import org.elasticsearch.search.rescore.QueryRescoreBuilderTests;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilderTests;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.ESTestCase;

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

public abstract class AbstractSearchTestCase extends ESTestCase {

    protected NamedWriteableRegistry namedWriteableRegistry;
    protected SearchRequestParsers searchRequestParsers;
    private TestSearchExtPlugin searchExtPlugin;
    protected IndicesQueriesRegistry queriesRegistry;

    public void setUp() throws Exception {
        super.setUp();
        IndicesModule indicesModule = new IndicesModule(Collections.emptyList());
        searchExtPlugin = new TestSearchExtPlugin();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.singletonList(searchExtPlugin));
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
        searchRequestParsers = searchModule.getSearchRequestParsers();
        queriesRegistry = searchModule.getQueryParserRegistry();
    }

    protected SearchSourceBuilder createSearchSourceBuilder() throws IOException {
        SearchSourceBuilder builder = new SearchSourceBuilder();
        if (randomBoolean()) {
            builder.from(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            builder.size(randomIntBetween(0, 10000));
        }
        if (randomBoolean()) {
            builder.explain(randomBoolean());
        }
        if (randomBoolean()) {
            builder.version(randomBoolean());
        }
        if (randomBoolean()) {
            builder.trackScores(randomBoolean());
        }
        if (randomBoolean()) {
            builder.minScore(randomFloat() * 1000);
        }
        if (randomBoolean()) {
            builder.timeout(TimeValue.parseTimeValue(randomTimeValue(), null, "timeout"));
        }
        if (randomBoolean()) {
            builder.terminateAfter(randomIntBetween(1, 100000));
        }

        switch(randomInt(2)) {
            case 0:
                builder.storedFields();
                break;
            case 1:
                builder.storedField("_none_");
                break;
            case 2:
                int fieldsSize = randomInt(25);
                List<String> fields = new ArrayList<>(fieldsSize);
                for (int i = 0; i < fieldsSize; i++) {
                    fields.add(randomAsciiOfLengthBetween(5, 50));
                }
                builder.storedFields(fields);
                break;
            default:
                throw new IllegalStateException();
        }

        if (randomBoolean()) {
            int scriptFieldsSize = randomInt(25);
            for (int i = 0; i < scriptFieldsSize; i++) {
                if (randomBoolean()) {
                    builder.scriptField(randomAsciiOfLengthBetween(5, 50), new Script("foo"), randomBoolean());
                } else {
                    builder.scriptField(randomAsciiOfLengthBetween(5, 50), new Script("foo"));
                }
            }
        }
        if (randomBoolean()) {
            FetchSourceContext fetchSourceContext;
            int branch = randomInt(5);
            String[] includes = new String[randomIntBetween(0, 20)];
            for (int i = 0; i < includes.length; i++) {
                includes[i] = randomAsciiOfLengthBetween(5, 20);
            }
            String[] excludes = new String[randomIntBetween(0, 20)];
            for (int i = 0; i < excludes.length; i++) {
                excludes[i] = randomAsciiOfLengthBetween(5, 20);
            }
            switch (branch) {
                case 0:
                    fetchSourceContext = new FetchSourceContext(randomBoolean());
                    break;
                case 1:
                    fetchSourceContext = new FetchSourceContext(true, includes, excludes);
                    break;
                case 2:
                    fetchSourceContext = new FetchSourceContext(true, new String[]{randomAsciiOfLengthBetween(5, 20)},
                        new String[]{randomAsciiOfLengthBetween(5, 20)});
                    break;
                case 3:
                    fetchSourceContext = new FetchSourceContext(true, includes, excludes);
                    break;
                case 4:
                    fetchSourceContext = new FetchSourceContext(true, includes, null);
                    break;
                case 5:
                    fetchSourceContext = new FetchSourceContext(true, new String[] {randomAsciiOfLengthBetween(5, 20)}, null);
                    break;
                default:
                    throw new IllegalStateException();
            }
            builder.fetchSource(fetchSourceContext);
        }
        if (randomBoolean()) {
            int size = randomIntBetween(0, 20);
            List<String> statsGroups = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                statsGroups.add(randomAsciiOfLengthBetween(5, 20));
            }
            builder.stats(statsGroups);
        }
        if (randomBoolean()) {
            int indexBoostSize = randomIntBetween(1, 10);
            for (int i = 0; i < indexBoostSize; i++) {
                builder.indexBoost(randomAsciiOfLengthBetween(5, 20), randomFloat() * 10);
            }
        }
        if (randomBoolean()) {
            builder.query(QueryBuilders.termQuery(randomAsciiOfLengthBetween(5, 20), randomAsciiOfLengthBetween(5, 20)));
        }
        if (randomBoolean()) {
            builder.postFilter(QueryBuilders.termQuery(randomAsciiOfLengthBetween(5, 20), randomAsciiOfLengthBetween(5, 20)));
        }
        if (randomBoolean()) {
            int numSorts = randomIntBetween(1, 5);
            for (int i = 0; i < numSorts; i++) {
                int branch = randomInt(5);
                switch (branch) {
                    case 0:
                        builder.sort(SortBuilders.fieldSort(randomAsciiOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
                        break;
                    case 1:
                        builder.sort(SortBuilders.geoDistanceSort(randomAsciiOfLengthBetween(5, 20),
                                AbstractQueryTestCase.randomGeohash(1, 12)).order(randomFrom(SortOrder.values())));
                        break;
                    case 2:
                        builder.sort(SortBuilders.scoreSort().order(randomFrom(SortOrder.values())));
                        break;
                    case 3:
                        builder.sort(SortBuilders.scriptSort(new Script("foo"),
                                ScriptSortBuilder.ScriptSortType.NUMBER).order(randomFrom(SortOrder.values())));
                        break;
                    case 4:
                        builder.sort(randomAsciiOfLengthBetween(5, 20));
                        break;
                    case 5:
                        builder.sort(randomAsciiOfLengthBetween(5, 20), randomFrom(SortOrder.values()));
                        break;
                }
            }
        }

        if (randomBoolean()) {
            int numSearchFrom = randomIntBetween(1, 5);
            // We build a json version of the search_from first in order to
            // ensure that every number type remain the same before/after xcontent (de)serialization.
            // This is not a problem because the final type of each field value is extracted from associated sort field.
            // This little trick ensure that equals and hashcode are the same when using the xcontent serialization.
            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
            jsonBuilder.startObject();
            jsonBuilder.startArray("search_from");
            for (int i = 0; i < numSearchFrom; i++) {
                int branch = randomInt(8);
                switch (branch) {
                    case 0:
                        jsonBuilder.value(randomInt());
                        break;
                    case 1:
                        jsonBuilder.value(randomFloat());
                        break;
                    case 2:
                        jsonBuilder.value(randomLong());
                        break;
                    case 3:
                        jsonBuilder.value(randomDouble());
                        break;
                    case 4:
                        jsonBuilder.value(randomAsciiOfLengthBetween(5, 20));
                        break;
                    case 5:
                        jsonBuilder.value(randomBoolean());
                        break;
                    case 6:
                        jsonBuilder.value(randomByte());
                        break;
                    case 7:
                        jsonBuilder.value(randomShort());
                        break;
                    case 8:
                        jsonBuilder.value(new Text(randomAsciiOfLengthBetween(5, 20)));
                        break;
                }
            }
            jsonBuilder.endArray();
            jsonBuilder.endObject();
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(jsonBuilder.bytes());
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();
            builder.searchAfter(SearchAfterBuilder.fromXContent(parser, null).getSortValues());
        }
        if (randomBoolean()) {
            builder.highlighter(HighlightBuilderTests.randomHighlighterBuilder());
        }
        if (randomBoolean()) {
            builder.suggest(SuggestBuilderTests.randomSuggestBuilder());
        }
        if (randomBoolean()) {
            int numRescores = randomIntBetween(1, 5);
            for (int i = 0; i < numRescores; i++) {
                builder.addRescorer(QueryRescoreBuilderTests.randomRescoreBuilder());
            }
        }
        if (randomBoolean()) {
            builder.aggregation(AggregationBuilders.avg(randomAsciiOfLengthBetween(5, 20)));
        }
        if (randomBoolean()) {
            Set<String> elementNames = new HashSet<>(searchExtPlugin.getSupportedElements().keySet());
            int numSearchExts = randomIntBetween(1, elementNames.size());
            while(elementNames.size() > numSearchExts) {
                elementNames.remove(randomFrom(elementNames));
            }
            List<SearchExtBuilder> searchExtBuilders = new ArrayList<>();
            for (String elementName : elementNames) {
                searchExtBuilders.add(searchExtPlugin.getSupportedElements().get(elementName).apply(randomAsciiOfLengthBetween(3, 10)));
            }
            builder.ext(searchExtBuilders);
        }
        if (randomBoolean()) {
            String field = randomBoolean() ? null : randomAsciiOfLengthBetween(5, 20);
            int max = between(2, 1000);
            int id = randomInt(max-1);
            if (field == null) {
                builder.slice(new SliceBuilder(id, max));
            } else {
                builder.slice(new SliceBuilder(field, id, max));
            }
        }
        return builder;
    }

    protected SearchRequest createSearchRequest() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        if (randomBoolean()) {
            searchRequest.indices(generateRandomStringArray(10, 10, false, false));
        }
        if (randomBoolean()) {
            searchRequest.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            searchRequest.types(generateRandomStringArray(10, 10, false, false));
        }
        if (randomBoolean()) {
            searchRequest.preference(randomAsciiOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.requestCache(randomBoolean());
        }
        if (randomBoolean()) {
            searchRequest.routing(randomAsciiOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            searchRequest.scroll(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            searchRequest.searchType(randomFrom(SearchType.values()));
        }
        if (randomBoolean()) {
            searchRequest.source(createSearchSourceBuilder());
        }
        return searchRequest;
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
                            this.searchExtSpecs.add(new SearchExtSpec<>(TestSearchExtBuilder1.NAME, TestSearchExtBuilder1::new,
                                    new TestSearchExtParser<>(TestSearchExtBuilder1::new)));
                        }
                        break;
                    case 1:
                        if (this.supportedElements.put(TestSearchExtBuilder2.NAME, TestSearchExtBuilder2::new) == null) {
                            this.searchExtSpecs.add(new SearchExtSpec<>(TestSearchExtBuilder2.NAME, TestSearchExtBuilder2::new,
                                    new TestSearchExtParser<>(TestSearchExtBuilder2::new)));
                        }
                        break;
                    case 2:
                        if (this.supportedElements.put(TestSearchExtBuilder3.NAME, TestSearchExtBuilder3::new) == null) {
                            this.searchExtSpecs.add(new SearchExtSpec<>(TestSearchExtBuilder3.NAME, TestSearchExtBuilder3::new,
                                    new TestSearchExtParser<>(TestSearchExtBuilder3::new)));
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

    private static class TestSearchExtParser<T extends SearchExtBuilder> implements SearchExtParser<T> {
        private final Function<String, T> searchExtBuilderFunction;

        TestSearchExtParser(Function<String, T> searchExtBuilderFunction) {
            this.searchExtBuilderFunction = searchExtBuilderFunction;
        }

        @Override
        public T fromXContent(XContentParser parser) throws IOException {
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

    //Would be nice to have a single builder that gets its name as a parameter, but the name wouldn't get a value when the object
    //is created reading from the stream (constructor that takes a StreamInput) which is a problem as we check that after reading
    //a named writeable its name is the expected one. That's why we go for the following less dynamic approach.
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
            return Objects.equals(objectName, that.objectName) &&
                    Objects.equals(name, that.name);
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
