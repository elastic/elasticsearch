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
package org.elasticsearch.index.query;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.ParseFieldMatcher;
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
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightBuilderTests;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class InnerHitBuilderTests extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;
    private static IndicesQueriesRegistry indicesQueriesRegistry;

    @BeforeClass
    public static void init() {
        namedWriteableRegistry = new NamedWriteableRegistry();
        indicesQueriesRegistry = new SearchModule(Settings.EMPTY, namedWriteableRegistry).getQueryParserRegistry();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
        indicesQueriesRegistry = null;
    }

    public void testSerialization() throws Exception {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            InnerHitBuilder original = randomInnerHits();
            InnerHitBuilder deserialized = serializedCopy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
        }
    }

    public void testFromAndToXContent() throws Exception {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            InnerHitBuilder innerHit = randomInnerHits(true, false);
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            innerHit.toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentBuilder shuffled = shuffleXContent(builder);
            if (randomBoolean()) {
                shuffled.prettyPrint();
            }

            XContentParser parser = XContentHelper.createParser(shuffled.bytes());
            QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, ParseFieldMatcher.EMPTY);
            InnerHitBuilder secondInnerHits = InnerHitBuilder.fromXContent(context);
            assertThat(innerHit, not(sameInstance(secondInnerHits)));
            assertThat(innerHit, equalTo(secondInnerHits));
            assertThat(innerHit.hashCode(), equalTo(secondInnerHits.hashCode()));
        }
    }

    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            InnerHitBuilder firstInnerHit = randomInnerHits();
            assertFalse("inner hit is equal to null", firstInnerHit.equals(null));
            assertFalse("inner hit is equal to incompatible type", firstInnerHit.equals(""));
            assertTrue("inner it is not equal to self", firstInnerHit.equals(firstInnerHit));
            assertThat("same inner hit's hashcode returns different values if called multiple times", firstInnerHit.hashCode(),
                    equalTo(firstInnerHit.hashCode()));
            assertThat("different inner hits should not be equal", mutate(serializedCopy(firstInnerHit)), not(equalTo(firstInnerHit)));

            InnerHitBuilder secondBuilder = serializedCopy(firstInnerHit);
            assertTrue("inner hit is not equal to self", secondBuilder.equals(secondBuilder));
            assertTrue("inner hit is not equal to its copy", firstInnerHit.equals(secondBuilder));
            assertTrue("equals is not symmetric", secondBuilder.equals(firstInnerHit));
            assertThat("inner hits copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(firstInnerHit.hashCode()));

            InnerHitBuilder thirdBuilder = serializedCopy(secondBuilder);
            assertTrue("inner hit is not equal to self", thirdBuilder.equals(thirdBuilder));
            assertTrue("inner hit is not equal to its copy", secondBuilder.equals(thirdBuilder));
            assertThat("inner hit copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not transitive", firstInnerHit.equals(thirdBuilder));
            assertThat("inner hit copy's hashcode is different from original hashcode", firstInnerHit.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not symmetric", thirdBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", thirdBuilder.equals(firstInnerHit));
        }
    }

    public void testInlineLeafInnerHitsNestedQuery() throws Exception {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        nestedQueryBuilder.innerHit(leafInnerHits);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        nestedQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsHasChildQuery() throws Exception {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder("type", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        hasChildQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsHasParentQuery() throws Exception {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder("type", new MatchAllQueryBuilder(), false)
                .innerHit(leafInnerHits);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        hasParentQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaBoolQuery() {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().should(nestedQueryBuilder);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        boolQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaConstantScoreQuery() {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(nestedQueryBuilder);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        constantScoreQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaBoostingQuery() {
        InnerHitBuilder leafInnerHits1 = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits1);
        InnerHitBuilder leafInnerHits2 = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits2);
        BoostingQueryBuilder constantScoreQueryBuilder = new BoostingQueryBuilder(nestedQueryBuilder1, nestedQueryBuilder2);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        constantScoreQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits1.getName()), notNullValue());
        assertThat(innerHitBuilders.get(leafInnerHits2.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaFunctionScoreQuery() {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits);
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(nestedQueryBuilder);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        ((AbstractQueryBuilder<?>) functionScoreQueryBuilder).extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public static InnerHitBuilder randomInnerHits() {
        return randomInnerHits(true, true);
    }

    public static InnerHitBuilder randomInnerHits(boolean recursive, boolean includeQueryTypeOrPath) {
        InnerHitBuilder innerHits = new InnerHitBuilder();
        innerHits.setName(randomAsciiOfLengthBetween(1, 16));
        innerHits.setFrom(randomIntBetween(0, 128));
        innerHits.setSize(randomIntBetween(0, 128));
        innerHits.setExplain(randomBoolean());
        innerHits.setVersion(randomBoolean());
        innerHits.setTrackScores(randomBoolean());
        innerHits.setFieldNames(randomListStuff(16, () -> randomAsciiOfLengthBetween(1, 16)));
        innerHits.setFieldDataFields(randomListStuff(16, () -> randomAsciiOfLengthBetween(1, 16)));
        // Random script fields deduped on their field name.
        Map<String, SearchSourceBuilder.ScriptField> scriptFields = new HashMap<>();
        for (SearchSourceBuilder.ScriptField field: randomListStuff(16, InnerHitBuilderTests::randomScript)) {
            scriptFields.put(field.fieldName(), field);
        }
        innerHits.setScriptFields(new HashSet<>(scriptFields.values()));
        FetchSourceContext randomFetchSourceContext;
        if (randomBoolean()) {
            randomFetchSourceContext = new FetchSourceContext(randomBoolean());
        } else {
            randomFetchSourceContext = new FetchSourceContext(
                    generateRandomStringArray(12, 16, false),
                    generateRandomStringArray(12, 16, false)
            );
        }
        innerHits.setFetchSourceContext(randomFetchSourceContext);
        if (randomBoolean()) {
            innerHits.setSorts(randomListStuff(16,
                    () -> SortBuilders.fieldSort(randomAsciiOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())))
            );
        }
        innerHits.setHighlightBuilder(HighlightBuilderTests.randomHighlighterBuilder());
        if (recursive && randomBoolean()) {
            int size = randomIntBetween(1, 16);
            for (int i = 0; i < size; i++) {
                innerHits.addChildInnerHit(randomInnerHits(false, includeQueryTypeOrPath));
            }
        }

        if (includeQueryTypeOrPath) {
            QueryBuilder query = new MatchQueryBuilder(randomAsciiOfLengthBetween(1, 16), randomAsciiOfLengthBetween(1, 16));
            if (randomBoolean()) {
                return new InnerHitBuilder(innerHits, randomAsciiOfLength(8), query);
            } else {
                return new InnerHitBuilder(innerHits, query, randomAsciiOfLength(8));
            }
        } else {
            return innerHits;
        }
    }

    public void testCopyConstructor() throws Exception {
        InnerHitBuilder original = randomInnerHits();
        InnerHitBuilder copy = original.getNestedPath() != null ?
                new InnerHitBuilder(original, original.getNestedPath(), original.getQuery()) :
                new InnerHitBuilder(original, original.getQuery(), original.getParentChildType());
        assertThat(copy, equalTo(original));
        copy = mutate(copy);
        assertThat(copy, not(equalTo(original)));
    }

    static InnerHitBuilder mutate(InnerHitBuilder instance) throws IOException {
        int surprise = randomIntBetween(0, 11);
        switch (surprise) {
            case 0:
                instance.setFrom(randomValueOtherThan(instance.getFrom(), () -> randomIntBetween(0, 128)));
                break;
            case 1:
                instance.setSize(randomValueOtherThan(instance.getSize(), () -> randomIntBetween(0, 128)));
                break;
            case 2:
                instance.setExplain(!instance.isExplain());
                break;
            case 3:
                instance.setVersion(!instance.isVersion());
                break;
            case 4:
                instance.setTrackScores(!instance.isTrackScores());
                break;
            case 5:
                instance.setName(randomValueOtherThan(instance.getName(), () -> randomAsciiOfLengthBetween(1, 16)));
                break;
            case 6:
                if (randomBoolean()) {
                    instance.setFieldDataFields(randomValueOtherThan(instance.getFieldDataFields(), () -> {
                        return randomListStuff(16, () -> randomAsciiOfLengthBetween(1, 16));
                    }));
                } else {
                    instance.addFieldDataField(randomAsciiOfLengthBetween(1, 16));
                }
                break;
            case 7:
                if (randomBoolean()) {
                    instance.setScriptFields(randomValueOtherThan(instance.getScriptFields(), () -> {
                        return new HashSet<>(randomListStuff(16, InnerHitBuilderTests::randomScript));}));
                } else {
                    SearchSourceBuilder.ScriptField script = randomScript();
                    instance.addScriptField(script.fieldName(), script.script());
                }
                break;
            case 8:
                instance.setFetchSourceContext(randomValueOtherThan(instance.getFetchSourceContext(), () -> {
                    FetchSourceContext randomFetchSourceContext;
                    if (randomBoolean()) {
                        randomFetchSourceContext = new FetchSourceContext(randomBoolean());
                    } else {
                        randomFetchSourceContext = new FetchSourceContext(
                                generateRandomStringArray(12, 16, false),
                                generateRandomStringArray(12, 16, false)
                        );
                    }
                    return randomFetchSourceContext;
                }));
                break;
            case 9:
                if (randomBoolean()) {
                    final List<SortBuilder<?>> sortBuilders = randomValueOtherThan(instance.getSorts(), () -> {
                        List<SortBuilder<?>> builders = randomListStuff(16,
                                () -> SortBuilders.fieldSort(randomAsciiOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
                        return builders;
                    });
                    instance.setSorts(sortBuilders);
                } else {
                    instance.addSort(SortBuilders.fieldSort(randomAsciiOfLengthBetween(5, 20)));
                }
                break;
            case 10:
                instance.setHighlightBuilder(randomValueOtherThan(instance.getHighlightBuilder(),
                        HighlightBuilderTests::randomHighlighterBuilder));
                break;
            case 11:
                if (instance.getFieldNames() == null || randomBoolean()) {
                    instance.setFieldNames(randomValueOtherThan(instance.getFieldNames(), () -> {
                        return randomListStuff(16, () -> randomAsciiOfLengthBetween(1, 16));
                    }));
                } else {
                    instance.getFieldNames().add(randomAsciiOfLengthBetween(1, 16));
                }
                break;
            default:
                throw new IllegalStateException("unexpected surprise [" + surprise + "]");
        }
        return instance;
    }

    static SearchSourceBuilder.ScriptField randomScript() {
        ScriptService.ScriptType randomScriptType = randomFrom(ScriptService.ScriptType.values());
        Map<String, Object> randomMap = null;
        if (randomBoolean()) {
            randomMap = new HashMap<>();
            int numEntries = randomIntBetween(0, 32);
            for (int i = 0; i < numEntries; i++) {
                randomMap.put(String.valueOf(i), randomAsciiOfLength(16));
            }
        }
        Script script = new Script(randomAsciiOfLength(128), randomScriptType, randomAsciiOfLengthBetween(1, 4),randomMap);
        return new SearchSourceBuilder.ScriptField(randomAsciiOfLengthBetween(1, 32), script, randomBoolean());
    }

    static <T> List<T> randomListStuff(int maxSize, Supplier<T> valueSupplier) {
        int size = randomIntBetween(0, maxSize);
        List<T> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(valueSupplier.get());
        }
        return list;
    }

    private static InnerHitBuilder serializedCopy(InnerHitBuilder original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                return new InnerHitBuilder(in);
            }
        }
    }

}
