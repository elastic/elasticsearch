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

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilderTests;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InnerHitBuilderTests extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;
    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
        xContentRegistry = null;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
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

    /**
     * Test that if we serialize and deserialize an object, further
     * serialization leads to identical bytes representation.
     *
     * This is necessary to ensure because we use the serialized BytesReference
     * of this builder as part of the cacheKey in
     * {@link ShardSearchLocalRequest} (via
     * {@link SearchSourceBuilder#collapse(org.elasticsearch.search.collapse.CollapseBuilder)})
     */
    public void testSerializationOrder() throws Exception {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            InnerHitBuilder original = randomInnerHits();
            InnerHitBuilder deserialized = serializedCopy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
            BytesStreamOutput out1 = new BytesStreamOutput();
            BytesStreamOutput out2 = new BytesStreamOutput();
            original.writeTo(out1);
            deserialized.writeTo(out2);
            assertEquals(out1.bytes(), out2.bytes());
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

            XContentParser parser = createParser(shuffled);
            QueryParseContext context = new QueryParseContext(parser);
            InnerHitBuilder secondInnerHits = InnerHitBuilder.fromXContent(context);
            assertThat(innerHit, not(sameInstance(secondInnerHits)));
            assertThat(innerHit, equalTo(secondInnerHits));
            assertThat(innerHit.hashCode(), equalTo(secondInnerHits.hashCode()));
        }
    }

    public void testEqualsAndHashcode() {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            checkEqualsAndHashCode(randomInnerHits(), InnerHitBuilderTests::serializedCopy, InnerHitBuilderTests::mutate);
        }
    }

    public void testInlineLeafInnerHitsNestedQuery() throws Exception {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        nestedQueryBuilder.innerHit(leafInnerHits, false);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        nestedQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsHasChildQuery() throws Exception {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder("type", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits, false);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        hasChildQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsHasParentQuery() throws Exception {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder("type", new MatchAllQueryBuilder(), false)
                .innerHit(leafInnerHits, false);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        hasParentQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaBoolQuery() {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits, false);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().should(nestedQueryBuilder);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        boolQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaConstantScoreQuery() {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits, false);
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(nestedQueryBuilder);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        constantScoreQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaBoostingQuery() {
        InnerHitBuilder leafInnerHits1 = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits1, false);
        InnerHitBuilder leafInnerHits2 = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits2, false);
        BoostingQueryBuilder constantScoreQueryBuilder = new BoostingQueryBuilder(nestedQueryBuilder1, nestedQueryBuilder2);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        constantScoreQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits1.getName()), notNullValue());
        assertThat(innerHitBuilders.get(leafInnerHits2.getName()), notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaFunctionScoreQuery() {
        InnerHitBuilder leafInnerHits = randomInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits, false);
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(nestedQueryBuilder);
        Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
        ((AbstractQueryBuilder<?>) functionScoreQueryBuilder).extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), notNullValue());
    }

    public void testBuild_ingoreUnmappedNestQuery() throws Exception {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.getObjectMapper("path")).thenReturn(null);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);

        InnerHitBuilder leafInnerHits = randomInnerHits();
        NestedQueryBuilder query1 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        query1.innerHit(leafInnerHits, false);
        expectThrows(IllegalStateException.class, () -> query1.innerHit().build(searchContext, new InnerHitsContext()));

        NestedQueryBuilder query2 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        query2.innerHit(leafInnerHits, true);
        InnerHitsContext innerHitsContext = new InnerHitsContext();
        query2.innerHit().build(searchContext, innerHitsContext);
        assertThat(innerHitsContext.getInnerHits().size(), equalTo(0));
    }

    public void testBuild_ignoreUnmappedHasChildQuery() throws Exception {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.documentMapper("type")).thenReturn(null);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);

        InnerHitBuilder leafInnerHits = randomInnerHits();
        HasChildQueryBuilder query1 = new HasChildQueryBuilder("type", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits, false);
        expectThrows(IllegalStateException.class, () -> query1.innerHit().build(searchContext, new InnerHitsContext()));

        HasChildQueryBuilder query2 = new HasChildQueryBuilder("type", new MatchAllQueryBuilder(), ScoreMode.None)
                .innerHit(leafInnerHits, true);
        InnerHitsContext innerHitsContext = new InnerHitsContext();
        query2.innerHit().build(searchContext, innerHitsContext);
        assertThat(innerHitsContext.getInnerHits().size(), equalTo(0));
    }

    public void testBuild_ingoreUnmappedHasParentQuery() throws Exception {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.documentMapper("type")).thenReturn(null);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);

        InnerHitBuilder leafInnerHits = randomInnerHits();
        HasParentQueryBuilder query1 = new HasParentQueryBuilder("type", new MatchAllQueryBuilder(), false)
                .innerHit(leafInnerHits, false);
        expectThrows(IllegalStateException.class, () -> query1.innerHit().build(searchContext, new InnerHitsContext()));

        HasParentQueryBuilder query2 = new HasParentQueryBuilder("type", new MatchAllQueryBuilder(), false)
                .innerHit(leafInnerHits, true);
        InnerHitsContext innerHitsContext = new InnerHitsContext();
        query2.innerHit().build(searchContext, innerHitsContext);
        assertThat(innerHitsContext.getInnerHits().size(), equalTo(0));
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
        if (randomBoolean()) {
            innerHits.setStoredFieldNames(randomListStuff(16, () -> randomAsciiOfLengthBetween(1, 16)));
        }
        innerHits.setDocValueFields(randomListStuff(16, () -> randomAsciiOfLengthBetween(1, 16)));
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
            randomFetchSourceContext = new FetchSourceContext(true,
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
                return new InnerHitBuilder(innerHits, randomAsciiOfLength(8), query, randomBoolean());
            } else {
                return new InnerHitBuilder(innerHits, query, randomAsciiOfLength(8), randomBoolean());
            }
        } else {
            return innerHits;
        }
    }

    public void testCopyConstructor() throws Exception {
        InnerHitBuilder original = randomInnerHits();
        InnerHitBuilder copy = original.getNestedPath() != null ?
                new InnerHitBuilder(original, original.getNestedPath(), original.getQuery(), original.isIgnoreUnmapped()) :
                new InnerHitBuilder(original, original.getQuery(), original.getParentChildType(), original.isIgnoreUnmapped());
        assertThat(copy, equalTo(original));
        copy = mutate(copy);
        assertThat(copy, not(equalTo(original)));
    }

    static InnerHitBuilder mutate(InnerHitBuilder original) throws IOException {
        final InnerHitBuilder copy = serializedCopy(original);
        List<Runnable> modifiers = new ArrayList<>(12);
        modifiers.add(() -> copy.setFrom(randomValueOtherThan(copy.getFrom(), () -> randomIntBetween(0, 128))));
        modifiers.add(() -> copy.setSize(randomValueOtherThan(copy.getSize(), () -> randomIntBetween(0, 128))));
        modifiers.add(() -> copy.setExplain(!copy.isExplain()));
        modifiers.add(() -> copy.setVersion(!copy.isVersion()));
        modifiers.add(() -> copy.setTrackScores(!copy.isTrackScores()));
        modifiers.add(() -> copy.setName(randomValueOtherThan(copy.getName(), () -> randomAsciiOfLengthBetween(1, 16))));
        modifiers.add(() -> {
            if (randomBoolean()) {
                copy.setDocValueFields(randomValueOtherThan(copy.getDocValueFields(), () -> {
                    return randomListStuff(16, () -> randomAsciiOfLengthBetween(1, 16));
                }));
            } else {
                copy.addDocValueField(randomAsciiOfLengthBetween(1, 16));
            }
        });
        modifiers.add(() -> {
            if (randomBoolean()) {
                copy.setScriptFields(randomValueOtherThan(copy.getScriptFields(), () -> {
                    return new HashSet<>(randomListStuff(16, InnerHitBuilderTests::randomScript));
                }));
            } else {
                SearchSourceBuilder.ScriptField script = randomScript();
                copy.addScriptField(script.fieldName(), script.script());
            }
        });
        modifiers.add(() -> copy.setFetchSourceContext(randomValueOtherThan(copy.getFetchSourceContext(), () -> {
            FetchSourceContext randomFetchSourceContext;
            if (randomBoolean()) {
                randomFetchSourceContext = new FetchSourceContext(randomBoolean());
            } else {
                randomFetchSourceContext = new FetchSourceContext(true, generateRandomStringArray(12, 16, false),
                        generateRandomStringArray(12, 16, false));
            }
            return randomFetchSourceContext;
        })));
        modifiers.add(() -> {
                if (randomBoolean()) {
                    final List<SortBuilder<?>> sortBuilders = randomValueOtherThan(copy.getSorts(), () -> {
                        List<SortBuilder<?>> builders = randomListStuff(16,
                                () -> SortBuilders.fieldSort(randomAsciiOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
                        return builders;
                    });
                    copy.setSorts(sortBuilders);
                } else {
                    copy.addSort(SortBuilders.fieldSort(randomAsciiOfLengthBetween(5, 20)));
                }
        });
        modifiers.add(() -> copy
                .setHighlightBuilder(randomValueOtherThan(copy.getHighlightBuilder(), HighlightBuilderTests::randomHighlighterBuilder)));
        modifiers.add(() -> {
                if (copy.getStoredFieldsContext() == null || randomBoolean()) {
                    List<String> previous = copy.getStoredFieldsContext() == null ?
                        Collections.emptyList() : copy.getStoredFieldsContext().fieldNames();
                    List<String> newValues = randomValueOtherThan(previous,
                            () -> randomListStuff(1, 16, () -> randomAsciiOfLengthBetween(1, 16)));
                    copy.setStoredFieldNames(newValues);
                } else {
                    copy.getStoredFieldsContext().addFieldName(randomAsciiOfLengthBetween(1, 16));
                }
        });
        randomFrom(modifiers).run();
        return copy;
    }

    static SearchSourceBuilder.ScriptField randomScript() {
        ScriptType randomScriptType = randomFrom(ScriptType.values());
        Map<String, Object> randomMap = new HashMap<>();
        if (randomBoolean()) {
            int numEntries = randomIntBetween(0, 32);
            for (int i = 0; i < numEntries; i++) {
                randomMap.put(String.valueOf(i), randomAsciiOfLength(16));
            }
        }
        Script script = new Script(randomScriptType, randomScriptType == ScriptType.STORED ? null : randomAsciiOfLengthBetween(1, 4),
            randomAsciiOfLength(128), randomMap);
        return new SearchSourceBuilder.ScriptField(randomAsciiOfLengthBetween(1, 32), script, randomBoolean());
    }

    static <T> List<T> randomListStuff(int maxSize, Supplier<T> valueSupplier) {
        return randomListStuff(0, maxSize, valueSupplier);
    }

    static <T> List<T> randomListStuff(int minSize, int maxSize, Supplier<T> valueSupplier) {
        int size = randomIntBetween(minSize, maxSize);
        List<T> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(valueSupplier.get());
        }
        return list;
    }

    private static InnerHitBuilder serializedCopy(InnerHitBuilder original) throws IOException {
        return ESTestCase.copyWriteable(original, namedWriteableRegistry, InnerHitBuilder::new);
    }

}
