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
package org.elasticsearch.index.query.support;

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
import org.elasticsearch.index.query.AbstractQueryTestCase;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightBuilderTests;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

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
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            InnerHitBuilder innerHit = randomInnerHits();
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            innerHit.toXContent(builder, ToXContent.EMPTY_PARAMS);

            XContentParser parser = XContentHelper.createParser(builder.bytes());
            context.reset(parser);
            InnerHitBuilder secondInnerHits = InnerHitBuilder.fromXContent(parser, context);
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
            assertThat("different inner hits should not be equal", mutate(firstInnerHit), not(equalTo(firstInnerHit)));

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

    public static InnerHitBuilder randomInnerHits() {
        return randomInnerHits(true);
    }

    public static InnerHitBuilder randomInnerHits(boolean recursive) {
        InnerHitBuilder innerHits = new InnerHitBuilder();
        if (randomBoolean()) {
            innerHits.setNestedPath(randomAsciiOfLengthBetween(1, 16));
        } else {
            innerHits.setParentChildType(randomAsciiOfLengthBetween(1, 16));
        }

        innerHits.setName(randomAsciiOfLengthBetween(1, 16));
        innerHits.setFrom(randomIntBetween(0, 128));
        innerHits.setSize(randomIntBetween(0, 128));
        innerHits.setExplain(randomBoolean());
        innerHits.setVersion(randomBoolean());
        innerHits.setTrackScores(randomBoolean());
        innerHits.setFieldNames(randomListStuff(16, () -> randomAsciiOfLengthBetween(1, 16)));
        innerHits.setFieldDataFields(randomListStuff(16, () -> randomAsciiOfLengthBetween(1, 16)));
        innerHits.setScriptFields(randomListStuff(16, InnerHitBuilderTests::randomScript));
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
        if (randomBoolean()) {
            innerHits.setQuery(new MatchQueryBuilder(randomAsciiOfLengthBetween(1, 16), randomAsciiOfLengthBetween(1, 16)));
        }
        if (recursive && randomBoolean()) {
            InnerHitsBuilder innerHitsBuilder = new InnerHitsBuilder();
            int size = randomIntBetween(1, 16);
            for (int i = 0; i < size; i++) {
                innerHitsBuilder.addInnerHit(randomAsciiOfLengthBetween(1, 16), randomInnerHits(false));
            }
            innerHits.setInnerHitsBuilder(innerHitsBuilder);
        }

        return innerHits;
    }

    static InnerHitBuilder mutate(InnerHitBuilder innerHits) throws IOException {
        InnerHitBuilder copy = serializedCopy(innerHits);
        int surprise = randomIntBetween(0, 10);
        switch (surprise) {
            case 0:
                copy.setFrom(randomValueOtherThan(innerHits.getFrom(), () -> randomIntBetween(0, 128)));
                break;
            case 1:
                copy.setSize(randomValueOtherThan(innerHits.getSize(), () -> randomIntBetween(0, 128)));
                break;
            case 2:
                copy.setExplain(!copy.isExplain());
                break;
            case 3:
                copy.setVersion(!copy.isVersion());
                break;
            case 4:
                copy.setTrackScores(!copy.isTrackScores());
                break;
            case 5:
                copy.setName(randomValueOtherThan(innerHits.getName(), () -> randomAsciiOfLengthBetween(1, 16)));
                break;
            case 6:
                copy.setFieldDataFields(randomValueOtherThan(copy.getFieldDataFields(), () -> {
                    return randomListStuff(16, () -> randomAsciiOfLengthBetween(1, 16));
                }));
                break;
            case 7:
                copy.setScriptFields(randomValueOtherThan(copy.getScriptFields(), () -> {
                    return randomListStuff(16, InnerHitBuilderTests::randomScript);}));
                break;
            case 8:
                copy.setFetchSourceContext(randomValueOtherThan(copy.getFetchSourceContext(), () -> {
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
                final List<SortBuilder<?>> sortBuilders = randomValueOtherThan(copy.getSorts(), () -> {
                    List<SortBuilder<?>> builders = randomListStuff(16,
                        () -> SortBuilders.fieldSort(randomAsciiOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
                    return builders;
                });
                copy.setSorts(sortBuilders);
                break;
            case 10:
                copy.setHighlightBuilder(randomValueOtherThan(copy.getHighlightBuilder(),
                        HighlightBuilderTests::randomHighlighterBuilder));
                break;
            default:
                throw new IllegalStateException("unexpected surprise [" + surprise + "]");
        }
        return copy;
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
