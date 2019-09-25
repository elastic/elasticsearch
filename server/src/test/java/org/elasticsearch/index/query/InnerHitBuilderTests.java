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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilderTests;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import static org.hamcrest.Matchers.sameInstance;

public class InnerHitBuilderTests extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;
    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
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
     * {@link ShardSearchRequest} (via
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
            InnerHitBuilder innerHit = randomInnerHits();
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            innerHit.toXContent(builder, ToXContent.EMPTY_PARAMS);
            //fields is printed out as an object but parsed into a List where order matters, we disable shuffling
            XContentBuilder shuffled = shuffleXContent(builder, "fields");
            try (XContentParser parser = createParser(shuffled)) {
                InnerHitBuilder secondInnerHits = InnerHitBuilder.fromXContent(parser);
                assertThat(innerHit, not(sameInstance(secondInnerHits)));
                assertThat(innerHit, equalTo(secondInnerHits));
                assertThat(innerHit.hashCode(), equalTo(secondInnerHits.hashCode()));
            }
        }
    }

    public void testEqualsAndHashcode() {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            checkEqualsAndHashCode(randomInnerHits(), InnerHitBuilderTests::serializedCopy, InnerHitBuilderTests::mutate);
        }
    }

    public static InnerHitBuilder randomNestedInnerHits() {
        InnerHitBuilder innerHitBuilder = randomInnerHits();
        innerHitBuilder.setSeqNoAndPrimaryTerm(false); // not supported by nested queries
        return innerHitBuilder;
    }
    public static InnerHitBuilder randomInnerHits() {
        InnerHitBuilder innerHits = new InnerHitBuilder();
        innerHits.setName(randomAlphaOfLengthBetween(5, 16));
        innerHits.setFrom(randomIntBetween(0, 32));
        innerHits.setSize(randomIntBetween(0, 32));
        innerHits.setExplain(randomBoolean());
        innerHits.setVersion(randomBoolean());
        innerHits.setSeqNoAndPrimaryTerm(randomBoolean());
        innerHits.setTrackScores(randomBoolean());
        if (randomBoolean()) {
            innerHits.setStoredFieldNames(randomListStuff(16, () -> randomAlphaOfLengthBetween(1, 16)));
        }
        innerHits.setDocValueFields(randomListStuff(16,
                () -> new FieldAndFormat(randomAlphaOfLengthBetween(1, 16), null)));
        // Random script fields deduped on their field name.
        Map<String, SearchSourceBuilder.ScriptField> scriptFields = new HashMap<>();
        for (SearchSourceBuilder.ScriptField field: randomListStuff(16, InnerHitBuilderTests::randomScript)) {
            scriptFields.put(field.fieldName(), field);
        }
        innerHits.setScriptFields(new HashSet<>(scriptFields.values()));
        FetchSourceContext randomFetchSourceContext;
        int randomInt = randomIntBetween(0, 2);
        if (randomInt == 0) {
            randomFetchSourceContext = new FetchSourceContext(true, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
        } else if (randomInt == 1) {
            randomFetchSourceContext = new FetchSourceContext(true,
                    generateRandomStringArray(12, 16, false),
                    generateRandomStringArray(12, 16, false)
            );
        } else {
            randomFetchSourceContext = new FetchSourceContext(randomBoolean());
        }
        innerHits.setFetchSourceContext(randomFetchSourceContext);
        if (randomBoolean()) {
            innerHits.setSorts(randomListStuff(16,
                    () -> SortBuilders.fieldSort(randomAlphaOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())))
            );
        }
        innerHits.setHighlightBuilder(HighlightBuilderTests.randomHighlighterBuilder());
        return innerHits;
    }

    static InnerHitBuilder mutate(InnerHitBuilder original) throws IOException {
        final InnerHitBuilder copy = serializedCopy(original);
        List<Runnable> modifiers = new ArrayList<>(12);
        modifiers.add(() -> copy.setFrom(randomValueOtherThan(copy.getFrom(), () -> randomIntBetween(0, 128))));
        modifiers.add(() -> copy.setSize(randomValueOtherThan(copy.getSize(), () -> randomIntBetween(0, 128))));
        modifiers.add(() -> copy.setExplain(!copy.isExplain()));
        modifiers.add(() -> copy.setVersion(!copy.isVersion()));
        modifiers.add(() -> copy.setSeqNoAndPrimaryTerm(!copy.isSeqNoAndPrimaryTerm()));
        modifiers.add(() -> copy.setTrackScores(!copy.isTrackScores()));
        modifiers.add(() -> copy.setName(randomValueOtherThan(copy.getName(), () -> randomAlphaOfLengthBetween(1, 16))));
        modifiers.add(() -> {
            if (randomBoolean()) {
                copy.setDocValueFields(randomValueOtherThan(copy.getDocValueFields(),
                        () -> randomListStuff(16, () -> new FieldAndFormat(randomAlphaOfLengthBetween(1, 16), null))));
            } else {
                copy.addDocValueField(randomAlphaOfLengthBetween(1, 16));
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
                                () -> SortBuilders.fieldSort(randomAlphaOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
                        return builders;
                    });
                    copy.setSorts(sortBuilders);
                } else {
                    copy.addSort(SortBuilders.fieldSort(randomAlphaOfLengthBetween(5, 20)));
                }
        });
        modifiers.add(() -> copy
                .setHighlightBuilder(randomValueOtherThan(copy.getHighlightBuilder(), HighlightBuilderTests::randomHighlighterBuilder)));
        modifiers.add(() -> {
                if (copy.getStoredFieldsContext() == null || randomBoolean()) {
                    List<String> previous = copy.getStoredFieldsContext() == null ?
                        Collections.emptyList() : copy.getStoredFieldsContext().fieldNames();
                    List<String> newValues = randomValueOtherThan(previous,
                            () -> randomListStuff(1, 16, () -> randomAlphaOfLengthBetween(1, 16)));
                    copy.setStoredFieldNames(newValues);
                } else {
                    copy.getStoredFieldsContext().addFieldName(randomAlphaOfLengthBetween(1, 16));
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
                randomMap.put(String.valueOf(i), randomAlphaOfLength(16));
            }
        }
        Script script = new Script(randomScriptType, randomScriptType == ScriptType.STORED ? null : randomAlphaOfLengthBetween(1, 4),
            randomAlphaOfLength(128), randomMap);
        return new SearchSourceBuilder.ScriptField(randomAlphaOfLengthBetween(1, 32), script, randomBoolean());
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

    public void testSetDocValueFormat() {
        InnerHitBuilder innerHit = new InnerHitBuilder();
        innerHit.addDocValueField("foo");
        innerHit.addDocValueField("@timestamp", "epoch_millis");
        assertEquals(
                Arrays.asList(new FieldAndFormat("foo", null), new FieldAndFormat("@timestamp", "epoch_millis")),
                innerHit.getDocValueFields());
    }
}
