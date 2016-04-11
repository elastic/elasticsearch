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

public class InnerHitsBuilderTests extends ESTestCase {

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
            InnerHitsBuilder original = randomInnerHits();
            InnerHitsBuilder deserialized = serializedCopy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
        }
    }

    public void testFromAndToXContent() throws Exception {
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry);
        context.parseFieldMatcher(new ParseFieldMatcher(Settings.EMPTY));
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            InnerHitsBuilder innerHits = randomInnerHits();
            XContentBuilder builder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            innerHits.toXContent(builder, ToXContent.EMPTY_PARAMS);

            XContentParser parser = XContentHelper.createParser(builder.bytes());
            context.reset(parser);
            parser.nextToken();
            InnerHitsBuilder secondInnerHits = InnerHitsBuilder.fromXContent(parser, context);
            assertThat(innerHits, not(sameInstance(secondInnerHits)));
            assertThat(innerHits, equalTo(secondInnerHits));
            assertThat(innerHits.hashCode(), equalTo(secondInnerHits.hashCode()));
        }
    }

    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            InnerHitsBuilder firstInnerHits = randomInnerHits();
            assertFalse("inner hit is equal to null", firstInnerHits.equals(null));
            assertFalse("inner hit is equal to incompatible type", firstInnerHits.equals(""));
            assertTrue("inner it is not equal to self", firstInnerHits.equals(firstInnerHits));
            assertThat("same inner hit's hashcode returns different values if called multiple times", firstInnerHits.hashCode(),
                    equalTo(firstInnerHits.hashCode()));

            InnerHitsBuilder secondBuilder = serializedCopy(firstInnerHits);
            assertTrue("inner hit is not equal to self", secondBuilder.equals(secondBuilder));
            assertTrue("inner hit is not equal to its copy", firstInnerHits.equals(secondBuilder));
            assertTrue("equals is not symmetric", secondBuilder.equals(firstInnerHits));
            assertThat("inner hits copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(firstInnerHits.hashCode()));

            InnerHitsBuilder thirdBuilder = serializedCopy(secondBuilder);
            assertTrue("inner hit is not equal to self", thirdBuilder.equals(thirdBuilder));
            assertTrue("inner hit is not equal to its copy", secondBuilder.equals(thirdBuilder));
            assertThat("inner hit copy's hashcode is different from original hashcode", secondBuilder.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not transitive", firstInnerHits.equals(thirdBuilder));
            assertThat("inner hit copy's hashcode is different from original hashcode", firstInnerHits.hashCode(),
                    equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not symmetric", thirdBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", thirdBuilder.equals(firstInnerHits));
        }
    }

    public static InnerHitsBuilder randomInnerHits() {
        InnerHitsBuilder innerHits = new InnerHitsBuilder();
        int numInnerHits = randomIntBetween(0, 12);
        for (int i = 0; i < numInnerHits; i++) {
            innerHits.addInnerHit(randomAsciiOfLength(5), InnerHitBuilderTests.randomInnerHits());
        }
        return innerHits;
    }

    private static InnerHitsBuilder serializedCopy(InnerHitsBuilder original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                return InnerHitsBuilder.PROTO.readFrom(in);
            }
        }
    }

}
