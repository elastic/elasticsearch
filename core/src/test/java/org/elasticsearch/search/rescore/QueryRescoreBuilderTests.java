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

package org.elasticsearch.search.rescore;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.rescore.RescoreBuilder.QueryRescorer;
import org.elasticsearch.search.rescore.RescoreBuilder.Rescorer;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class QueryRescoreBuilderTests extends ESTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        namedWriteableRegistry = new NamedWriteableRegistry();
        namedWriteableRegistry.registerPrototype(Rescorer.class, org.elasticsearch.search.rescore.RescoreBuilder.QueryRescorer.PROTOTYPE);
        namedWriteableRegistry.registerPrototype(QueryBuilder.class, new MatchAllQueryBuilder());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
    }

    /**
     * Test serialization and deserialization of the rescore builder
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            RescoreBuilder original = randomRescoreBuilder();
            RescoreBuilder deserialized = serializedCopy(original);
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
            RescoreBuilder firstBuilder = randomRescoreBuilder();
            assertFalse("rescore builder is equal to null", firstBuilder.equals(null));
            assertFalse("rescore builder is equal to incompatible type", firstBuilder.equals(""));
            assertTrue("rescore builder is not equal to self", firstBuilder.equals(firstBuilder));
            assertThat("same rescore builder's hashcode returns different values if called multiple times", firstBuilder.hashCode(),
                    equalTo(firstBuilder.hashCode()));
            assertThat("different rescore builder should not be equal", mutate(firstBuilder), not(equalTo(firstBuilder)));

            RescoreBuilder secondBuilder = serializedCopy(firstBuilder);
            assertTrue("rescore builder is not equal to self", secondBuilder.equals(secondBuilder));
            assertTrue("rescore builder is not equal to its copy", firstBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", secondBuilder.equals(firstBuilder));
            assertThat("rescore builder copy's hashcode is different from original hashcode", secondBuilder.hashCode(), equalTo(firstBuilder.hashCode()));

            RescoreBuilder thirdBuilder = serializedCopy(secondBuilder);
            assertTrue("rescore builder is not equal to self", thirdBuilder.equals(thirdBuilder));
            assertTrue("rescore builder is not equal to its copy", secondBuilder.equals(thirdBuilder));
            assertThat("rescore builder copy's hashcode is different from original hashcode", secondBuilder.hashCode(), equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not transitive", firstBuilder.equals(thirdBuilder));
            assertThat("rescore builder copy's hashcode is different from original hashcode", firstBuilder.hashCode(), equalTo(thirdBuilder.hashCode()));
            assertTrue("equals is not symmetric", thirdBuilder.equals(secondBuilder));
            assertTrue("equals is not symmetric", thirdBuilder.equals(firstBuilder));
        }
    }

    private RescoreBuilder mutate(RescoreBuilder original) throws IOException {
        RescoreBuilder mutation = serializedCopy(original);
        if (randomBoolean()) {
            Integer windowSize = original.windowSize();
            if (windowSize != null) {
                mutation.windowSize(windowSize + 1);
            } else {
                mutation.windowSize(randomIntBetween(0, 100));
            }
        } else {
            QueryRescorer queryRescorer = (QueryRescorer) mutation.rescorer();
            switch (randomIntBetween(0, 3)) {
            case 0:
                queryRescorer.setQueryWeight(queryRescorer.getQueryWeight() + 0.1f);
                break;
            case 1:
                queryRescorer.setRescoreQueryWeight(queryRescorer.getRescoreQueryWeight() + 0.1f);
                break;
            case 2:
                QueryRescoreMode other;
                do {
                    other = randomFrom(QueryRescoreMode.values());
                } while (other == queryRescorer.getScoreMode());
                queryRescorer.setScoreMode(other);
                break;
            case 3:
                // only increase the boost to make it a slightly different query
                queryRescorer.getRescoreQuery().boost(queryRescorer.getRescoreQuery().boost() + 0.1f);
                break;
            default:
                throw new IllegalStateException("unexpected random mutation in test");
            }
        }
        return mutation;
    }

    /**
     * create random shape that is put under test
     */
    private static RescoreBuilder randomRescoreBuilder() {
        QueryBuilder<MatchAllQueryBuilder> queryBuilder = new MatchAllQueryBuilder().boost(randomFloat()).queryName(randomAsciiOfLength(20));
        org.elasticsearch.search.rescore.RescoreBuilder.QueryRescorer rescorer = new
                org.elasticsearch.search.rescore.RescoreBuilder.QueryRescorer(queryBuilder);
        if (randomBoolean()) {
            rescorer.setQueryWeight(randomFloat());
        }
        if (randomBoolean()) {
            rescorer.setRescoreQueryWeight(randomFloat());
        }
        if (randomBoolean()) {
            rescorer.setScoreMode(randomFrom(QueryRescoreMode.values()));
        }
        RescoreBuilder builder = new RescoreBuilder(rescorer);
        if (randomBoolean()) {
            builder.windowSize(randomIntBetween(0, 100));
        }
        return builder;
    }

    private static RescoreBuilder serializedCopy(RescoreBuilder original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(output.bytes()), namedWriteableRegistry)) {
                return RescoreBuilder.PROTOYPE.readFrom(in);
            }
        }
    }

}
