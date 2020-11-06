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

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction.AnalyzeToken;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo.Failure;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.IndexShardRecoveringException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.carrotsearch.randomizedtesting.generators.RandomNumbers.randomIntBetween;
import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomAsciiLettersOfLength;
import static com.carrotsearch.randomizedtesting.generators.RandomStrings.randomUnicodeOfLengthBetween;
import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.elasticsearch.test.ESTestCase.randomFrom;

public final class RandomObjects {

    private RandomObjects() {

    }

    /**
     * Returns a tuple containing random stored field values and their corresponding expected values once printed out
     * via {@link org.elasticsearch.common.xcontent.ToXContent#toXContent(XContentBuilder, ToXContent.Params)} and parsed back via
     * {@link org.elasticsearch.common.xcontent.XContentParser#objectText()}.
     * Generates values based on what can get printed out. Stored fields values are retrieved from lucene and converted via
     * {@link org.elasticsearch.index.mapper.MappedFieldType#valueForDisplay(Object)} to either strings, numbers or booleans.
     *
     * @param random Random generator
     * @param xContentType the content type, used to determine what the expected values are for float numbers.
     */
    public static Tuple<List<Object>, List<Object>> randomStoredFieldValues(Random random, XContentType xContentType) {
        int numValues = randomIntBetween(random, 1, 5);
        List<Object> originalValues = randomStoredFieldValues(random, numValues);
        List<Object> expectedParsedValues = new ArrayList<>(numValues);
        for (Object originalValue : originalValues) {
            expectedParsedValues.add(getExpectedParsedValue(xContentType, originalValue));
        }
        return Tuple.tuple(originalValues, expectedParsedValues);
    }

    private static List<Object> randomStoredFieldValues(Random random, int numValues) {
        List<Object> values = new ArrayList<>(numValues);
        int dataType = randomIntBetween(random, 0, 8);
        for (int i = 0; i < numValues; i++) {
            switch(dataType) {
                case 0:
                    values.add(random.nextLong());
                    break;
                case 1:
                    values.add(random.nextInt());
                    break;
                case 2:
                    values.add((short) random.nextInt());
                    break;
                case 3:
                    values.add((byte) random.nextInt());
                    break;
                case 4:
                    values.add(random.nextDouble());
                    break;
                case 5:
                    values.add(random.nextFloat());
                    break;
                case 6:
                    values.add(random.nextBoolean());
                    break;
                case 7:
                    values.add(random.nextBoolean() ? RandomStrings.randomAsciiLettersOfLengthBetween(random, 3, 10) :
                        randomUnicodeOfLengthBetween(random, 3, 10));
                    break;
                case 8:
                    byte[] randomBytes = RandomStrings.randomUnicodeOfLengthBetween(random, 10, 50).getBytes(StandardCharsets.UTF_8);
                    values.add(new BytesArray(randomBytes));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        return values;
    }

    /**
     * Converts the provided field value to its corresponding expected value once printed out
     * via {@link org.elasticsearch.common.xcontent.ToXContent#toXContent(XContentBuilder, ToXContent.Params)} and parsed back via
     * {@link org.elasticsearch.common.xcontent.XContentParser#objectText()}.
     * Generates values based on what can get printed out. Stored fields values are retrieved from lucene and converted via
     * {@link org.elasticsearch.index.mapper.MappedFieldType#valueForDisplay(Object)} to either strings, numbers or booleans.
     */
    public static Object getExpectedParsedValue(XContentType xContentType, Object value) {
        if (value instanceof BytesArray) {
            if (xContentType == XContentType.JSON) {
                //JSON writes base64 format
                return Base64.getEncoder().encodeToString(((BytesArray)value).toBytesRef().bytes);
            }
        }
        if (value instanceof Float) {
            if (xContentType == XContentType.CBOR || xContentType == XContentType.SMILE) {
                // with binary content types we pass back the object as is
                return value;
            }
            //with JSON AND YAML we get back a double, but with float precision.
            return Double.parseDouble(value.toString());
        }
        if (value instanceof Byte) {
            return ((Byte)value).intValue();
        }
        if (value instanceof Short) {
            return ((Short)value).intValue();
        }
        return value;
    }

    /**
     * Returns a random source containing a random number of fields, objects and array, with maximum depth 5.
     *
     * @param random Random generator
     */
    public static BytesReference randomSource(Random random) {
        //the source can be stored in any format and eventually converted when retrieved depending on the format of the response
        return randomSource(random, RandomPicks.randomFrom(random, XContentType.values()));
    }

    /**
     * Returns a random source in a given XContentType containing a random number of fields, objects and array, with maximum depth 5.
     * The minimum number of fields per object is 1.
     *
     * @param random Random generator
     */
    public static BytesReference randomSource(Random random, XContentType xContentType) {
        return randomSource(random, xContentType, 1);
    }

    /**
     * Returns a random source in a given XContentType containing a random number of fields, objects and array, with maximum depth 5.
     * The minimum number of fields per object is provided as an argument.
     *
     * @param random Random generator
     */
    public static BytesReference randomSource(Random random, XContentType xContentType, int minNumFields) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType)) {
            builder.startObject();
            addFields(random, builder, minNumFields, 0);
            builder.endObject();
            return BytesReference.bytes(builder);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Randomly adds fields, objects, or arrays to the provided builder. The maximum depth is 5.
     */
    private static void addFields(Random random, XContentBuilder builder, int minNumFields, int currentDepth) throws IOException {
        int numFields = randomIntBetween(random, minNumFields, 5);
        for (int i = 0; i < numFields; i++) {
            if (currentDepth < 5 && random.nextInt(100) >= 70) {
                if (random.nextBoolean()) {
                    builder.startObject(RandomStrings.randomAsciiLettersOfLengthBetween(random, 6, 10));
                    addFields(random, builder, minNumFields, currentDepth + 1);
                    builder.endObject();
                } else {
                    builder.startArray(RandomStrings.randomAsciiLettersOfLengthBetween(random, 6, 10));
                    int numElements = randomIntBetween(random, 1, 5);
                    boolean object = random.nextBoolean();
                    int dataType = -1;
                    if (object == false) {
                        dataType = randomDataType(random);
                    }
                    for (int j = 0; j < numElements; j++) {
                        if (object) {
                            builder.startObject();
                            addFields(random, builder, minNumFields, 5);
                            builder.endObject();
                        } else {
                            builder.value(randomFieldValue(random, dataType));
                        }
                    }
                    builder.endArray();
                }
            } else {
                builder.field(RandomStrings.randomAsciiLettersOfLengthBetween(random, 6, 10),
                        randomFieldValue(random, randomDataType(random)));
            }
        }
    }

    private static int randomDataType(Random random) {
        return randomIntBetween(random, 0, 3);
    }

    private static Object randomFieldValue(Random random, int dataType) {
        switch(dataType) {
            case 0:
                return RandomStrings.randomAsciiLettersOfLengthBetween(random, 3, 10);
            case 1:
                return RandomStrings.randomAsciiLettersOfLengthBetween(random, 3, 10);
            case 2:
                return random.nextLong();
            case 3:
                return random.nextDouble();
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Returns a tuple that contains a randomized {@link ShardInfo} value (left side) and its corresponding
     * value (right side) after it has been printed out as a {@link ToXContent} and parsed back using a parsing
     * method like {@link ShardInfo#fromXContent(XContentParser)}. The ShardInfo randomly contains shard failures.
     *
     * @param random Random generator
     */
    public static Tuple<ShardInfo, ShardInfo> randomShardInfo(Random random) {
        return randomShardInfo(random, random.nextBoolean());
    }

    /**
     * Returns a tuple that contains a randomized {@link ShardInfo} value (left side) and its corresponding
     * value (right side) after it has been printed out as a {@link ToXContent} and parsed back using a parsing
     * method like {@link ShardInfo#fromXContent(XContentParser)}. A `withShardFailures` parameter indicates if
     * the randomized ShardInfo must or must not contain shard failures.
     *
     * @param random            Random generator
     * @param withShardFailures indicates if the generated ShardInfo must contain shard failures
     */
    public static Tuple<ShardInfo, ShardInfo> randomShardInfo(Random random, boolean withShardFailures) {
        int total = randomIntBetween(random, 1, 10);
        if (withShardFailures == false) {
            return Tuple.tuple(new ShardInfo(total, total), new ShardInfo(total, total));
        }

        int successful = randomIntBetween(random, 1, Math.max(1, (total - 1)));
        int failures = Math.max(1, (total - successful));

        Failure[] actualFailures = new Failure[failures];
        Failure[] expectedFailures = new Failure[failures];

        for (int i = 0; i < failures; i++) {
            Tuple<Failure, Failure> failure = randomShardInfoFailure(random);
            actualFailures[i] = failure.v1();
            expectedFailures[i] = failure.v2();
        }
        return Tuple.tuple(new ShardInfo(total, successful, actualFailures), new ShardInfo(total, successful, expectedFailures));
    }

    /**
     * Returns a tuple that contains a randomized {@link Failure} value (left side) and its corresponding
     * value (right side) after it has been printed out as a {@link ToXContent} and parsed back using a parsing
     * method like {@link ShardInfo.Failure#fromXContent(XContentParser)}.
     *
     * @param random Random generator
     */
    private static Tuple<Failure, Failure> randomShardInfoFailure(Random random) {
        String index = randomAsciiLettersOfLength(random, 5);
        String indexUuid = randomAsciiLettersOfLength(random, 5);
        int shardId = randomIntBetween(random, 1, 10);
        String nodeId = randomAsciiLettersOfLength(random, 5);
        RestStatus status = randomFrom(random, RestStatus.INTERNAL_SERVER_ERROR, RestStatus.FORBIDDEN, RestStatus.NOT_FOUND);
        boolean primary = random.nextBoolean();
        ShardId shard = new ShardId(index, indexUuid, shardId);

        Exception actualException;
        ElasticsearchException expectedException;

        int type = randomIntBetween(random, 0, 3);
        switch (type) {
            case 0:
                actualException = new ClusterBlockException(singleton(NoMasterBlockService.NO_MASTER_BLOCK_WRITES));
                expectedException = new ElasticsearchException("Elasticsearch exception [type=cluster_block_exception, " +
                        "reason=blocked by: [SERVICE_UNAVAILABLE/2/no master];]");
                break;
            case 1:
                actualException = new ShardNotFoundException(shard);
                expectedException = new ElasticsearchException("Elasticsearch exception [type=shard_not_found_exception, " +
                        "reason=no such shard]");
                expectedException.setShard(shard);
                break;
            case 2:
                actualException = new IllegalArgumentException("Closed resource", new RuntimeException("Resource"));
                expectedException = new ElasticsearchException("Elasticsearch exception [type=illegal_argument_exception, " +
                        "reason=Closed resource]",
                        new ElasticsearchException("Elasticsearch exception [type=runtime_exception, reason=Resource]"));
                break;
            case 3:
                actualException = new IndexShardRecoveringException(shard);
                expectedException = new ElasticsearchException("Elasticsearch exception [type=index_shard_recovering_exception, " +
                        "reason=CurrentState[RECOVERING] Already recovering]");
                expectedException.setShard(shard);
                break;
            default:
                throw new UnsupportedOperationException("No randomized exceptions generated for type [" + type + "]");
        }

        Failure actual = new Failure(shard, nodeId, actualException, status, primary);
        Failure expected = new Failure(new ShardId(index, INDEX_UUID_NA_VALUE, shardId), nodeId, expectedException, status, primary);

        return Tuple.tuple(actual, expected);
    }

    public static AnalyzeToken randomToken(Random random) {
        String token = RandomStrings.randomAsciiLettersOfLengthBetween(random, 1, 20);
        int position = RandomizedTest.randomIntBetween(0, 1000);
        int startOffset = RandomizedTest.randomIntBetween(0, 1000);
        int endOffset = RandomizedTest.randomIntBetween(0, 1000);
        int posLength = RandomizedTest.randomIntBetween(1, 5);
        String type =  RandomStrings.randomAsciiLettersOfLengthBetween(random, 1, 20);
        Map<String, Object> extras = new HashMap<>();
        if (random.nextBoolean()) {
            int entryCount = RandomNumbers.randomIntBetween(random, 0, 6);
            for (int i = 0; i < entryCount; i++) {
                switch (RandomNumbers.randomIntBetween(random, 0, 6)) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                        String key = RandomStrings.randomAsciiLettersOfLength(random, 5);
                        String value = RandomStrings.randomAsciiLettersOfLength(random, 10);
                        extras.put(key, value);
                        break;
                    case 4:
                        String objkey = RandomStrings.randomAsciiLettersOfLength(random, 5);
                        Map<String, String> obj = new HashMap<>();
                        obj.put(RandomStrings.randomAsciiLettersOfLength(random, 5), RandomStrings.randomAsciiLettersOfLength(random, 10));
                        extras.put(objkey, obj);
                        break;
                    case 5:
                        String listkey = RandomStrings.randomAsciiLettersOfLength(random, 5);
                        List<String> list = new ArrayList<>();
                        list.add(RandomStrings.randomAsciiLettersOfLength(random, 4));
                        list.add(RandomStrings.randomAsciiLettersOfLength(random, 6));
                        extras.put(listkey, list);
                        break;
                }
            }
        }
        return new AnalyzeAction.AnalyzeToken(token, position, startOffset, endOffset, posLength, type, extras);
    }
}
