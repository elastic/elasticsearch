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

package org.elasticsearch.search.morelikethis;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.Random;

public class ItemSerializationTests extends ESTestCase {

    private Item generateRandomItem(int arraySize, int stringSize) {
        String index = randomAsciiOfLength(stringSize);
        String type = randomAsciiOfLength(stringSize);
        String id = String.valueOf(Math.abs(randomInt()));
        String[] fields = generateRandomStringArray(arraySize, stringSize, true);
        String routing = randomBoolean() ? randomAsciiOfLength(stringSize) : null;
        long version = Math.abs(randomLong());
        VersionType versionType = RandomPicks.randomFrom(new Random(), VersionType.values());
        return new Item(index, type, id).fields(fields).routing(routing).version(version).versionType(versionType);
    }

    @Test
    public void testItemSerialization() throws Exception {
        int numOfTrials = 100;
        int maxArraySize = 7;
        int maxStringSize = 8;
        for (int i = 0; i < numOfTrials; i++) {
            Item item1 = generateRandomItem(maxArraySize, maxStringSize);
            String json = item1.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string();
            XContentParser parser = XContentFactory.xContent(json).createParser(json);
            Item item2 = Item.parse(parser, ParseFieldMatcher.STRICT, new Item());
            assertEquals(item1, item2);
        }
    }
}
