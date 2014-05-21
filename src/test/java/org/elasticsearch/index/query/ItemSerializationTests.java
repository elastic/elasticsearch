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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.Matchers.is;

public class ItemSerializationTests extends ElasticsearchTestCase {

    private Item generateRandomItem(int arraySize, int stringSize) {
        String index = randomAsciiOfLength(stringSize);
        String type = randomAsciiOfLength(stringSize);
        String id = String.valueOf(Math.abs(randomInt()));
        String routing = randomBoolean() ? randomAsciiOfLength(stringSize) : null;
        String[] fields = generateRandomStringArray(arraySize, stringSize, true);

        long version = Math.abs(randomLong());
        VersionType versionType = RandomPicks.randomFrom(new Random(), VersionType.values());

        FetchSourceContext fetchSourceContext;
        switch (randomIntBetween(0, 3)) {
            case 0 :
                fetchSourceContext = new FetchSourceContext(randomBoolean());
                break;
            case 1 :
                fetchSourceContext = new FetchSourceContext(generateRandomStringArray(arraySize, stringSize, true));
                break;
            case 2 :
                fetchSourceContext = new FetchSourceContext(generateRandomStringArray(arraySize, stringSize, true),
                        generateRandomStringArray(arraySize, stringSize, true));
                break;
            default:
                fetchSourceContext = null;
                break;
        }
        return (Item) new Item(index, type, id).routing(routing).fields(fields).version(version).versionType(versionType)
                .fetchSourceContext(fetchSourceContext);
    }

    private String ItemToJSON(Item item) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startArray("docs");
        item.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endArray();
        builder.endObject();
        return XContentHelper.convertToJson(builder.bytes(), false);
    }

    private MultiGetRequest.Item JSONtoItem(String json) throws Exception {
        MultiGetRequest request = new MultiGetRequest().add(null, null, null, null, new BytesArray(json), true);
        return request.getItems().get(0);
    }

    @Test
    public void testItemSerialization() throws Exception {
        int numOfTrials = 100;
        int maxArraySize = 7;
        int maxStringSize = 8;
        for (int i = 0; i < numOfTrials; i++) {
            Item item1 = generateRandomItem(maxArraySize, maxStringSize);
            String json = ItemToJSON(item1);
            MultiGetRequest.Item item2 = JSONtoItem(json);
            assertEquals(item1, item2);
        }
    }

    private List<MultiGetRequest.Item> testItemsFromJSON(String json) throws Exception {
        MultiGetRequest request = new MultiGetRequest();
        request.add(null, null, null, null, new BytesArray(json), true);
        List<MultiGetRequest.Item> items = request.getItems();

        assertEquals(items.size(), 3);
        for (MultiGetRequest.Item item : items) {
            assertThat(item.index(), is("test"));
            assertThat(item.type(), is("type"));
            FetchSourceContext fetchSource = item.fetchSourceContext();
            switch (item.id()) {
                case "1" :
                    assertThat(fetchSource.fetchSource(), is(false));
                    break;
                case "2" :
                    assertThat(fetchSource.fetchSource(), is(true));
                    assertThat(fetchSource.includes(), is(new String[]{"field3", "field4"}));
                    break;
                case "3" :
                    assertThat(fetchSource.fetchSource(), is(true));
                    assertThat(fetchSource.includes(), is(new String[]{"user"}));
                    assertThat(fetchSource.excludes(), is(new String[]{"user.location"}));
                    break;
                default:
                    fail("item with id: " + item.id() + " is not 1, 2 or 3");
                    break;
            }
        }
        return items;
    }

    @Test
    public void testSimpleItemSerializationFromFile() throws Exception {
        // test items from JSON
        List<MultiGetRequest.Item> itemsFromJSON = testItemsFromJSON(
                copyToStringFromClasspath("/org/elasticsearch/index/query/items.json"));

        // create builder from items
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startArray("docs");
        for (MultiGetRequest.Item item : itemsFromJSON) {
            MoreLikeThisQueryBuilder.Item itemForBuilder = (MoreLikeThisQueryBuilder.Item) new MoreLikeThisQueryBuilder.Item(
                    item.index(), item.type(), item.id())
                    .fetchSourceContext(item.fetchSourceContext())
                    .fields(item.fields());
            itemForBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endArray();
        builder.endObject();

        // verify generated JSON lead to the same items
        String json = XContentHelper.convertToJson(builder.bytes(), false);
        testItemsFromJSON(json);
    }

}
