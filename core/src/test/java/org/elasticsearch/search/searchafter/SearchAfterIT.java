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

package org.elasticsearch.search.searchafter;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchContextException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.Matchers.equalTo;

public class SearchAfterIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test";
    private static final String TYPE_NAME = "type1";
    private static final int NUM_DOCS = 100;

    public void testsShouldFail() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field2", "type=keyword").get());
        ensureGreen();
        indexRandom(true, client().prepareIndex("test", "type1", "0").setSource("field1", 0, "field2", "toto"));
        try {
            client().prepareSearch("test")
                    .addSort("field1", SortOrder.ASC)
                    .setQuery(matchAllQuery())
                    .searchAfter(new Object[]{0})
                    .setScroll("1m")
                    .get();

            fail("Should fail on search_after cannot be used with scroll.");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getClass(), Matchers.equalTo(RemoteTransportException.class));
            assertThat(e.getCause().getCause().getClass(), Matchers.equalTo(SearchContextException.class));
            assertThat(e.getCause().getCause().getMessage(), Matchers.equalTo("`search_after` cannot be used in a scroll context."));
        }
        try {
            client().prepareSearch("test")
                .addSort("field1", SortOrder.ASC)
                .setQuery(matchAllQuery())
                .searchAfter(new Object[]{0})
                .setFrom(10)
                .get();

            fail("Should fail on search_after cannot be used with from > 0.");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getClass(), Matchers.equalTo(RemoteTransportException.class));
            assertThat(e.getCause().getCause().getClass(), Matchers.equalTo(SearchContextException.class));
            assertThat(e.getCause().getCause().getMessage(), Matchers.equalTo("`from` parameter must be set to 0 when `search_after` is used."));
        }

        try {
            client().prepareSearch("test")
                    .setQuery(matchAllQuery())
                    .searchAfter(new Object[]{0.75f})
                    .get();

            fail("Should fail on search_after on score only is disabled");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getClass(), Matchers.equalTo(RemoteTransportException.class));
            assertThat(e.getCause().getCause().getClass(), Matchers.equalTo(IllegalArgumentException.class));
            assertThat(e.getCause().getCause().getMessage(), Matchers.equalTo("Sort must contain at least one field."));
        }

        try {
            client().prepareSearch("test")
                    .addSort("field2", SortOrder.DESC)
                    .addSort("field1", SortOrder.ASC)
                    .setQuery(matchAllQuery())
                    .searchAfter(new Object[]{1})
                    .get();
            fail("Should fail on search_after size differs from sort field size");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getClass(), Matchers.equalTo(RemoteTransportException.class));
            assertThat(e.getCause().getCause().getClass(), Matchers.equalTo(IllegalArgumentException.class));
            assertThat(e.getCause().getCause().getMessage(), Matchers.equalTo("search_after has 1 value(s) but sort has 2."));
        }

        try {
            client().prepareSearch("test")
                    .setQuery(matchAllQuery())
                    .addSort("field1", SortOrder.ASC)
                    .searchAfter(new Object[]{1, 2})
                    .get();
            fail("Should fail on search_after size differs from sort field size");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getClass(), Matchers.equalTo(RemoteTransportException.class));
            assertThat(e.getCause().getCause().getClass(), Matchers.equalTo(IllegalArgumentException.class));
            assertThat(e.getCause().getCause().getMessage(), Matchers.equalTo("search_after has 2 value(s) but sort has 1."));
        }

        try {
            client().prepareSearch("test")
                    .setQuery(matchAllQuery())
                    .addSort("field1", SortOrder.ASC)
                    .searchAfter(new Object[]{"toto"})
                    .get();

            fail("Should fail on search_after on score only is disabled");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getCause().getClass(), Matchers.equalTo(RemoteTransportException.class));
            assertThat(e.getCause().getCause().getClass(), Matchers.equalTo(IllegalArgumentException.class));
            assertThat(e.getCause().getCause().getMessage(), Matchers.equalTo("Failed to parse search_after value for field [field1]."));
        }
    }

    public void testWithNullStrings() throws ExecutionException, InterruptedException {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field2", "type=keyword").get());
        ensureGreen();
        indexRandom(true,
                client().prepareIndex("test", "type1", "0").setSource("field1", 0),
                client().prepareIndex("test", "type1", "1").setSource("field1", 100, "field2", "toto"));
        SearchResponse searchResponse = client().prepareSearch("test")
                .addSort("field1", SortOrder.ASC)
                .addSort("field2", SortOrder.ASC)
                .setQuery(matchAllQuery())
                .searchAfter(new Object[]{0, null})
                .get();
        assertThat(searchResponse.getHits().getTotalHits(), Matchers.equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, Matchers.equalTo(1));
        assertThat(searchResponse.getHits().getHits()[0].sourceAsMap().get("field1"), Matchers.equalTo(100));
        assertThat(searchResponse.getHits().getHits()[0].sourceAsMap().get("field2"), Matchers.equalTo("toto"));
    }

    public void testWithSimpleTypes() throws Exception {
        int numFields = randomInt(20) + 1;
        int[] types = new int[numFields-1];
        for (int i = 0; i < numFields-1; i++) {
            types[i] = randomInt(6);
        }
        List<List> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; i++) {
            List values = new ArrayList<>();
            for (int type : types) {
                switch (type) {
                    case 0:
                        values.add(randomBoolean());
                        break;
                    case 1:
                        values.add(randomByte());
                        break;
                    case 2:
                        values.add(randomShort());
                        break;
                    case 3:
                        values.add(randomInt());
                        break;
                    case 4:
                        values.add(randomFloat());
                        break;
                    case 5:
                        values.add(randomDouble());
                        break;
                    case 6:
                        values.add(randomAsciiOfLengthBetween(5, 20));
                        break;
                }
            }
            values.add(UUIDs.randomBase64UUID());
            documents.add(values);
        }
        int reqSize = randomInt(NUM_DOCS-1);
        if (reqSize == 0) {
            reqSize = 1;
        }
        assertSearchFromWithSortValues(INDEX_NAME, TYPE_NAME, documents, reqSize);
    }

    private static class ListComparator implements Comparator<List> {
        @Override
        public int compare(List o1, List o2) {
            if (o1.size() > o2.size()) {
                return 1;
            }

            if (o2.size() > o1.size()) {
                return -1;
            }

            for (int i = 0; i < o1.size(); i++) {
                if (!(o1.get(i) instanceof Comparable)) {
                    throw new RuntimeException(o1.get(i).getClass() + " is not comparable");
                }
                Object cmp1 = o1.get(i);
                Object cmp2 = o2.get(i);
                int cmp = ((Comparable)cmp1).compareTo(cmp2);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    }
    private ListComparator LST_COMPARATOR = new ListComparator();

    private void assertSearchFromWithSortValues(String indexName, String typeName, List<List> documents, int reqSize) throws Exception {
        int numFields = documents.get(0).size();
        {
            createIndexMappingsFromObjectType(indexName, typeName, documents.get(0));
            List<IndexRequestBuilder> requests = new ArrayList<>();
            for (int i = 0; i < documents.size(); i++) {
                XContentBuilder builder = jsonBuilder();
                assertThat(documents.get(i).size(), Matchers.equalTo(numFields));
                builder.startObject();
                for (int j = 0; j < numFields; j++) {
                    builder.field("field" + Integer.toString(j), documents.get(i).get(j));
                }
                builder.endObject();
                requests.add(client().prepareIndex(INDEX_NAME, TYPE_NAME, Integer.toString(i)).setSource(builder));
            }
            indexRandom(true, requests);
        }

        Collections.sort(documents, LST_COMPARATOR);
        int offset = 0;
        Object[] sortValues = null;
        while (offset < documents.size()) {
            SearchRequestBuilder req = client().prepareSearch(indexName);
            for (int i = 0; i < documents.get(0).size(); i++) {
                req.addSort("field" + Integer.toString(i), SortOrder.ASC);
            }
            req.setQuery(matchAllQuery()).setSize(reqSize);
            if (sortValues != null) {
                req.searchAfter(sortValues);
            }
            SearchResponse searchResponse = req.get();
            for (SearchHit hit : searchResponse.getHits()) {
                List toCompare = convertSortValues(documents.get(offset++));
                assertThat(LST_COMPARATOR.compare(toCompare, Arrays.asList(hit.sortValues())), equalTo(0));
            }
            sortValues = searchResponse.getHits().hits()[searchResponse.getHits().hits().length-1].getSortValues();
        }
    }

    private void createIndexMappingsFromObjectType(String indexName, String typeName, List<Object> types) {
        CreateIndexRequestBuilder indexRequestBuilder = client().admin().indices().prepareCreate(indexName);
        List<String> mappings = new ArrayList<> ();
        int numFields = types.size();
        for (int i = 0; i < numFields; i++) {
            Class type = types.get(i).getClass();
            if (type == Integer.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=integer");
            } else if (type == Long.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=long");
            } else if (type == Float.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=float");
            } else if (type == Double.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=double");
            } else if (type == Byte.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=byte");
            } else if (type == Short.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=short");
            } else if (type == Boolean.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=boolean");
            } else if (types.get(i) instanceof String) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=keyword");
            } else {
                fail("Can't match type [" + type + "]");
            }
        }
        indexRequestBuilder.addMapping(typeName, mappings.toArray()).get();
        ensureGreen();
    }

    // Convert Integer, Short, Byte and Boolean to Long in order to match the conversion done
    // by the internal hits when populating the sort values.
    private List<Object> convertSortValues(List<Object> sortValues) {
        List<Object> converted = new ArrayList<> ();
        for (int i = 0; i < sortValues.size(); i++) {
            Object from = sortValues.get(i);
            if (from instanceof Integer) {
                converted.add(((Integer) from).longValue());
            } else if (from instanceof Short) {
                converted.add(((Short) from).longValue());
            } else if (from instanceof Byte) {
                converted.add(((Byte) from).longValue());
            } else if (from instanceof Boolean) {
                boolean b = (boolean) from;
                if (b) {
                    converted.add(1L);
                } else {
                    converted.add(0L);
                }
            } else {
                converted.add(from);
            }
        }
        return converted;
    }
}
