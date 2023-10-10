/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.percolator;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class PercolatorQuerySearchTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(PercolatorPlugin.class, CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("1==1", vars -> Boolean.TRUE);
            scripts.put("use_fielddata_please", vars -> {
                LeafDocLookup leafDocLookup = (LeafDocLookup) vars.get("_doc");
                ScriptDocValues<?> scriptDocValues = leafDocLookup.get("employees.name");
                return "virginia_potts".equals(scriptDocValues.get(0));
            });
            return scripts;
        }
    }

    public void testPercolateScriptQuery() throws IOException {
        indicesAdmin().prepareCreate("index").setMapping("query", "type=percolator").get();
        client().prepareIndex("index")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field(
                        "query",
                        QueryBuilders.scriptQuery(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "1==1", Collections.emptyMap()))
                    )
                    .endObject()
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute()
            .actionGet();
        SearchResponse response = client().prepareSearch("index")
            .setQuery(
                new PercolateQueryBuilder(
                    "query",
                    BytesReference.bytes(jsonBuilder().startObject().field("field1", "b").endObject()),
                    XContentType.JSON
                )
            )
            .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");
    }

    public void testPercolateQueryWithNestedDocuments_doNotLeakBitsetCacheEntries() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject()
            .startObject("properties")
            .startObject("companyname")
            .field("type", "text")
            .endObject()
            .startObject("query")
            .field("type", "percolator")
            .endObject()
            .startObject("employee")
            .field("type", "nested")
            .startObject("properties")
            .startObject("name")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex(
            "test",
            indicesAdmin().prepareCreate("test")
                // to avoid normal document from being cached by BitsetFilterCache
                .setSettings(Settings.builder().put(BitsetFilterCache.INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING.getKey(), false))
                .setMapping(mapping)
        );
        client().prepareIndex("test")
            .setId("q1")
            .setSource(
                jsonBuilder().startObject()
                    .field(
                        "query",
                        QueryBuilders.nestedQuery(
                            "employee",
                            matchQuery("employee.name", "virginia potts").operator(Operator.AND),
                            ScoreMode.Avg
                        )
                    )
                    .endObject()
            )
            .get();
        indicesAdmin().prepareRefresh().get();

        for (int i = 0; i < 32; i++) {
            SearchResponse response = client().prepareSearch()
                .setQuery(
                    new PercolateQueryBuilder(
                        "query",
                        BytesReference.bytes(
                            XContentFactory.jsonBuilder()
                                .startObject()
                                .field("companyname", "stark")
                                .startArray("employee")
                                .startObject()
                                .field("name", "virginia potts")
                                .endObject()
                                .startObject()
                                .field("name", "tony stark")
                                .endObject()
                                .endArray()
                                .endObject()
                        ),
                        XContentType.JSON
                    )
                )
                .addSort("_doc", SortOrder.ASC)
                // size 0, because other wise load bitsets for normal document in FetchPhase#findRootDocumentIfNested(...)
                .setSize(0)
                .get();
            assertHitCount(response, 1);
        }

        // We can't check via api... because BitsetCacheListener requires that it can extract shardId from index reader
        // and for percolator it can't do that, but that means we don't keep track of
        // memory for BitsetCache in case of percolator
        long bitsetSize = clusterAdmin().prepareClusterStats().get().getIndicesStats().getSegments().getBitsetMemoryInBytes();
        assertEquals("The percolator works with in-memory index and therefor shouldn't use bitset cache", 0L, bitsetSize);
    }

    public void testPercolateQueryWithNestedDocuments_doLeakFieldDataCacheEntries() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("properties");
            {
                mapping.startObject("query");
                mapping.field("type", "percolator");
                mapping.endObject();
            }
            {
                mapping.startObject("companyname");
                mapping.field("type", "text");
                mapping.endObject();
            }
            {
                mapping.startObject("employees");
                mapping.field("type", "nested");
                {
                    mapping.startObject("properties");
                    {
                        mapping.startObject("name");
                        mapping.field("type", "text");
                        mapping.field("fielddata", true);
                        mapping.endObject();
                    }
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        createIndex("test", indicesAdmin().prepareCreate("test").setMapping(mapping));
        Script script = new Script(ScriptType.INLINE, MockScriptPlugin.NAME, "use_fielddata_please", Collections.emptyMap());
        client().prepareIndex("test")
            .setId("q1")
            .setSource(
                jsonBuilder().startObject()
                    .field("query", QueryBuilders.nestedQuery("employees", QueryBuilders.scriptQuery(script), ScoreMode.Avg))
                    .endObject()
            )
            .get();
        indicesAdmin().prepareRefresh().get();
        XContentBuilder doc = jsonBuilder();
        doc.startObject();
        {
            doc.field("companyname", "stark");
            doc.startArray("employees");
            {
                doc.startObject();
                doc.field("name", "virginia_potts");
                doc.endObject();
            }
            {
                doc.startObject();
                doc.field("name", "tony_stark");
                doc.endObject();
            }
            doc.endArray();
        }
        doc.endObject();
        for (int i = 0; i < 32; i++) {
            SearchResponse response = client().prepareSearch()
                .setQuery(new PercolateQueryBuilder("query", BytesReference.bytes(doc), XContentType.JSON))
                .addSort("_doc", SortOrder.ASC)
                .get();
            assertHitCount(response, 1);
        }

        long fieldDataSize = clusterAdmin().prepareClusterStats().get().getIndicesStats().getFieldData().getMemorySizeInBytes();
        assertEquals("The percolator works with in-memory index and therefor shouldn't use field-data cache", 0L, fieldDataSize);
    }

    public void testMapUnmappedFieldAsText() throws IOException {
        Settings.Builder settings = Settings.builder().put("index.percolator.map_unmapped_fields_as_text", true);
        createIndex("test", settings.build(), "query", "query", "type=percolator");
        client().prepareIndex("test")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "value")).endObject())
            .get();
        indicesAdmin().prepareRefresh().get();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(
                new PercolateQueryBuilder(
                    "query",
                    BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").endObject()),
                    XContentType.JSON
                )
            )
            .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");
    }

    public void testRangeQueriesWithNow() throws Exception {
        IndexService indexService = createIndex(
            "test",
            Settings.builder().put("index.number_of_shards", 1).build(),
            "_doc",
            "field1",
            "type=keyword",
            "field2",
            "type=date",
            "query",
            "type=percolator"
        );

        client().prepareIndex("test")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("query", rangeQuery("field2").from("now-1h").to("now+1h")).endObject())
            .get();
        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field(
                        "query",
                        boolQuery().filter(termQuery("field1", "value")).filter(rangeQuery("field2").from("now-1h").to("now+1h"))
                    )
                    .endObject()
            )
            .get();

        Script script = new Script(ScriptType.INLINE, MockScriptPlugin.NAME, "1==1", Collections.emptyMap());
        client().prepareIndex("test")
            .setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("query", boolQuery().filter(scriptQuery(script)).filter(rangeQuery("field2").from("now-1h").to("now+1h")))
                    .endObject()
            )
            .get();
        indicesAdmin().prepareRefresh().get();

        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("test")) {
            long[] currentTime = new long[] { System.currentTimeMillis() };
            SearchExecutionContext searchExecutionContext = indexService.newSearchExecutionContext(
                0,
                0,
                searcher,
                () -> currentTime[0],
                null,
                emptyMap()
            );

            BytesReference source = BytesReference.bytes(
                jsonBuilder().startObject().field("field1", "value").field("field2", currentTime[0]).endObject()
            );
            QueryBuilder queryBuilder = new PercolateQueryBuilder("query", source, XContentType.JSON);
            Query query = queryBuilder.toQuery(searchExecutionContext);
            assertThat(searcher.count(query), equalTo(3));

            currentTime[0] = currentTime[0] + 10800000; // + 3 hours
            source = BytesReference.bytes(jsonBuilder().startObject().field("field1", "value").field("field2", currentTime[0]).endObject());
            queryBuilder = new PercolateQueryBuilder("query", source, XContentType.JSON);
            query = queryBuilder.toQuery(searchExecutionContext);
            assertThat(searcher.count(query), equalTo(3));
        }
    }

}
