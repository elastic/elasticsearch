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

package org.elasticsearch.common.lucene.docset;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

public class DocIdSetsTests extends ElasticsearchSingleNodeTest {

    private static final Settings SINGLE_SHARD_SETTINGS = ImmutableSettings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).build();

    private void test(IndexService indexService, boolean broken, FilterBuilder filterBuilder) throws IOException {
        client().admin().indices().prepareRefresh("test").get();
        XContentBuilder builder = filterBuilder.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS);
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        Filter filter = indexService.queryParserService().parseInnerFilter(parser).filter();
        try (Searcher searcher = indexService.shardSafe(0).acquireSearcher("test")) {
            final LeafReaderContext ctx = searcher.reader().leaves().get(0);
            DocIdSet set = filter.getDocIdSet(ctx, null);
            assertEquals(broken, DocIdSets.isBroken(set.iterator()));
        }
    }

    public void testTermIsNotBroken() throws IOException {
        IndexService indexService = createIndex("test", SINGLE_SHARD_SETTINGS, "type", "l", "type=long");
        client().prepareIndex("test", "type").setSource("l", 7).get();
        TermFilterBuilder filter = FilterBuilders.termFilter("l", 7).cache(randomBoolean());
        test(indexService, false, filter);
    }

    public void testDefaultGeoIsBroken() throws IOException {
        // Geo is slow by default :'(
        IndexService indexService = createIndex("test", SINGLE_SHARD_SETTINGS, "type", "gp", "type=geo_point");
        client().prepareIndex("test", "type").setSource("gp", "2,3").get();
        FilterBuilder filter = FilterBuilders.geoDistanceFilter("gp").distance(1000, DistanceUnit.KILOMETERS).point(3, 2);
        test(indexService, true, filter);

    }

    public void testIndexedGeoIsNotBroken() throws IOException {
        // Geo has a fast iterator when indexing lat,lon and using the "indexed" bbox optimization
        IndexService indexService = createIndex("test", SINGLE_SHARD_SETTINGS, "type", "gp", "type=geo_point,lat_lon=true");
        client().prepareIndex("test", "type").setSource("gp", "2,3").get();
        FilterBuilder filter = FilterBuilders.geoDistanceFilter("gp").distance(1000, DistanceUnit.KILOMETERS).point(3, 2).optimizeBbox("indexed");
        test(indexService, false, filter);
    }

    public void testScriptIsBroken() throws IOException { //  by nature unfortunately
        IndexService indexService = createIndex("test", SINGLE_SHARD_SETTINGS, "type", "l", "type=long");
        client().prepareIndex("test", "type").setSource("l", 7).get();
        FilterBuilder filter = FilterBuilders.scriptFilter("doc['l'].value < 8");
        test(indexService, true, filter);
    }

    public void testCachedIsNotBroken() throws IOException {
        IndexService indexService = createIndex("test", SINGLE_SHARD_SETTINGS, "type", "l", "type=long");
        client().prepareIndex("test", "type").setSource("l", 7).get();
        // This filter is inherently slow but by caching it we pay the price at caching time, not iteration
        FilterBuilder filter = FilterBuilders.scriptFilter("doc['l'].value < 8").cache(true);
        test(indexService, false, filter);
    }

    public void testOr() throws IOException {
        IndexService indexService = createIndex("test", SINGLE_SHARD_SETTINGS, "type", "l", "type=long");
        client().prepareIndex("test", "type").setSource("l", new long[] {7, 8}).get();
        // Or with fast clauses is fast
        FilterBuilder filter = FilterBuilders.orFilter(FilterBuilders.termFilter("l", 7), FilterBuilders.termFilter("l", 8));
        test(indexService, false, filter);
        // But if at least one clause is broken, it is broken
        filter = FilterBuilders.orFilter(FilterBuilders.termFilter("l", 7), FilterBuilders.scriptFilter("doc['l'].value < 8"));
        test(indexService, true, filter);
    }

    public void testAnd() throws IOException {
        IndexService indexService = createIndex("test", SINGLE_SHARD_SETTINGS, "type", "l", "type=long");
        client().prepareIndex("test", "type").setSource("l", new long[] {7, 8}).get();
        // And with fast clauses is fast
        FilterBuilder filter = FilterBuilders.andFilter(FilterBuilders.termFilter("l", 7), FilterBuilders.termFilter("l", 8));
        test(indexService, false, filter);
        // If at least one clause is 'fast' and the other clauses supports random-access, it is still fast
        filter = FilterBuilders.andFilter(FilterBuilders.termFilter("l", 7).cache(randomBoolean()), FilterBuilders.scriptFilter("doc['l'].value < 8"));
        test(indexService, false, filter);
        // However if all clauses are broken, the and is broken
        filter = FilterBuilders.andFilter(FilterBuilders.scriptFilter("doc['l'].value > 5"), FilterBuilders.scriptFilter("doc['l'].value < 8"));
        test(indexService, true, filter);
    }

}
