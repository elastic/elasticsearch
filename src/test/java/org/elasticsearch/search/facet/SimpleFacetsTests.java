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

package org.elasticsearch.search.facet;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
import org.elasticsearch.search.facet.filter.FilterFacet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.facet.query.QueryFacet;
import org.elasticsearch.search.facet.range.RangeFacet;
import org.elasticsearch.search.facet.statistical.StatisticalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacet.Entry;
import org.elasticsearch.search.facet.terms.doubles.InternalDoubleTermsFacet;
import org.elasticsearch.search.facet.terms.longs.InternalLongTermsFacet;
import org.elasticsearch.search.facet.termsstats.TermsStatsFacet;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.search.facet.FacetBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

/**
 *
 */

public class SimpleFacetsTests extends ElasticsearchIntegrationTest {
    private int numRuns = -1;

    protected int numberOfRuns() {
        if (numRuns == -1) {
            numRuns = scaledRandomIntBetween(3, 10);
        }
        return numRuns;
    }

    @Test
    public void testSimpleFacetEmptyFacetFilter() throws Exception {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("tag", "green")
                .endObject()).execute().actionGet();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(new BytesArray(
                        "{\"facet1\":{\"filter\":{ }}}"))
                .get();

        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().hits().length, equalTo(0));
        FilterFacet facet = searchResponse.getFacets().facet("facet1");
        assertThat(facet.getName(), equalTo("facet1"));
        assertThat(facet.getCount(), equalTo(1l));
    }

    @Test
    public void testSimpleFacetEmptyFilterFacet() throws Exception {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("tag", "green")
                .endObject()).execute().actionGet();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
                .setSearchType(SearchType.COUNT)
                .setFacets(new BytesArray(
                        "{\"facet1\":{\"terms\":{\"field\":\"tag\"},\"facet_filter\":{ }}}"))
                .get();

        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().hits().length, equalTo(0));
        TermsFacet facet = searchResponse.getFacets().facet("facet1");
        assertThat(facet.getName(), equalTo("facet1"));
        assertThat(facet.getEntries().size(), equalTo(1));
        assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("green"));
        assertThat(facet.getEntries().get(0).getCount(), equalTo(1));
    }

    @Test
    public void testBinaryFacet() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("tag", "green")
                .endObject()).execute().actionGet();

        flushAndRefresh();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("tag", "blue")
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setSearchType(SearchType.COUNT)
                    .setFacets(XContentFactory.jsonBuilder().startObject()
                            .startObject("facet1")
                            .startObject("terms")
                            .field("field", "tag")
                            .endObject()
                            .endObject()
                            .endObject().bytes())
                    .execute().actionGet();

            assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
            assertThat(searchResponse.getHits().hits().length, equalTo(0));
            TermsFacet facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1));
            assertThat(facet.getEntries().get(1).getTerm().string(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1));
        }
    }

    @Test
    public void testFacetNumeric() throws ElasticsearchException, IOException {
        assertAcked(prepareCreate("test").addMapping("type", jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("byte").field("type", "byte").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                .startObject("short").field("type", "short").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                .startObject("integer").field("type", "integer").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                .startObject("long").field("type", "long").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                .startObject("float").field("type", "float").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                .startObject("double").field("type", "double").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                .endObject().endObject().endObject()));
        ensureGreen();

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type", "" + i).setSource(jsonBuilder().startObject()
                    .field("name", "" + i)
                    .field("multiValued", "" + i, "" + (90 + i % 10))
                    .field("byte", i)
                    .field("short", i + Byte.MAX_VALUE)
                    .field("integer", i + Short.MAX_VALUE)
                    .field("long", i + Integer.MAX_VALUE)
                    .field("float", (float) i)
                    .field("double", (double) i)
                    .endObject()).execute().actionGet();
        }

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type", "" + (i + 100)).setSource(jsonBuilder().startObject()
                    .field("foo", "" + i)
                    .endObject()).execute().actionGet();
        }
        int shardSize = getNumShards("test").numPrimaries > 5 ? 20 : 10;
        String[] execHint = new String[]{"map", null};
        for (String hint : execHint) {

            flushAndRefresh();
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("double").shardSize(shardSize).executionHint(hint).field("double").size(10))
                    .addFacet(termsFacet("float").shardSize(shardSize).executionHint(hint).field("float").size(10))
                    .addFacet(termsFacet("integer").shardSize(shardSize).executionHint(hint).field("integer").size(10))
                    .addFacet(termsFacet("long").shardSize(shardSize).executionHint(hint).field("long").size(10))
                    .addFacet(termsFacet("short").shardSize(shardSize).executionHint(hint).field("short").size(10))
                    .addFacet(termsFacet("byte").shardSize(shardSize).executionHint(hint).field("byte").size(10))
                    .addFacet(termsFacet("termFacet").shardSize(shardSize).executionHint(hint).field("name").size(10))
                    .addFacet(termsFacet("termFacetRegex").shardSize(shardSize).executionHint(hint).field("multiValued").regex("9\\d").size(20))
                    .addFacet(termsFacet("termFacetScript").shardSize(shardSize).executionHint(hint).field("multiValued").script("Integer.toHexString(Integer.parseInt(term))").size(10))
                    .addFacet(termsFacet("termFacetScriptRegex").shardSize(shardSize).executionHint(hint).field("multiValued").script("Integer.toHexString(Integer.parseInt(term))").regex("9\\d").size(20))

                    .execute().actionGet();

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(110l));
            TermsFacet facet = searchResponse.getFacets().facet("termFacet");
            assertThat(facet.getName(), equalTo("termFacet"));
            assertThat(facet.getEntries().size(), equalTo(10));
            assertThat(facet.getTotalCount(), equalTo(100l));
            assertThat(facet.getOtherCount(), equalTo(90l));
            assertThat(facet.getMissingCount(), equalTo(10l));

            facet = searchResponse.getFacets().facet("termFacetRegex");
            assertThat(facet.getName(), equalTo("termFacetRegex"));
            assertThat(facet.getEntries().size(), equalTo(10));
            assertThat(facet.getTotalCount(), equalTo(190l));
            assertThat(facet.getOtherCount(), equalTo(90l));
            assertThat(facet.getMissingCount(), equalTo(10l));

            int count = 99;
            for (Entry entry : facet) {
                assertThat(Integer.parseInt(entry.getTerm().string()), equalTo(count--));
                assertThat(entry.getCount(), equalTo(10));
            }

            facet = searchResponse.getFacets().facet("termFacetScriptRegex");
            assertThat(facet.getName(), equalTo("termFacetScriptRegex"));
            assertThat(facet.getEntries().size(), equalTo(10));
            assertThat(facet.getTotalCount(), equalTo(190l));
            assertThat(facet.getOtherCount(), equalTo(90l));
            assertThat(facet.getMissingCount(), equalTo(10l));

            count = 99;
            for (Entry entry : facet) {
                assertThat(entry.getTerm().string(), equalTo(Integer.toHexString(count--)));
                assertThat(entry.getCount(), equalTo(10));
            }

            facet = searchResponse.getFacets().facet("termFacetScript");
            assertThat(facet.getName(), equalTo("termFacetScript"));
            assertThat(facet.getEntries().size(), equalTo(10));
            assertThat(facet.getTotalCount(), equalTo(190l));
            assertThat(facet.getOtherCount(), equalTo(90l));
            assertThat(facet.getMissingCount(), equalTo(10l));

            count = 99;
            for (Entry entry : facet) {
                assertThat(entry.getTerm().string(), equalTo(Integer.toHexString(count--)));
                assertThat(entry.getCount(), equalTo(10));
            }

            facet = searchResponse.getFacets().facet("double");
            assertThat(facet.getName(), equalTo("double"));
            assertThat(facet.getEntries().size(), equalTo(10));
            assertThat(facet.getTotalCount(), equalTo(100l));
            assertThat(facet.getOtherCount(), equalTo(90l));
            assertThat(facet.getMissingCount(), equalTo(10l));

            facet = searchResponse.getFacets().facet("float");
            assertThat(facet.getName(), equalTo("float"));
            assertThat(facet.getEntries().size(), equalTo(10));
            assertThat(facet.getTotalCount(), equalTo(100l));
            assertThat(facet.getOtherCount(), equalTo(90l));
            assertThat(facet.getMissingCount(), equalTo(10l));

            facet = searchResponse.getFacets().facet("long");
            assertThat(facet.getName(), equalTo("long"));
            assertThat(facet.getEntries().size(), equalTo(10));
            assertThat(facet.getTotalCount(), equalTo(100l));
            assertThat(facet.getOtherCount(), equalTo(90l));
            assertThat(facet.getMissingCount(), equalTo(10l));

            facet = searchResponse.getFacets().facet("integer");
            assertThat(facet.getName(), equalTo("integer"));
            assertThat(facet.getEntries().size(), equalTo(10));
            assertThat(facet.getTotalCount(), equalTo(100l));
            assertThat(facet.getOtherCount(), equalTo(90l));
            assertThat(facet.getMissingCount(), equalTo(10l));

            facet = searchResponse.getFacets().facet("short");
            assertThat(facet.getName(), equalTo("short"));
            assertThat(facet.getEntries().size(), equalTo(10));
            assertThat(facet.getTotalCount(), equalTo(100l));
            assertThat(facet.getOtherCount(), equalTo(90l));
            assertThat(facet.getMissingCount(), equalTo(10l));
        }
    }

    @Test
    @Slow
    public void testConcurrentFacets() throws ElasticsearchException, IOException, InterruptedException, ExecutionException {
        assertAcked(prepareCreate("test")
                .addMapping("type", jsonBuilder().startObject().startObject("type").startObject("properties")
                        .startObject("byte").field("type", "byte").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                        .startObject("short").field("type", "short").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                        .startObject("integer").field("type", "integer").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                        .startObject("long").field("type", "long").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                        .startObject("float").field("type", "float").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                        .startObject("double").field("type", "double").startObject("fielddata").field("format", maybeDocValues() ? "doc_values" : null).endObject().endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type", "" + i).setSource(jsonBuilder().startObject()
                    .field("name", "" + i)
                    .field("byte", i)
                    .field("short", i + Byte.MAX_VALUE)
                    .field("integer", i + Short.MAX_VALUE)
                    .field("long", i + Integer.MAX_VALUE)
                    .field("float", (float) i)
                    .field("double", (double) i)
                    .endObject()).execute().actionGet();
        }

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type", "" + (i + 100)).setSource(jsonBuilder().startObject()
                    .field("foo", "" + i)
                    .endObject()).execute().actionGet();
        }

        logger.info("done indexing. issue a refresh.");
        flushAndRefresh();
        final AtomicInteger searchId = new AtomicInteger(0);
        ConcurrentDuel<Facets> duel = new ConcurrentDuel<>(5);
        {
            final Client cl = client();

            duel.duel(new ConcurrentDuel.DuelJudge<Facets>() {

                          @Override
                          public void judge(Facets firstRun, Facets result) {
                              for (Facet f : result) {
                                  TermsFacet facet = (TermsFacet) f;
                                  assertThat(facet.getName(), isIn(new String[]{"short", "double", "byte", "float", "integer", "long", "termFacet"}));
                                  TermsFacet firstRunFacet = (TermsFacet) firstRun.getFacets().get(facet.getName());
                                  assertThat(facet.getEntries().size(), equalTo(firstRunFacet.getEntries().size()));

                                  assertThat(facet.getEntries().size(), equalTo(10));
                                  assertThat(facet.getTotalCount(), equalTo(100l));
                                  assertThat(facet.getOtherCount(), equalTo(90l));
                                  assertThat(facet.getMissingCount(), equalTo(10l));

                                  List<? extends Entry> right = facet.getEntries();
                                  List<? extends Entry> left = firstRunFacet.getEntries();

                                  for (int i = 0; i < facet.getEntries().size(); i++) {
                                      assertThat(left.get(i).getTerm(), equalTo(right.get(i).getTerm()));
                                      assertThat(left.get(i).getCount(), equalTo(right.get(i).getCount()));
                                  }
                              }
                          }
                      }, new ConcurrentDuel.DuelExecutor<Facets>() {
                          AtomicInteger count = new AtomicInteger();

                          @Override
                          public Facets run() {
                              final SearchRequestBuilder facetRequest;
                              int searchId = count.incrementAndGet();
                              if (searchId % 100 == 0) {
                                  logger.info("-> run {} searches", searchId);
                              }
                              if (searchId % 2 == 0) { // every second request is mapped
                                  facetRequest = cl.prepareSearch().setQuery(matchAllQuery())
                                          .addFacet(termsFacet("double").field("double").size(10))
                                          .addFacet(termsFacet("float").field("float").size(10))
                                          .addFacet(termsFacet("integer").field("integer").size(10))
                                          .addFacet(termsFacet("long").field("long").size(10))
                                          .addFacet(termsFacet("short").field("short").size(10))
                                          .addFacet(termsFacet("byte").field("byte").size(10))
                                          .addFacet(termsFacet("termFacet").field("name").size(10));
                              } else {
                                  facetRequest = cl.prepareSearch()
                                          .setQuery(matchAllQuery())
                                          .addFacet(termsFacet("double").executionHint("map").field("double").size(10))
                                          .addFacet(termsFacet("float").executionHint("map").field("float").size(10))
                                          .addFacet(termsFacet("integer").executionHint("map").field("integer").size(10))
                                          .addFacet(termsFacet("long").executionHint("map").field("long").size(10))
                                          .addFacet(termsFacet("short").executionHint("map").field("short").size(10))
                                          .addFacet(termsFacet("byte").executionHint("map").field("byte").size(10))
                                          .addFacet(termsFacet("termFacet").executionHint("map").field("name").size(10));
                              }

                              SearchResponse actionGet = facetRequest.execute().actionGet();
                              return actionGet.getFacets();
                          }
                      }, 5000
            );
        }
        logger.info("starting second duel");
        {
            duel.duel(new ConcurrentDuel.DuelJudge<Facets>() {

                          @Override
                          public void judge(Facets firstRun, Facets result) {
                              for (Facet f : result) {
                                  TermsFacet facet = (TermsFacet) f;
                                  assertThat(facet.getName(), equalTo("termFacet"));
                                  TermsFacet firstRunFacet = (TermsFacet) firstRun.getFacets().get(facet.getName());
                                  assertThat(facet.getEntries().size(), equalTo(firstRunFacet.getEntries().size()));

                                  assertThat(facet.getEntries().size(), equalTo(10));
                                  assertThat(facet.getTotalCount(), equalTo(100l));
                                  assertThat(facet.getOtherCount(), equalTo(90l));
                                  assertThat(facet.getMissingCount(), equalTo(10l));

                                  List<? extends Entry> right = facet.getEntries();
                                  List<? extends Entry> left = firstRunFacet.getEntries();

                                  for (int i = 0; i < facet.getEntries().size(); i++) {
                                      assertThat(left.get(i).getTerm(), equalTo(right.get(i).getTerm()));
                                      assertThat(left.get(i).getCount(), equalTo(right.get(i).getCount()));
                                  }
                              }
                          }
                      }, new ConcurrentDuel.DuelExecutor<Facets>() {
                          AtomicInteger count = new AtomicInteger();

                          @Override
                          public Facets run() {
                              final SearchRequestBuilder facetRequest;
                              int searchId = count.incrementAndGet();
                              if (searchId % 100 == 0) {
                                  logger.info("->  run {} searches", searchId);
                              }

                              switch (searchId % 6) {
                                  case 4:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").executionHint("map").field("name").script("\"\" + (Integer.parseInt(term) % 100)").size(10));
                                      break;
                                  case 3:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").field("name").regex("\\d+").size(10));
                                      break;
                                  case 2:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").executionHint("map").field("name").regex("\\d+").script("term").size(10));
                                      break;
                                  case 1:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").field("name").regex("\\d+").script("term").size(10));
                                      break;
                                  case 0:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").field("name").size(10));
                                      break;
                                  default:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").executionHint("map").field("name").size(10));
                                      break;
                              }
                              SearchResponse actionGet = facetRequest.execute().actionGet();
                              return actionGet.getFacets();
                          }
                      }, 5000
            );
        }

        duel.close();
    }

    @Test
    @Slow
    public void testDuelByteFieldDataImpl() throws ElasticsearchException, IOException, InterruptedException, ExecutionException {
        assertAcked(prepareCreate("test")
                .addMapping("type", jsonBuilder().startObject().startObject("type").startObject("properties")
                        .startObject("name_paged")
                        .field("type", "string")
                        .startObject("fielddata").field("format", "paged_bytes").field("loading", randomBoolean() ? "eager" : "lazy").endObject()
                        .endObject()
                        .startObject("name_fst")
                        .field("type", "string")
                        .startObject("fielddata").field("format", "fst").field("loading", randomBoolean() ? "eager" : "lazy").endObject()
                        .endObject()
                        .startObject("name_dv")
                        .field("type", "string")
                        .field("index", "no")
                        .startObject("fielddata").field("format", "doc_values").field("loading", randomBoolean() ? "eager" : "lazy").endObject()
                        .endObject()
                        .startObject("name_paged_mv")
                        .field("type", "string")
                        .startObject("fielddata").field("format", "paged_bytes").field("loading", randomBoolean() ? "eager" : "lazy").endObject()
                        .endObject()
                        .startObject("name_fst_mv")
                        .field("type", "string")
                        .startObject("fielddata").field("format", "fst").field("loading", randomBoolean() ? "eager" : "lazy").endObject()
                        .endObject()
                        .startObject("name_dv_mv")
                        .field("type", "string")
                        .field("index", "no")
                        .startObject("fielddata").field("format", "doc_values").field("loading", randomBoolean() ? "eager" : "lazy").endObject()
                        .endObject()
                        .startObject("filtered")
                        .field("type", "string")
                        .startObject("fielddata").field("format", "fst").field("loading", randomBoolean() ? "eager" : "lazy").startObject("filter")
                        .startObject("regex").field("pattern", "\\d{1,2}").endObject().endObject()
                        .endObject()
                                // only 1 or 2 digits 
                        .endObject()
                        .startObject("filtered_mv")
                        .field("type", "string")
                        .startObject("fielddata").field("format", "fst").field("loading", randomBoolean() ? "eager" : "lazy").startObject("filter")
                        .startObject("regex").field("pattern", "\\d{1,2}").endObject().endObject()
                        .endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type", "" + i).setSource(jsonBuilder().startObject()
                    .field("name_paged", "" + i)
                    .field("name_fst", "" + i)
                    .field("filtered", "" + i)
                    .field("name_paged_mv", "" + i, "" + Math.min(99, i + 1))
                    .field("name_fst_mv", "" + i, "" + Math.min(99, i + 1))
                    .field("filtered_mv", "" + i, "" + Math.min(99, i + 1), "" + (100 + i))
                    .endObject()).execute().actionGet();
        }

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type", "" + (i + 100)).setSource(jsonBuilder().startObject()
                    .field("foo", "" + i)
                    .endObject()).execute().actionGet();
        }

        logger.info("done indexing. refreshing.");
        flushAndRefresh();
        ConcurrentDuel<Facets> duel = new ConcurrentDuel<>(5);
        String[] fieldPostFix = new String[]{"", "_mv"};
        for (final String postfix : fieldPostFix) {
            duel.duel(new ConcurrentDuel.DuelJudge<Facets>() {

                          @Override
                          public void judge(Facets firstRun, Facets result) {
                              for (Facet f : result) {
                                  TermsFacet facet = (TermsFacet) f;
                                  assertThat(facet.getName(), equalTo("termFacet"));
                                  TermsFacet firstRunFacet = (TermsFacet) firstRun.getFacets().get(facet.getName());
                                  assertThat(facet.getEntries().size(), equalTo(firstRunFacet.getEntries().size()));

                                  if ("_mv".equals(postfix)) {
                                      assertThat(facet.getEntries().size(), equalTo(10));
                                      assertThat(facet.getTotalCount(), equalTo(199l));
                                      assertThat(facet.getOtherCount(), equalTo(179l));
                                      assertThat(facet.getMissingCount(), equalTo(10l));
                                  } else {
                                      assertThat(facet.getEntries().size(), equalTo(10));
                                      assertThat(facet.getTotalCount(), equalTo(100l));
                                      assertThat(facet.getOtherCount(), equalTo(90l));
                                      assertThat(facet.getMissingCount(), equalTo(10l));
                                  }
                                  List<? extends Entry> right = facet.getEntries();
                                  List<? extends Entry> left = firstRunFacet.getEntries();

                                  for (int i = 0; i < facet.getEntries().size(); i++) {
                                      assertThat(left.get(i).getTerm(), equalTo(right.get(i).getTerm()));
                                      assertThat(left.get(i).getCount(), equalTo(right.get(i).getCount()));
                                  }
                              }
                          }
                      }, new ConcurrentDuel.DuelExecutor<Facets>() {
                          AtomicInteger count = new AtomicInteger();

                          @Override
                          public Facets run() {
                              final SearchRequestBuilder facetRequest;
                              int incrementAndGet = count.incrementAndGet();
                              final String field;
                              if (incrementAndGet % 100 == 0) {
                                  logger.info("-> run {} searches", incrementAndGet);
                              }
                              switch (incrementAndGet % 2) {
                                  case 1:
                                      field = "filtered" + postfix;
                                      break;
                                  case 0:
                                      field = "name_paged" + postfix;
                                      break;
                                  default:
                                      field = "name_fst" + postfix;
                              }
                              switch (incrementAndGet % 5) {
                                  case 4:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").executionHint("map").field(field).script("\"\" + (Integer.parseInt(term) % 100)").size(10));
                                      break;
                                  case 3:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").field(field).regex("\\d+").size(10));
                                      break;
                                  case 2:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").executionHint("map").field(field).regex("\\d+").script("term").size(10));
                                      break;
                                  case 1:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").field(field).regex("\\d+").script("term").size(10));
                                      break;
                                  case 0:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").field(field).size(10));
                                      break;
                                  default:
                                      facetRequest = client().prepareSearch()
                                              .setQuery(matchAllQuery())
                                              .addFacet(termsFacet("termFacet").executionHint("map").field(field).size(10));
                                      break;
                              }
                              SearchResponse actionGet = facetRequest.execute().actionGet();
                              return actionGet.getFacets();
                          }
                      }, 2000
            );
        }

        duel.close();
    }

    @Test
    public void testSearchFilter() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("tag", "green")
                .endObject()).execute().actionGet();
        flushAndRefresh();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("tag", "blue")
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10))
                    .execute().actionGet();

            assertThat(searchResponse.getHits().hits().length, equalTo(2));
            TermsFacet facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1));
            assertThat(facet.getEntries().get(1).getTerm().string(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1));

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .setPostFilter(termFilter("tag", "blue"))
                    .addFacet(termsFacet("facet1").field("tag").size(10))
                    .execute().actionGet();

            assertThat(searchResponse.getHits().hits().length, equalTo(1));
            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1));
            assertThat(facet.getEntries().get(1).getTerm().string(), anyOf(equalTo("green"), equalTo("blue")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1));
        }
    }

    @Test
    public void testFacetsWithSize0() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .field("lstag", 111)
                .startArray("tag").value("xxx").value("yyy").endArray()
                .startArray("ltag").value(1000l).value(2000l).endArray()
                .endObject()).execute().actionGet();
        flushAndRefresh();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .field("lstag", 111)
                .startArray("tag").value("zzz").value("yyy").endArray()
                .startArray("ltag").value(3000l).value(2000l).endArray()
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setSize(0)
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("stag").size(10))
                    .execute().actionGet();

            assertThat(searchResponse.getHits().hits().length, equalTo(0));

            TermsFacet facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));

            searchResponse = client().prepareSearch()
                    .setSearchType(SearchType.QUERY_AND_FETCH)
                    .setSize(0)
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("stag").size(10))
                    .addFacet(termsFacet("facet2").field("tag").size(10))
                    .execute().actionGet();

            assertThat(searchResponse.getHits().hits().length, equalTo(0));

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));
        }
    }

    @Test
    public void testTermsIndexFacet() throws Exception {
        createIndex("test1");
        createIndex("test2");
        ensureGreen();

        client().prepareIndex("test1", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .endObject()).execute().actionGet();

        client().prepareIndex("test1", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .endObject()).execute().actionGet();

        client().prepareIndex("test2", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .endObject()).execute().actionGet();
        flushAndRefresh();


        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setSize(0)
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("_index").size(10))
                    .execute().actionGet();


            TermsFacet facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("test1"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("test2"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1));
        }

        try {
            client().admin().indices().prepareDelete("test1").execute().actionGet();
            client().admin().indices().prepareDelete("test2").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
    }

    @Test
    public void testFilterFacets() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .startArray("tag").value("xxx").value("yyy").endArray()
                .endObject()).execute().actionGet();
        flushAndRefresh();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .startArray("tag").value("zzz").value("yyy").endArray()
                .endObject()).execute().actionGet();

        refresh();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(filterFacet("facet1").filter(termFilter("stag", "111")))
                    .addFacet(filterFacet("facet2").filter(termFilter("tag", "xxx")))
                    .addFacet(filterFacet("facet3").filter(termFilter("tag", "yyy")))
                    .addFacet(filterFacet("facet4").filter(termFilter("tag", "zzz")))
                    .execute().actionGet();

            FilterFacet facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getCount(), equalTo(2l));

            facet = searchResponse.getFacets().facet("facet2");
            assertThat(facet.getName(), equalTo("facet2"));
            assertThat(facet.getCount(), equalTo(1l));

            facet = searchResponse.getFacets().facet("facet3");
            assertThat(facet.getName(), equalTo("facet3"));
            assertThat(facet.getCount(), equalTo(2l));

            facet = searchResponse.getFacets().facet("facet4");
            assertThat(facet.getName(), equalTo("facet4"));
            assertThat(facet.getCount(), equalTo(1l));
        }
    }

    @Test
    public void testTermsFacetsMissing() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("bstag").field("type", "byte").endObject()
                        .startObject("shstag").field("type", "short").endObject()
                        .startObject("istag").field("type", "integer").endObject()
                        .startObject("lstag").field("type", "long").endObject()
                        .startObject("fstag").field("type", "float").endObject()
                        .startObject("dstag").field("type", "double").endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .field("bstag", 111)
                .field("shstag", 111)
                .field("istag", 111)
                .field("lstag", 111)
                .field("fstag", 111.1f)
                .field("dstag", 111.1)
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("kuku", "kuku")
                .endObject()).execute().actionGet();
        flushAndRefresh();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("stag").size(10))
                    .execute().actionGet();

            TermsFacet facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getMissingCount(), equalTo(1l));
        }
    }

    @Test
    public void testTermsFacetsNoHint() throws Exception {
        testTermsFacets(null);
    }

    @Test
    public void testTermsFacetsMapHint() throws Exception {
        testTermsFacets("map");
    }

    private void testTermsFacets(String executionHint) throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("bstag").field("type", "byte").endObject()
                        .startObject("shstag").field("type", "short").endObject()
                        .startObject("istag").field("type", "integer").endObject()
                        .startObject("lstag").field("type", "long").endObject()
                        .startObject("fstag").field("type", "float").endObject()
                        .startObject("dstag").field("type", "double").endObject()
                        .endObject().endObject().endObject()));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .field("bstag", 111)
                .field("shstag", 111)
                .field("istag", 111)
                .field("lstag", 111)
                .field("fstag", 111.1f)
                .field("dstag", 111.1)
                .startArray("tag").value("xxx").value("yyy").endArray()
                .startArray("ltag").value(1000l).value(2000l).endArray()
                .startArray("dtag").value(1000.1).value(2000.1).endArray()
                .endObject()).execute().actionGet();
        flushAndRefresh();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("stag", "111")
                .field("bstag", 111)
                .field("shstag", 111)
                .field("istag", 111)
                .field("lstag", 111)
                .field("fstag", 111.1f)
                .field("dstag", 111.1)
                .startArray("tag").value("zzz").value("yyy").endArray()
                .startArray("ltag").value(3000l).value(2000l).endArray()
                .startArray("dtag").value(3000.1).value(2000.1).endArray()
                .endObject()).execute().actionGet();

        refresh();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("stag").size(10).executionHint(executionHint))
                    .addFacet(termsFacet("facet2").field("tag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            TermsFacet facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getTotalCount(), equalTo(2l));
            assertThat(facet.getOtherCount(), equalTo(0l));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));

            facet = searchResponse.getFacets().facet("facet2");
            assertThat(facet.getName(), equalTo("facet2"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));

            // Numeric

            searchResponse = client().prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("lstag").size(10).executionHint(executionHint))
                    .addFacet(termsFacet("facet2").field("ltag").size(10).executionHint(executionHint))
                    .addFacet(termsFacet("facet3").field("ltag").size(10).exclude(3000).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet, instanceOf(InternalLongTermsFacet.class));
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));

            facet = searchResponse.getFacets().facet("facet2");
            assertThat(facet, instanceOf(InternalLongTermsFacet.class));
            assertThat(facet.getName(), equalTo("facet2"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("2000"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(1).getTerm().string(), anyOf(equalTo("1000"), equalTo("3000")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1));
            assertThat(facet.getEntries().get(2).getTerm().string(), anyOf(equalTo("1000"), equalTo("3000")));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1));

            facet = searchResponse.getFacets().facet("facet3");
            assertThat(facet, instanceOf(InternalLongTermsFacet.class));
            assertThat(facet.getName(), equalTo("facet3"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("2000"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("1000"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1));

            searchResponse = client().prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("dstag").size(10).executionHint(executionHint))
                    .addFacet(termsFacet("facet2").field("dtag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet, instanceOf(InternalDoubleTermsFacet.class));
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111.1"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));

            facet = searchResponse.getFacets().facet("facet2");
            assertThat(facet, instanceOf(InternalDoubleTermsFacet.class));
            assertThat(facet.getName(), equalTo("facet2"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("2000.1"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(1).getTerm().string(), anyOf(equalTo("1000.1"), equalTo("3000.1")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1));
            assertThat(facet.getEntries().get(2).getTerm().string(), anyOf(equalTo("1000.1"), equalTo("3000.1")));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1));

            searchResponse = client().prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("bstag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));

            searchResponse = client().prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("istag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));

            searchResponse = client().prepareSearch()
                    .setQuery(termQuery("stag", "111"))
                    .addFacet(termsFacet("facet1").field("shstag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));

            // Test Facet Filter

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("stag").size(10).facetFilter(termFilter("tag", "xxx")).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1));

            // now with global
            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("stag").size(10).facetFilter(termFilter("tag", "xxx")).global(true).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1));

            // Test Facet Filter (with a type)

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("type1.stag").size(10).facetFilter(termFilter("tag", "xxx")).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1));

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(1).getTerm().string(), anyOf(equalTo("xxx"), equalTo("zzz")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1));
            assertThat(facet.getEntries().get(2).getTerm().string(), anyOf(equalTo("xxx"), equalTo("zzz")));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1));

            // Bounded Size

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(2).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(1).getTerm().string(), anyOf(equalTo("xxx"), equalTo("zzz")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1));

            // Test Exclude

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).exclude("yyy").executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), anyOf(equalTo("xxx"), equalTo("zzz")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1));
            assertThat(facet.getEntries().get(1).getTerm().string(), anyOf(equalTo("xxx"), equalTo("zzz")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1));

            // Test Order

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).order(TermsFacet.ComparatorType.TERM).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(2).getTerm().string(), equalTo("zzz"));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1));

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).order(TermsFacet.ComparatorType.REVERSE_TERM).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(2).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("zzz"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1));

            // Script

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).script("term + param1").param("param1", "a").order(TermsFacet.ComparatorType.TERM).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("xxxa"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyya"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(2).getTerm().string(), equalTo("zzza"));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1));

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("tag").size(10).script("term == 'xxx' ? false : true").order(TermsFacet.ComparatorType.TERM).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("zzz"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1));

            // Fields Facets

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").fields("stag", "tag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(4));
            assertThat(facet.getEntries().get(0).getTerm().string(), anyOf(equalTo("111"), equalTo("yyy")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(1).getTerm().string(), anyOf(equalTo("111"), equalTo("yyy")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2));
            assertThat(facet.getEntries().get(2).getTerm().string(), anyOf(equalTo("zzz"), equalTo("xxx")));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1));
            assertThat(facet.getEntries().get(3).getTerm().string(), anyOf(equalTo("zzz"), equalTo("xxx")));
            assertThat(facet.getEntries().get(3).getCount(), equalTo(1));

            searchResponse = client().prepareSearch()
                    .setQuery(termQuery("xxx", "yyy")) // don't match anything
                    .addFacet(termsFacet("facet1").field("tag").size(10).allTerms(true).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTerm().string(), anyOf(equalTo("xxx"), equalTo("yyy"), equalTo("zzz")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(0));
            assertThat(facet.getEntries().get(1).getTerm().string(), anyOf(equalTo("xxx"), equalTo("yyy"), equalTo("zzz")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(0));
            assertThat(facet.getEntries().get(2).getTerm().string(), anyOf(equalTo("xxx"), equalTo("yyy"), equalTo("zzz")));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(0));

            searchResponse = client().prepareSearch()
                    .setQuery(termQuery("xxx", "yyy")) // don't match anything
                    .addFacet(termsFacet("facet1").fields("tag", "stag").size(10).allTerms(true).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(4));
            assertThat(facet.getEntries().get(0).getTerm().string(), anyOf(equalTo("xxx"), equalTo("yyy"), equalTo("zzz"), equalTo("111")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(0));
            assertThat(facet.getEntries().get(1).getTerm().string(), anyOf(equalTo("xxx"), equalTo("yyy"), equalTo("zzz"), equalTo("111")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(0));
            assertThat(facet.getEntries().get(2).getTerm().string(), anyOf(equalTo("xxx"), equalTo("yyy"), equalTo("zzz"), equalTo("111")));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(0));
            assertThat(facet.getEntries().get(3).getTerm().string(), anyOf(equalTo("xxx"), equalTo("yyy"), equalTo("zzz"), equalTo("111")));
            assertThat(facet.getEntries().get(3).getCount(), equalTo(0));

            searchResponse = client().prepareSearch()
                    .setQuery(termQuery("xxx", "yyy")) // don't match anything
                    .addFacet(termsFacet("facet1").field("ltag").size(10).allTerms(true).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTermAsNumber().intValue(), anyOf(equalTo(1000), equalTo(2000), equalTo(3000)));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(0));
            assertThat(facet.getEntries().get(1).getTermAsNumber().intValue(), anyOf(equalTo(1000), equalTo(2000), equalTo(3000)));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(0));
            assertThat(facet.getEntries().get(2).getTermAsNumber().intValue(), anyOf(equalTo(1000), equalTo(2000), equalTo(3000)));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(0));

            searchResponse = client().prepareSearch()
                    .setQuery(termQuery("xxx", "yyy")) // don't match anything
                    .addFacet(termsFacet("facet1").field("dtag").size(10).allTerms(true).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTermAsNumber().doubleValue(), anyOf(equalTo(1000.1), equalTo(2000.1), equalTo(3000.1)));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(0));
            assertThat(facet.getEntries().get(1).getTermAsNumber().doubleValue(), anyOf(equalTo(1000.1), equalTo(2000.1), equalTo(3000.1)));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(0));
            assertThat(facet.getEntries().get(2).getTermAsNumber().doubleValue(), anyOf(equalTo(1000.1), equalTo(2000.1), equalTo(3000.1)));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(0));

            // Script Field

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").scriptField("_source.stag").size(10).executionHint(executionHint))
                    .addFacet(termsFacet("facet2").scriptField("_source.tag").size(10).executionHint(executionHint))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getTotalCount(), equalTo(2l));
            assertThat(facet.getOtherCount(), equalTo(0l));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("111"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));

            facet = searchResponse.getFacets().facet("facet2");
            assertThat(facet.getTotalCount(), equalTo(4l));
            assertThat(facet.getOtherCount(), equalTo(0l));
            assertThat(facet.getName(), equalTo("facet2"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2));
        }
    }

    @Test
    public void testTermFacetWithEqualTermDistribution() throws Exception {
        createIndex("test");
        ensureGreen();

        // at the end of the index, we should have 10 of each `bar`, `foo`, and `baz`
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                    .field("text", "foo bar")
                    .endObject()).execute().actionGet();
        }
        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                    .field("text", "bar baz")
                    .endObject()).execute().actionGet();
        }

        for (int i = 0; i < 5; i++) {
            client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                    .field("text", "baz foo")
                    .endObject()).execute().actionGet();
        }
        refresh();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsFacet("facet1").field("text").size(10))
                    .execute().actionGet();

            TermsFacet facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(3));
            for (int j = 0; j < 3; j++) {
                assertThat(facet.getEntries().get(j).getTerm().string(), anyOf(equalTo("foo"), equalTo("bar"), equalTo("baz")));
                assertThat(facet.getEntries().get(j).getCount(), equalTo(10));
            }
        }
    }

    @Test
    public void testStatsFacets() throws Exception {
        // TODO: facet shouldn't fail when faceted field is mapped dynamically
        // We have to specify mapping explicitly because by the time search is performed dynamic mapping might not
        // be propagated to all nodes yet and some facets fail when the facet field is not defined
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("num").field("type", "integer").endObject()
                .startObject("multi_num").field("type", "float").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1)
                .startArray("multi_num").value(1.0).value(2.0f).endArray()
                .endObject()).execute().actionGet();
        flushAndRefresh();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 2)
                .startArray("multi_num").value(3.0).value(4.0f).endArray()
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(statisticalFacet("stats1").field("num"))
                    .addFacet(statisticalFacet("stats2").field("multi_num"))
                    .addFacet(statisticalScriptFacet("stats3").script("doc['num'].value * 2"))
                    .execute().actionGet();

            if (searchResponse.getFailedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.getFailedShards(), equalTo(0));

            StatisticalFacet facet = searchResponse.getFacets().facet("stats1");
            assertThat(facet.getName(), equalTo(facet.getName()));
            assertThat(facet.getCount(), equalTo(2l));
            assertThat(facet.getTotal(), equalTo(3d));
            assertThat(facet.getMin(), equalTo(1d));
            assertThat(facet.getMax(), equalTo(2d));
            assertThat(facet.getMean(), equalTo(1.5d));
            assertThat(facet.getSumOfSquares(), equalTo(5d));

            facet = searchResponse.getFacets().facet("stats2");
            assertThat(facet.getName(), equalTo(facet.getName()));
            assertThat(facet.getCount(), equalTo(4l));
            assertThat(facet.getTotal(), equalTo(10d));
            assertThat(facet.getMin(), equalTo(1d));
            assertThat(facet.getMax(), equalTo(4d));
            assertThat(facet.getMean(), equalTo(2.5d));

            facet = searchResponse.getFacets().facet("stats3");
            assertThat(facet.getName(), equalTo(facet.getName()));
            assertThat(facet.getCount(), equalTo(2l));
            assertThat(facet.getTotal(), equalTo(6d));
            assertThat(facet.getMin(), equalTo(2d));
            assertThat(facet.getMax(), equalTo(4d));
            assertThat(facet.getMean(), equalTo(3d));
            assertThat(facet.getSumOfSquares(), equalTo(20d));

            // test multi field facet
            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(statisticalFacet("stats").fields("num", "multi_num"))
                    .execute().actionGet();


            facet = searchResponse.getFacets().facet("stats");
            assertThat(facet.getName(), equalTo(facet.getName()));
            assertThat(facet.getCount(), equalTo(6l));
            assertThat(facet.getTotal(), equalTo(13d));
            assertThat(facet.getMin(), equalTo(1d));
            assertThat(facet.getMax(), equalTo(4d));
            assertThat(facet.getMean(), equalTo(13d / 6d));
            assertThat(facet.getSumOfSquares(), equalTo(35d));

            // test cross field facet using the same facet name...
            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(statisticalFacet("stats").field("num"))
                    .addFacet(statisticalFacet("stats").field("multi_num"))
                    .execute().actionGet();


            facet = searchResponse.getFacets().facet("stats");
            assertThat(facet.getName(), equalTo(facet.getName()));
            assertThat(facet.getCount(), equalTo(6l));
            assertThat(facet.getTotal(), equalTo(13d));
            assertThat(facet.getMin(), equalTo(1d));
            assertThat(facet.getMax(), equalTo(4d));
            assertThat(facet.getMean(), equalTo(13d / 6d));
            assertThat(facet.getSumOfSquares(), equalTo(35d));
        }
    }

    @Test
    public void testHistoFacetEdge() throws Exception {
        // TODO: Make sure facet doesn't fail in case of dynamic mapping
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("num").field("type", "integer").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 100)
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 200)
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 300)
                .endObject()).execute().actionGet();
        flushAndRefresh();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(histogramFacet("facet1").field("num").valueField("num").interval(100))
                    .execute().actionGet();

            if (searchResponse.getFailedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.getFailedShards(), equalTo(0));

            HistogramFacet facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getKey(), equalTo(100l));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getKey(), equalTo(200l));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(2).getKey(), equalTo(300l));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1l));
        }
    }

    @Test
    public void testHistoFacets() throws Exception {
        // TODO: facet shouldn't fail when faceted field is mapped dynamically
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("num").field("type", "integer").endObject()
                .startObject("multi_num").field("type", "float").endObject()
                .startObject("date").field("type", "date").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1055)
                .field("date", "1970-01-01T00:00:00")
                .startArray("multi_num").value(13.0f).value(23.f).endArray()
                .endObject()).execute().actionGet();
        flushAndRefresh();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1065)
                .field("date", "1970-01-01T00:00:25")
                .startArray("multi_num").value(15.0f).value(31.0f).endArray()
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1175)
                .field("date", "1970-01-01T00:02:00")
                .startArray("multi_num").value(17.0f).value(25.0f).endArray()
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(histogramFacet("stats1").field("num").valueField("num").interval(100))
                    .addFacet(histogramFacet("stats2").field("multi_num").valueField("multi_num").interval(10))
                    .addFacet(histogramFacet("stats3").keyField("num").valueField("multi_num").interval(100))
                    .addFacet(histogramScriptFacet("stats4").keyScript("doc['date'].date.minuteOfHour").valueScript("doc['num'].value"))
                    .addFacet(histogramFacet("stats5").field("date").interval(1, TimeUnit.MINUTES))
                    .addFacet(histogramScriptFacet("stats6").keyField("num").valueScript("doc['num'].value").interval(100))
                    .addFacet(histogramFacet("stats7").field("num").interval(100))
                    .addFacet(histogramScriptFacet("stats8").keyField("num").valueScript("doc.score").interval(100))
                    .execute().actionGet();

            if (searchResponse.getFailedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.getFailedShards(), equalTo(0));

            HistogramFacet facet;

            facet = searchResponse.getFacets().facet("stats1");
            assertThat(facet.getName(), equalTo("stats1"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getKey(), equalTo(1000l));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getMin(), closeTo(1055d, 0.000001));
            assertThat(facet.getEntries().get(0).getMax(), closeTo(1065d, 0.000001));
            assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), equalTo(2120d));
            assertThat(facet.getEntries().get(0).getMean(), equalTo(1060d));
            assertThat(facet.getEntries().get(1).getKey(), equalTo(1100l));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getMin(), closeTo(1175d, 0.000001));
            assertThat(facet.getEntries().get(1).getMax(), closeTo(1175d, 0.000001));
            assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), equalTo(1175d));
            assertThat(facet.getEntries().get(1).getMean(), equalTo(1175d));

            facet = searchResponse.getFacets().facet("stats2");
            assertThat(facet.getName(), equalTo("stats2"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getKey(), equalTo(10l));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(3l));
            assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(3l));
            assertThat(facet.getEntries().get(0).getTotal(), equalTo(45d));
            assertThat(facet.getEntries().get(0).getMean(), equalTo(15d));
            assertThat(facet.getEntries().get(1).getKey(), equalTo(20l));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), equalTo(48d));
            assertThat(facet.getEntries().get(1).getMean(), equalTo(24d));
            assertThat(facet.getEntries().get(2).getKey(), equalTo(30l));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(2).getTotalCount(), equalTo(1l));
            assertThat(facet.getEntries().get(2).getTotal(), equalTo(31d));
            assertThat(facet.getEntries().get(2).getMean(), equalTo(31d));

            facet = searchResponse.getFacets().facet("stats3");
            assertThat(facet.getName(), equalTo("stats3"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getKey(), equalTo(1000l));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(4l));
            assertThat(facet.getEntries().get(0).getTotal(), equalTo(82d));
            assertThat(facet.getEntries().get(0).getMean(), equalTo(20.5d));
            assertThat(facet.getEntries().get(1).getKey(), equalTo(1100l));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), equalTo(42d));
            assertThat(facet.getEntries().get(1).getMean(), equalTo(21d));

            facet = searchResponse.getFacets().facet("stats4");
            assertThat(facet.getName(), equalTo("stats4"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getKey(), equalTo(0l));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), equalTo(2120d));
            assertThat(facet.getEntries().get(0).getMean(), equalTo(1060d));
            assertThat(facet.getEntries().get(1).getKey(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), equalTo(1175d));
            assertThat(facet.getEntries().get(1).getMean(), equalTo(1175d));

            facet = searchResponse.getFacets().facet("stats5");
            assertThat(facet.getName(), equalTo("stats5"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getKey(), equalTo(0l));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getKey(), equalTo(TimeValue.timeValueMinutes(2).millis()));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));

            facet = searchResponse.getFacets().facet("stats6");
            assertThat(facet.getName(), equalTo("stats6"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getKey(), equalTo(1000l));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), equalTo(2120d));
            assertThat(facet.getEntries().get(0).getMean(), equalTo(1060d));
            assertThat(facet.getEntries().get(1).getKey(), equalTo(1100l));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), equalTo(1175d));
            assertThat(facet.getEntries().get(1).getMean(), equalTo(1175d));

            facet = searchResponse.getFacets().facet("stats7");
            assertThat(facet.getName(), equalTo("stats7"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getKey(), equalTo(1000l));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getKey(), equalTo(1100l));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));

            facet = searchResponse.getFacets().facet("stats8");
            assertThat(facet.getName(), equalTo("stats8"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getKey(), equalTo(1000l));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), equalTo(2d));
            assertThat(facet.getEntries().get(0).getMean(), equalTo(1d));
            assertThat(facet.getEntries().get(1).getKey(), equalTo(1100l));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), equalTo(1d));
            assertThat(facet.getEntries().get(1).getMean(), equalTo(1d));

        }
    }

    @Test
    public void testRangeFacets() throws Exception {
        // TODO: facet shouldn't fail when faceted field is mapped dynamically
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("num").field("type", "integer").endObject()
                .startObject("multi_num").field("type", "float").endObject()
                .startObject("value").field("type", "integer").endObject()
                .startObject("multi_value").field("type", "float").endObject()
                .startObject("date").field("type", "date").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1055)
                .field("value", 1)
                .field("date", "1970-01-01T00:00:00")
                .startArray("multi_num").value(13.0f).value(23.f).endArray()
                .startArray("multi_value").value(10).value(11).endArray()
                .endObject()).execute().actionGet();
        flushAndRefresh();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1065)
                .field("value", 2)
                .field("date", "1970-01-01T00:00:25")
                .startArray("multi_num").value(15.0f).value(31.0f).endArray()
                .startArray("multi_value").value(20).value(21).endArray()
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("num", 1175)
                .field("value", 3)
                .field("date", "1970-01-01T00:00:52")
                .startArray("multi_num").value(17.0f).value(25.0f).endArray()
                .startArray("multi_value").value(30).value(31).endArray()
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(rangeFacet("range1").field("num").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170))
                    .addFacet(rangeFacet("range2").keyField("num").valueField("value").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170))
                    .addFacet(rangeFacet("range3").keyField("num").valueField("multi_value").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170))
                    .addFacet(rangeFacet("range4").keyField("multi_num").valueField("value").addUnboundedFrom(16).addRange(10, 26).addUnboundedTo(20))
                    .addFacet(rangeScriptFacet("range5").keyScript("doc['num'].value").valueScript("doc['value'].value").addUnboundedFrom(1056).addRange(1000, 1170).addUnboundedTo(1170))
                    .addFacet(rangeFacet("range6").field("date").addUnboundedFrom("1970-01-01T00:00:26").addRange("1970-01-01T00:00:15", "1970-01-01T00:00:53").addUnboundedTo("1970-01-01T00:00:26"))
                    .execute().actionGet();

            if (searchResponse.getFailedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.getFailedShards(), equalTo(0));

            RangeFacet facet = searchResponse.getFacets().facet("range1");
            assertThat(facet.getName(), equalTo("range1"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTo(), closeTo(1056, 0.000001));
            assertThat(Double.parseDouble(facet.getEntries().get(0).getToAsString()), closeTo(1056, 0.000001));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(1055, 0.000001));
            assertThat(facet.getEntries().get(0).getMin(), closeTo(1055, 0.000001));
            assertThat(facet.getEntries().get(0).getMax(), closeTo(1055, 0.000001));
            assertThat(facet.getEntries().get(1).getFrom(), closeTo(1000, 0.000001));
            assertThat(Double.parseDouble(facet.getEntries().get(1).getFromAsString()), closeTo(1000, 0.000001));
            assertThat(facet.getEntries().get(1).getTo(), closeTo(1170, 0.000001));
            assertThat(Double.parseDouble(facet.getEntries().get(1).getToAsString()), closeTo(1170, 0.000001));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(1055 + 1065, 0.000001));
            assertThat(facet.getEntries().get(1).getMin(), closeTo(1055, 0.000001));
            assertThat(facet.getEntries().get(1).getMax(), closeTo(1065, 0.000001));
            assertThat(facet.getEntries().get(2).getFrom(), closeTo(1170, 0.000001));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(2).getTotalCount(), equalTo(1l));
            assertThat(facet.getEntries().get(2).getTotal(), closeTo(1175, 0.000001));
            assertThat(facet.getEntries().get(2).getMin(), closeTo(1175, 0.000001));
            assertThat(facet.getEntries().get(2).getMax(), closeTo(1175, 0.000001));

            facet = searchResponse.getFacets().facet("range2");
            assertThat(facet.getName(), equalTo("range2"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTo(), closeTo(1056, 0.000001));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(1, 0.000001));
            assertThat(facet.getEntries().get(1).getFrom(), closeTo(1000, 0.000001));
            assertThat(facet.getEntries().get(1).getTo(), closeTo(1170, 0.000001));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(3, 0.000001));
            assertThat(facet.getEntries().get(2).getFrom(), closeTo(1170, 0.000001));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(2).getTotal(), closeTo(3, 0.000001));

            facet = searchResponse.getFacets().facet("range3");
            assertThat(facet.getName(), equalTo("range3"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTo(), closeTo(1056, 0.000001));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(10 + 11, 0.000001));
            assertThat(facet.getEntries().get(0).getMin(), closeTo(10, 0.000001));
            assertThat(facet.getEntries().get(0).getMax(), closeTo(11, 0.000001));
            assertThat(facet.getEntries().get(1).getFrom(), closeTo(1000, 0.000001));
            assertThat(facet.getEntries().get(1).getTo(), closeTo(1170, 0.000001));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(4l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(62, 0.000001));
            assertThat(facet.getEntries().get(1).getMin(), closeTo(10, 0.000001));
            assertThat(facet.getEntries().get(1).getMax(), closeTo(21, 0.000001));
            assertThat(facet.getEntries().get(2).getFrom(), closeTo(1170, 0.000001));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(2).getTotalCount(), equalTo(2l));
            assertThat(facet.getEntries().get(2).getTotal(), closeTo(61, 0.000001));
            assertThat(facet.getEntries().get(2).getMin(), closeTo(30, 0.000001));
            assertThat(facet.getEntries().get(2).getMax(), closeTo(31, 0.000001));

            facet = searchResponse.getFacets().facet("range4");
            assertThat(facet.getName(), equalTo("range4"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTo(), closeTo(16, 0.000001));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(3, 0.000001));
            assertThat(facet.getEntries().get(1).getFrom(), closeTo(10, 0.000001));
            assertThat(facet.getEntries().get(1).getTo(), closeTo(26, 0.000001));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(3l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(1 + 2 + 3, 0.000001));
            assertThat(facet.getEntries().get(2).getFrom(), closeTo(20, 0.000001));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(3l));
            assertThat(facet.getEntries().get(2).getTotal(), closeTo(1 + 2 + 3, 0.000001));

            facet = searchResponse.getFacets().facet("range5");
            assertThat(facet.getName(), equalTo("range5"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTo(), closeTo(1056, 0.000001));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(1, 0.000001));
            assertThat(facet.getEntries().get(1).getFrom(), closeTo(1000, 0.000001));
            assertThat(facet.getEntries().get(1).getTo(), closeTo(1170, 0.000001));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(3, 0.000001));
            assertThat(facet.getEntries().get(2).getFrom(), closeTo(1170, 0.000001));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(2).getTotal(), closeTo(3, 0.000001));

            facet = searchResponse.getFacets().facet("range6");
            assertThat(facet.getName(), equalTo("range6"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getToAsString(), equalTo("1970-01-01T00:00:26"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getFromAsString(), equalTo("1970-01-01T00:00:15"));
            assertThat(facet.getEntries().get(1).getToAsString(), equalTo("1970-01-01T00:00:53"));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(2).getFromAsString(), equalTo("1970-01-01T00:00:26"));
        }
    }

    @Test
    public void testDateHistoFacetsCollectorMode() throws Exception {
        testDateHistoFacets(FacetBuilder.Mode.COLLECTOR);
    }

    @Test
    public void testDateHistoFacetsPostMode() throws Exception {
        testDateHistoFacets(FacetBuilder.Mode.POST);
    }

    private void testDateHistoFacets(FacetBuilder.Mode mode) throws Exception {
        // TODO: facet shouldn't fail when faceted field is mapped dynamically
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("num").field("type", "integer").endObject()
                .startObject("date").field("type", "date").endObject()
                .startObject("date_in_seconds").field("type", "long").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();
        DateTimeFormatter parser = Joda.forPattern("dateOptionalTime").parser();
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T01:01:01")
                .field("num", 1)
                .field("date_in_seconds", TimeUnit.SECONDS.convert(parser.parseMillis("2009-03-05T01:01:01"), TimeUnit.MILLISECONDS))
                .endObject()).execute().actionGet();
        flushAndRefresh();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T04:01:01")
                .field("num", 2)
                .field("date_in_seconds", TimeUnit.SECONDS.convert(parser.parseMillis("2009-03-05T04:01:01"), TimeUnit.MILLISECONDS))
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2009-03-06T01:01:01")
                .field("num", 3)
                .field("date_in_seconds", TimeUnit.SECONDS.convert(parser.parseMillis("2009-03-06T01:01:01"), TimeUnit.MILLISECONDS))
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();


        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(dateHistogramFacet("stats1").field("date").interval("day").mode(mode))
                    .addFacet(dateHistogramFacet("stats2").field("date").interval("day").preZone("-02:00").mode(mode))
                    .addFacet(dateHistogramFacet("stats3").field("date").valueField("num").interval("day").preZone("-02:00").mode(mode))
                    .addFacet(dateHistogramFacet("stats4").field("date").valueScript("doc['num'].value * 2").interval("day").preZone("-02:00").mode(mode))
                    .addFacet(dateHistogramFacet("stats5").field("date").interval("24h").mode(mode))
                    .addFacet(dateHistogramFacet("stats6").field("date").valueField("num").interval("day").preZone("-02:00").postZone("-02:00").mode(mode))
                    .addFacet(dateHistogramFacet("stats7").field("date").interval("quarter").mode(mode))
                    .addFacet(dateHistogramFacet("stats8").field("date_in_seconds").interval("day").factor(1000f).mode(mode))
                    .execute().actionGet();

            if (searchResponse.getFailedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.getFailedShards(), equalTo(0));

            DateHistogramFacet facet = searchResponse.getFacets().facet("stats1");
            assertThat(facet.getName(), equalTo("stats1"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-06")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));

            // time zone causes the dates to shift by 2
            facet = searchResponse.getFacets().facet("stats2");
            assertThat(facet.getName(), equalTo("stats2"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-04")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));

            // time zone causes the dates to shift by 2
            facet = searchResponse.getFacets().facet("stats3");
            assertThat(facet.getName(), equalTo("stats3"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-04")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotal(), equalTo(1d));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), equalTo(5d));

            // time zone causes the dates to shift by 2
            facet = searchResponse.getFacets().facet("stats4");
            assertThat(facet.getName(), equalTo("stats4"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-04")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotal(), equalTo(2d));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), equalTo(10d));

            facet = searchResponse.getFacets().facet("stats5");
            assertThat(facet.getName(), equalTo("stats5"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-06")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));

            facet = searchResponse.getFacets().facet("stats6");
            assertThat(facet.getName(), equalTo("stats6"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-04") - TimeValue.timeValueHours(2).millis()));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotal(), equalTo(1d));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-05") - TimeValue.timeValueHours(2).millis()));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), equalTo(5d));

            facet = searchResponse.getFacets().facet("stats7");
            assertThat(facet.getName(), equalTo("stats7"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-01-01")));

            // check date_histogram on a long field containing date in seconds - we use a factor. 
            facet = searchResponse.getFacets().facet("stats8");
            assertThat(facet.getName(), equalTo("stats8"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-06")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
        }
    }

    @Test
    // https://github.com/elasticsearch/elasticsearch/issues/2141
    public void testDateHistoFacets_preZoneBug() throws Exception {
        // TODO: facet shouldn't fail when faceted field is mapped dynamically
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("num").field("type", "integer").endObject()
                .startObject("date").field("type", "date").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T23:31:01")
                .field("num", 1)
                .endObject()).execute().actionGet();
        flushAndRefresh();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T18:01:01")
                .field("num", 2)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2009-03-05T22:01:01")
                .field("num", 3)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();


        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(dateHistogramFacet("stats1").field("date").interval("day").preZone("+02:00"))
                    .addFacet(dateHistogramFacet("stats2").field("date").valueField("num").interval("day").preZone("+01:30"))
                    .execute().actionGet();

            if (searchResponse.getFailedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.getFailedShards(), equalTo(0));

            // time zone causes the dates to shift by 2:00
            DateHistogramFacet facet = searchResponse.getFacets().facet("stats1");
            assertThat(facet.getName(), equalTo("stats1"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-06")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));

            // time zone causes the dates to shift by 1:30
            facet = searchResponse.getFacets().facet("stats2");
            assertThat(facet.getName(), equalTo("stats2"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2009-03-05")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), equalTo(5d));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2009-03-06")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), equalTo(1d));
        }
    }

    @Test
    public void testTermsStatsFacets() throws Exception {
        // TODO: facet shouldn't fail when faceted field is mapped dynamically
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("field").field("type", "string").endObject()
                .startObject("num").field("type", "integer").endObject()
                .startObject("multi_num").field("type", "float").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("field", "xxx")
                .field("num", 100.0)
                .startArray("multi_num").value(1.0).value(2.0f).endArray()
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("field", "xxx")
                .field("num", 200.0)
                .startArray("multi_num").value(2.0).value(3.0f).endArray()
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("field", "yyy")
                .field("num", 500.0)
                .startArray("multi_num").value(5.0).value(6.0f).endArray()
                .endObject()).execute().actionGet();
        flushAndRefresh();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsStatsFacet("stats1").keyField("field").valueField("num"))
                    .addFacet(termsStatsFacet("stats2").keyField("field").valueField("multi_num"))
                    .addFacet(termsStatsFacet("stats3").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.COUNT))
                    .addFacet(termsStatsFacet("stats4").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.COUNT))
                    .addFacet(termsStatsFacet("stats5").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.TOTAL))
                    .addFacet(termsStatsFacet("stats6").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.TOTAL))

                    .addFacet(termsStatsFacet("stats7").keyField("field").valueField("num").allTerms())
                    .addFacet(termsStatsFacet("stats8").keyField("field").valueField("multi_num").allTerms())
                    .addFacet(termsStatsFacet("stats9").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.COUNT).allTerms())
                    .addFacet(termsStatsFacet("stats10").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.COUNT).allTerms())
                    .addFacet(termsStatsFacet("stats11").keyField("field").valueField("num").order(TermsStatsFacet.ComparatorType.TOTAL).allTerms())
                    .addFacet(termsStatsFacet("stats12").keyField("field").valueField("multi_num").order(TermsStatsFacet.ComparatorType.TOTAL).allTerms())

                    .addFacet(termsStatsFacet("stats13").keyField("field").valueScript("doc['num'].value * 2"))
                    .execute().actionGet();

            if (searchResponse.getFailedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.getFailedShards(), equalTo(0));

            TermsStatsFacet facet = searchResponse.getFacets().facet("stats1");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotalCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getMin(), closeTo(100d, 0.00001d));
            assertThat(facet.getEntries().get(0).getMax(), closeTo(200d, 0.00001d));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(300d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotalCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getMin(), closeTo(500d, 0.00001d));
            assertThat(facet.getEntries().get(1).getMax(), closeTo(500d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(500d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats2");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getMin(), closeTo(1d, 0.00001d));
            assertThat(facet.getEntries().get(0).getMax(), closeTo(3d, 0.00001d));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(8d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getMin(), closeTo(5d, 0.00001d));
            assertThat(facet.getEntries().get(1).getMax(), closeTo(6d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(11d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats3");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(300d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(500d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats4");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(8d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(11d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats5");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(500d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(300d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats6");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(11d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(8d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats7");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(300d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(500d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats8");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(8d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(11d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats9");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(300d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(500d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats10");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(8d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(11d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats11");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(500d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(300d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats12");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(11d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(8d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats13");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("xxx"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(600d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("yyy"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(1000d, 0.00001d));
        }
    }

    @Test
    public void testNumericTermsStatsFacets() throws Exception {
        // TODO: facet shouldn't fail when faceted field is mapped dynamically
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("lField").field("type", "long").endObject()
                .startObject("dField").field("type", "double").endObject()
                .startObject("num").field("type", "float").endObject()
                .startObject("multi_num").field("type", "integer").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("lField", 100l)
                .field("dField", 100.1d)
                .field("num", 100.0)
                .startArray("multi_num").value(1.0).value(2.0f).endArray()
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("lField", 100l)
                .field("dField", 100.1d)
                .field("num", 200.0)
                .startArray("multi_num").value(2.0).value(3.0f).endArray()
                .endObject()).execute().actionGet();
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("lField", 200l)
                .field("dField", 200.2d)
                .field("num", 500.0)
                .startArray("multi_num").value(5.0).value(6.0f).endArray()
                .endObject()).execute().actionGet();
        flushAndRefresh();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsStatsFacet("stats1").keyField("lField").valueField("num"))
                    .addFacet(termsStatsFacet("stats2").keyField("dField").valueField("num"))
                    .execute().actionGet();

            if (searchResponse.getFailedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.getFailedShards(), equalTo(0));

            TermsStatsFacet facet = searchResponse.getFacets().facet("stats1");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("100"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getMin(), closeTo(100d, 0.00001d));
            assertThat(facet.getEntries().get(0).getMax(), closeTo(200d, 0.00001d));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(300d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("200"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getMin(), closeTo(500d, 0.00001d));
            assertThat(facet.getEntries().get(1).getMax(), closeTo(500d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(500d, 0.00001d));

            facet = searchResponse.getFacets().facet("stats2");
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTerm().string(), equalTo("100.1"));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(0).getMin(), closeTo(100d, 0.00001d));
            assertThat(facet.getEntries().get(0).getMax(), closeTo(200d, 0.00001d));
            assertThat(facet.getEntries().get(0).getTotal(), closeTo(300d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTerm().string(), equalTo("200.2"));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getMin(), closeTo(500d, 0.00001d));
            assertThat(facet.getEntries().get(1).getMax(), closeTo(500d, 0.00001d));
            assertThat(facet.getEntries().get(1).getTotal(), closeTo(500d, 0.00001d));
        }
    }

    @Test
    public void testTermsStatsFacets2() throws Exception {
        // TODO: facet shouldn't fail when faceted field is mapped dynamically
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("num").field("type", "float").endObject()
                .endObject().endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        for (int i = 0; i < 20; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("num", i % 10).execute().actionGet();
        }
        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(termsStatsFacet("stats1").keyField("num").valueScript("doc.score").order(TermsStatsFacet.ComparatorType.COUNT))
                    .addFacet(termsStatsFacet("stats2").keyField("num").valueScript("doc.score").order(TermsStatsFacet.ComparatorType.TOTAL))
                    .execute().actionGet();

            if (searchResponse.getFailedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.getFailedShards(), equalTo(0));
            TermsStatsFacet facet = searchResponse.getFacets().facet("stats1");
            assertThat(facet.getEntries().size(), equalTo(10));

            facet = searchResponse.getFacets().facet("stats2");
            assertThat(facet.getEntries().size(), equalTo(10));
        }
    }

    @Test
    public void testQueryFacet() throws Exception {
        createIndex("test");
        ensureGreen();

        for (int i = 0; i < 20; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i)).setSource("num", i % 10).execute().actionGet();
        }
        client().admin().indices().prepareRefresh().execute().actionGet();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(queryFacet("query").query(termQuery("num", 1)))
                    .execute().actionGet();

            QueryFacet facet = searchResponse.getFacets().facet("query");
            assertThat(facet.getCount(), equalTo(2l));

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(queryFacet("query").query(termQuery("num", 1)).global(true))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("query");
            assertThat(facet.getCount(), equalTo(2l));

            searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(queryFacet("query").query(termsQuery("num", new long[]{1, 2})).facetFilter(termFilter("num", 1)).global(true))
                    .execute().actionGet();

            facet = searchResponse.getFacets().facet("query");
            assertThat(facet.getCount(), equalTo(2l));
        }
    }

    @Test // #3479: Null pointer exception for POST mode facets if facet_filter accepts no documents
    public void testFilterFacetWithFacetFilterPostMode() throws IOException {
        createIndex("test");
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("field", "xxx")
                .endObject()).execute().actionGet();

        refresh();

        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(
                            termsFacet("facet1").field("field").mode(FacetBuilder.Mode.POST).facetFilter(termFilter("tag", "doesnotexist"))
                    )
                    .execute().actionGet();

            assertThat(searchResponse.getFailedShards(), equalTo(0));
            TermsFacet facet = searchResponse.getFacets().facet("facet1");
            assertThat(facet.getName(), equalTo("facet1"));
            assertThat(facet.getEntries().size(), equalTo(0));
        }
    }

    private long utcTimeInMillis(String time) {
        return timeInMillis(time, DateTimeZone.UTC);
    }

    private long timeInMillis(String time, DateTimeZone zone) {
        return ISODateTimeFormat.dateOptionalTimeParser().withZone(zone).parseMillis(time);
    }
}
