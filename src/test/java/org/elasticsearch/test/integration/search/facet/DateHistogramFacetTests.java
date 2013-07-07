package org.elasticsearch.test.integration.search.facet;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.numericRangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.facet.FacetBuilders.dateHistogramFacet;
import static org.elasticsearch.test.integration.search.facet.SimpleFacetsTests.timeInMillis;
import static org.elasticsearch.test.integration.search.facet.SimpleFacetsTests.utcTimeInMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DateHistogramFacetTests extends AbstractSharedClusterTest {
    @Override
    public Settings getSettings() {
        return randomSettingsBuilder()
                .put("index.number_of_shards", numberOfShards())
                .put("index.number_of_replicas", 0)
                .build();
    }

    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected int numberOfNodes() {
        return 1;
    }

    protected int numberOfRuns() {
        return 5;
    }

    @Test
    public void testWeeklyOffsetDateHistoFacetsCollectorMode() throws Exception {
        testWeeklyOffsetDateHistoFacets(FacetBuilder.Mode.COLLECTOR);
    }

    @Test
    public void testWeeklyOffsetDateHistoFacetsPostMode() throws Exception {
        testWeeklyOffsetDateHistoFacets(FacetBuilder.Mode.POST);
    }

    private void testWeeklyOffsetDateHistoFacets(FacetBuilder.Mode mode) throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("num").field("type", "integer").endObject()
                .startObject("date").field("type", "date").endObject()
                .endObject().endObject().endObject().string();
        prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2013-01-28T23:01:01+02:00")
                .field("num", 0)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2013-03-30T23:01:01+02:00")
                .field("num", 1)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2013-03-31T01:01:01+02:00")
                .field("num", 2)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2013-03-31T16:01:01+02:00")
                .field("num", 3)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareFlush().setRefresh(true).execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2013-03-27T04:01:01+02:00")
                .field("num", 4)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject()
                .field("date", "2013-04-03T03:01:01+02:00")
                .field("num", 5)
                .endObject()).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();


        for (int i = 0; i < numberOfRuns(); i++) {
            SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addFacet(dateHistogramFacet("stats1").field("date").interval("day").mode(mode))
                    .addFacet(dateHistogramFacet("stats2").field("date").interval("day").preZone("+02:00").mode(mode))
                    .addFacet(dateHistogramFacet("stats_weekly").field("date").interval("week").preZone("+02:00")
                            .preOffset(new TimeValue(-1, TimeUnit.DAYS)).postOffset(new TimeValue(-1, TimeUnit.DAYS))
                            .mode(mode))
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
            assertThat(facet.getEntries().size(), equalTo(5));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2013-01-28")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2013-03-27")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(2).getTime(), equalTo(utcTimeInMillis("2013-03-30")));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(3).getTime(), equalTo(utcTimeInMillis("2013-03-31")));
            assertThat(facet.getEntries().get(3).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(4).getTime(), equalTo(utcTimeInMillis("2013-04-03")));
            assertThat(facet.getEntries().get(4).getCount(), equalTo(1l));

            // time zone causes the dates to shift by 2
            facet = searchResponse.getFacets().facet("stats2");
            assertThat(facet.getName(), equalTo("stats2"));
            assertThat(facet.getEntries().size(), equalTo(5));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2013-01-28")));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2013-03-27")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(2).getTime(), equalTo(utcTimeInMillis("2013-03-30")));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(3).getTime(), equalTo(utcTimeInMillis("2013-03-31")));
            assertThat(facet.getEntries().get(3).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(4).getTime(), equalTo(utcTimeInMillis("2013-04-03")));
            assertThat(facet.getEntries().get(4).getCount(), equalTo(1l));

            // For us, a week start and ends on Sunday
            // We expect to have facets for 3 weeks returned: Jan 27th (1), March 24 (2) and March 31 (3)
            // However, we get the weeks and counts messed up
            facet = searchResponse.getFacets().facet("stats_weekly");
            assertThat(facet.getName(), equalTo("stats_weekly"));
            assertThat(facet.getEntries().size(), equalTo(3));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(utcTimeInMillis("2013-01-27"))); // FAILS with incorrect date
            assertThat(facet.getEntries().get(0).getCount(), equalTo(1l));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(utcTimeInMillis("2013-03-24")));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(2l)); // FAILS with incorrect count
            assertThat(facet.getEntries().get(2).getTime(), equalTo(utcTimeInMillis("2013-03-31")));
            assertThat(facet.getEntries().get(2).getCount(), equalTo(3l)); // FAILS with incorrect count

            // Now issue the same search request with date range filters
            final FilterBuilder dateFilter = numericRangeFilter("date")
                    .from(timeInMillis("2013-03-31", DateTimeZone.forOffsetHours(2))).includeLower(true)
                    .to(timeInMillis("2013-04-06", DateTimeZone.forOffsetHours(2))).includeUpper(true);

            searchResponse = client().prepareSearch()
                    .setQuery(filteredQuery(matchAllQuery(), dateFilter))
                    .addFacet(dateHistogramFacet("stats2").field("date").interval("day").preZone("+02:00").postZone("+02:00").mode(mode).facetFilter(dateFilter))
                    .addFacet(dateHistogramFacet("stats_weekly").field("date").interval("week").preZone("+02:00").postZone("+02:00") // Israeli week
                            .preOffset(new TimeValue(-1, TimeUnit.DAYS)).postOffset(new TimeValue(-1, TimeUnit.DAYS)).mode(mode).facetFilter(dateFilter))
                    .execute().actionGet();

            if (searchResponse.getFailedShards() > 0) {
                logger.warn("Failed shards:");
                for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                    logger.warn("-> {}", shardSearchFailure);
                }
            }
            assertThat(searchResponse.getFailedShards(), equalTo(0));

            // time zone causes the dates to shift by 2
            facet = searchResponse.getFacets().facet("stats2");
            assertThat(facet.getName(), equalTo("stats2"));
            assertThat(facet.getEntries().size(), equalTo(2));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(timeInMillis("2013-03-31", DateTimeZone.forOffsetHours(-2))));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(2l));
            assertThat(facet.getEntries().get(1).getTime(), equalTo(timeInMillis("2013-04-03", DateTimeZone.forOffsetHours(-2))));
            assertThat(facet.getEntries().get(1).getCount(), equalTo(1l));

            // FAILS the week of the 24th is added to the response, even though pre offset and zone were specified
            facet = searchResponse.getFacets().facet("stats_weekly");
            assertThat(facet.getName(), equalTo("stats_weekly"));
            assertThat(facet.getEntries().size(), equalTo(1));
            assertThat(facet.getEntries().get(0).getTime(), equalTo(timeInMillis("2013-03-31", DateTimeZone.forOffsetHours(-2))));
            assertThat(facet.getEntries().get(0).getCount(), equalTo(3l));
        }
    }
}
