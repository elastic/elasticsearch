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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.Range.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geoDistance;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class GeoDistanceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class); // uses index.version.created
    }

    private Version version = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0,
            Version.CURRENT);

    private IndexRequestBuilder indexCity(String idx, String name, String... latLons) throws Exception {
        XContentBuilder source = jsonBuilder().startObject().field("city", name);
        source.startArray("location");
        for (int i = 0; i < latLons.length; i++) {
            source.value(latLons[i]);
        }
        source.endArray();
        source = source.endObject();
        return client().prepareIndex(idx, "type").setSource(source);
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        prepareCreate("idx").setSettings(settings)
                .addMapping("type", "location", "type=geo_point", "city", "type=keyword")
                .execute().actionGet();

        prepareCreate("idx-multi")
                .addMapping("type", "location", "type=geo_point", "city", "type=keyword")
                .execute().actionGet();

        createIndex("idx_unmapped");

        List<IndexRequestBuilder> cities = new ArrayList<>();
        cities.addAll(Arrays.asList(
                // below 500km
                indexCity("idx", "utrecht", "52.0945, 5.116"),
                indexCity("idx", "haarlem", "52.3890, 4.637"),
                // above 500km, below 1000km
                indexCity("idx", "berlin", "52.540, 13.409"),
                indexCity("idx", "prague", "50.097679, 14.441314"),
                // above 1000km
                indexCity("idx", "tel-aviv", "32.0741, 34.777")));

        // random cities with no location
        for (String cityName : Arrays.asList("london", "singapour", "tokyo", "milan")) {
            if (randomBoolean()) {
                cities.add(indexCity("idx", cityName));
            }
        }
        indexRandom(true, cities);

        cities.clear();
        cities.addAll(Arrays.asList(
                indexCity("idx-multi", "city1", "52.3890, 4.637", "50.097679,14.441314"), // first point is within the ~17.5km, the second is ~710km
                indexCity("idx-multi", "city2", "52.540, 13.409", "52.0945, 5.116"), // first point is ~576km, the second is within the ~35km
                indexCity("idx-multi", "city3", "32.0741, 34.777"))); // above 1000km

        // random cities with no location
        for (String cityName : Arrays.asList("london", "singapour", "tokyo", "milan")) {
            if (randomBoolean() || true) {
                cities.add(indexCity("idx-multi", cityName));
            }
        }
        indexRandom(true, cities);
        prepareCreate("empty_bucket_idx")
                .addMapping("type", "value", "type=integer", "location", "type=geo_point").execute().actionGet();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i * 2)
                    .field("location", "52.0945, 5.116")
                    .endObject()));
        }
        indexRandom(true, builders.toArray(new IndexRequestBuilder[builders.size()]));
        ensureSearchable();
    }

    public void testSimple() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(geoDistance("amsterdam_rings", new GeoPoint(52.3760, 4.894))
                        .field("location")
                        .unit(DistanceUnit.KILOMETERS)
                        .addUnboundedTo(500)
                        .addRange(500, 1000)
                        .addUnboundedFrom(1000))
                        .execute().actionGet();

        assertSearchResponse(response);


        Range geoDist = response.getAggregations().get("amsterdam_rings");
        assertThat(geoDist, notNullValue());
        assertThat(geoDist.getName(), equalTo("amsterdam_rings"));
        List<? extends Bucket> buckets = geoDist.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-500.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(0.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(500.0));
        assertThat(bucket.getFromAsString(), equalTo("0.0"));
        assertThat(bucket.getToAsString(), equalTo("500.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("500.0-1000.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(500.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(1000.0));
        assertThat(bucket.getFromAsString(), equalTo("500.0"));
        assertThat(bucket.getToAsString(), equalTo("1000.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("1000.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(1000.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("1000.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(1L));
    }

    public void testSimpleWithCustomKeys() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(geoDistance("amsterdam_rings", new GeoPoint(52.3760, 4.894))
                        .field("location")
                        .unit(DistanceUnit.KILOMETERS)
                        .addUnboundedTo("ring1", 500)
                        .addRange("ring2", 500, 1000)
                        .addUnboundedFrom("ring3", 1000))
                .execute().actionGet();

        assertSearchResponse(response);


        Range geoDist = response.getAggregations().get("amsterdam_rings");
        assertThat(geoDist, notNullValue());
        assertThat(geoDist.getName(), equalTo("amsterdam_rings"));
        List<? extends Bucket> buckets = geoDist.getBuckets();
        assertThat(buckets.size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("ring1"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(0.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(500.0));
        assertThat(bucket.getFromAsString(), equalTo("0.0"));
        assertThat(bucket.getToAsString(), equalTo("500.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("ring2"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(500.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(1000.0));
        assertThat(bucket.getFromAsString(), equalTo("500.0"));
        assertThat(bucket.getToAsString(), equalTo("1000.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("ring3"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(1000.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("1000.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(1L));
    }

    public void testUnmapped() throws Exception {
        client().admin().cluster().prepareHealth("idx_unmapped").setWaitForYellowStatus().execute().actionGet();

        SearchResponse response = client().prepareSearch("idx_unmapped")
                .addAggregation(geoDistance("amsterdam_rings", new GeoPoint(52.3760, 4.894))
                        .field("location")
                        .unit(DistanceUnit.KILOMETERS)
                        .addUnboundedTo(500)
                        .addRange(500, 1000)
                        .addUnboundedFrom(1000))
                .execute().actionGet();

        assertSearchResponse(response);


        Range geoDist = response.getAggregations().get("amsterdam_rings");
        assertThat(geoDist, notNullValue());
        assertThat(geoDist.getName(), equalTo("amsterdam_rings"));
        List<? extends Bucket> buckets = geoDist.getBuckets();
        assertThat(geoDist.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-500.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(0.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(500.0));
        assertThat(bucket.getFromAsString(), equalTo("0.0"));
        assertThat(bucket.getToAsString(), equalTo("500.0"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("500.0-1000.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(500.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(1000.0));
        assertThat(bucket.getFromAsString(), equalTo("500.0"));
        assertThat(bucket.getToAsString(), equalTo("1000.0"));
        assertThat(bucket.getDocCount(), equalTo(0L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("1000.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(1000.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("1000.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(0L));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
                .addAggregation(geoDistance("amsterdam_rings", new GeoPoint(52.3760, 4.894))
                        .field("location")
                        .unit(DistanceUnit.KILOMETERS)
                        .addUnboundedTo(500)
                        .addRange(500, 1000)
                        .addUnboundedFrom(1000))
                .execute().actionGet();

        assertSearchResponse(response);


        Range geoDist = response.getAggregations().get("amsterdam_rings");
        assertThat(geoDist, notNullValue());
        assertThat(geoDist.getName(), equalTo("amsterdam_rings"));
        List<? extends Bucket> buckets = geoDist.getBuckets();
        assertThat(geoDist.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-500.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(0.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(500.0));
        assertThat(bucket.getFromAsString(), equalTo("0.0"));
        assertThat(bucket.getToAsString(), equalTo("500.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("500.0-1000.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(500.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(1000.0));
        assertThat(bucket.getFromAsString(), equalTo("500.0"));
        assertThat(bucket.getToAsString(), equalTo("1000.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("1000.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(1000.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("1000.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(1L));
    }

    public void testWithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(geoDistance("amsterdam_rings", new GeoPoint(52.3760, 4.894))
                        .field("location")
                        .unit(DistanceUnit.KILOMETERS)
                        .addUnboundedTo(500)
                        .addRange(500, 1000)
                        .addUnboundedFrom(1000)
                        .subAggregation(terms("cities").field("city")
                                .collectMode(randomFrom(SubAggCollectionMode.values()))))
                .execute().actionGet();

        assertSearchResponse(response);


        Range geoDist = response.getAggregations().get("amsterdam_rings");
        assertThat(geoDist, notNullValue());
        assertThat(geoDist.getName(), equalTo("amsterdam_rings"));
        List<? extends Bucket> buckets = geoDist.getBuckets();
        assertThat(geoDist.getBuckets().size(), equalTo(3));
        assertThat(((InternalAggregation)geoDist).getProperty("_bucket_count"), equalTo(3));
        Object[] propertiesKeys = (Object[]) ((InternalAggregation)geoDist).getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) ((InternalAggregation)geoDist).getProperty("_count");
        Object[] propertiesCities = (Object[]) ((InternalAggregation)geoDist).getProperty("cities");

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-500.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(0.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(500.0));
        assertThat(bucket.getFromAsString(), equalTo("0.0"));
        assertThat(bucket.getToAsString(), equalTo("500.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        Terms cities = bucket.getAggregations().get("cities");
        assertThat(cities, Matchers.notNullValue());
        Set<String> names = new HashSet<>();
        for (Terms.Bucket city : cities.getBuckets()) {
            names.add(city.getKeyAsString());
        }
        assertThat(names.contains("utrecht") && names.contains("haarlem"), is(true));
        assertThat((String) propertiesKeys[0], equalTo("*-500.0"));
        assertThat((long) propertiesDocCounts[0], equalTo(2L));
        assertThat((Terms) propertiesCities[0], sameInstance(cities));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("500.0-1000.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(500.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(1000.0));
        assertThat(bucket.getFromAsString(), equalTo("500.0"));
        assertThat(bucket.getToAsString(), equalTo("1000.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        cities = bucket.getAggregations().get("cities");
        assertThat(cities, Matchers.notNullValue());
        names = new HashSet<>();
        for (Terms.Bucket city : cities.getBuckets()) {
            names.add(city.getKeyAsString());
        }
        assertThat(names.contains("berlin") && names.contains("prague"), is(true));
        assertThat((String) propertiesKeys[1], equalTo("500.0-1000.0"));
        assertThat((long) propertiesDocCounts[1], equalTo(2L));
        assertThat((Terms) propertiesCities[1], sameInstance(cities));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("1000.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(1000.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("1000.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(1L));
        assertThat(bucket.getAggregations().asList().isEmpty(), is(false));
        cities = bucket.getAggregations().get("cities");
        assertThat(cities, Matchers.notNullValue());
        names = new HashSet<>();
        for (Terms.Bucket city : cities.getBuckets()) {
            names.add(city.getKeyAsString());
        }
        assertThat(names.contains("tel-aviv"), is(true));
        assertThat((String) propertiesKeys[2], equalTo("1000.0-*"));
        assertThat((long) propertiesDocCounts[2], equalTo(1L));
        assertThat((Terms) propertiesCities[2], sameInstance(cities));
    }

    public void testEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1L).minDocCount(0)
                        .subAggregation(geoDistance("geo_dist", new GeoPoint(52.3760, 4.894)).field("location").addRange("0-100", 0.0, 100.0)))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Range geoDistance = bucket.getAggregations().get("geo_dist");
        // TODO: use diamond once JI-9019884 is fixed
        List<Range.Bucket> buckets = new ArrayList<>(geoDistance.getBuckets());
        assertThat(geoDistance, Matchers.notNullValue());
        assertThat(geoDistance.getName(), equalTo("geo_dist"));
        assertThat(buckets.size(), is(1));
        assertThat((String) buckets.get(0).getKey(), equalTo("0-100"));
        assertThat(((Number) buckets.get(0).getFrom()).doubleValue(), equalTo(0.0));
        assertThat(((Number) buckets.get(0).getTo()).doubleValue(), equalTo(100.0));
        assertThat(buckets.get(0).getFromAsString(), equalTo("0.0"));
        assertThat(buckets.get(0).getToAsString(), equalTo("100.0"));
        assertThat(buckets.get(0).getDocCount(), equalTo(0L));
    }

    public void testNoRangesInQuery()  {
        try {
            client().prepareSearch("idx")
                .addAggregation(geoDistance("geo_dist", new GeoPoint(52.3760, 4.894)))
                .execute().actionGet();
            fail();
        } catch (SearchPhaseExecutionException spee){
            Throwable rootCause = spee.getCause().getCause();
            assertThat(rootCause, instanceOf(IllegalArgumentException.class));
            assertEquals(rootCause.getMessage(), "No [ranges] specified for the [geo_dist] aggregation");
        }
    }

    public void testMultiValues() throws Exception {
        SearchResponse response = client().prepareSearch("idx-multi")
                .addAggregation(geoDistance("amsterdam_rings", new GeoPoint(52.3760, 4.894))
                        .field("location")
                        .unit(DistanceUnit.KILOMETERS)
                        .distanceType(org.elasticsearch.common.geo.GeoDistance.ARC)
                        .addUnboundedTo(500)
                        .addRange(500, 1000)
                        .addUnboundedFrom(1000))
                .execute().actionGet();

        assertSearchResponse(response);

        Range geoDist = response.getAggregations().get("amsterdam_rings");
        assertThat(geoDist, notNullValue());
        assertThat(geoDist.getName(), equalTo("amsterdam_rings"));
        List<? extends Bucket> buckets = geoDist.getBuckets();
        assertThat(geoDist.getBuckets().size(), equalTo(3));

        Range.Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("*-500.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(0.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(500.0));
        assertThat(bucket.getFromAsString(), equalTo("0.0"));
        assertThat(bucket.getToAsString(), equalTo("500.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("500.0-1000.0"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(500.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(1000.0));
        assertThat(bucket.getFromAsString(), equalTo("500.0"));
        assertThat(bucket.getToAsString(), equalTo("1000.0"));
        assertThat(bucket.getDocCount(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat((String) bucket.getKey(), equalTo("1000.0-*"));
        assertThat(((Number) bucket.getFrom()).doubleValue(), equalTo(1000.0));
        assertThat(((Number) bucket.getTo()).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(bucket.getFromAsString(), equalTo("1000.0"));
        assertThat(bucket.getToAsString(), nullValue());
        assertThat(bucket.getDocCount(), equalTo(1L));
    }



}
