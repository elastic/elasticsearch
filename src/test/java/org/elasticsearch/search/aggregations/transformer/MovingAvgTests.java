package org.elasticsearch.search.aggregations.transformer;


import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.transformer.moving.avg.MovingAvg;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.derivative;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.movingAvg;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class MovingAvgTests extends ElasticsearchIntegrationTest {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";
    private static final String MULTI_VALUED_FIELD_NAME = "l_values";

    static int numDocs;
    static int interval;
    static int numValueBuckets, numValuesBuckets;
    static int numFirstDerivValueBuckets, numFirstDerivValuesBuckets;
    static long[] valueCounts, valuesCounts;
    static long[] firstDerivValueCounts, firstDerivValuesCounts;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        numDocs = randomIntBetween(100, 200);
        interval = randomIntBetween(2, 5);

        numValueBuckets = numDocs / interval + 1;
        valueCounts = new long[numValueBuckets];
        for (int i = 0; i < numDocs; i++) {
            final int bucket = (i + 1) / interval;
            valueCounts[bucket]++;
        }

        numValuesBuckets = (numDocs + 1) / interval + 1;
        valuesCounts = new long[numValuesBuckets];
        for (int i = 0; i < numDocs; i++) {
            final int bucket1 = (i + 1) / interval;
            final int bucket2 = (i + 2) / interval;
            valuesCounts[bucket1]++;
            if (bucket1 != bucket2) {
                valuesCounts[bucket2]++;
            }
        }

        numFirstDerivValueBuckets = numValueBuckets - 1;
        firstDerivValueCounts = new long[numFirstDerivValueBuckets];
        long lastValueCount = -1;
        for (int i = 0; i < numValueBuckets; i++) {
            long thisValue = valueCounts[i];
            if (lastValueCount != -1) {
                long diff = thisValue - lastValueCount;
                firstDerivValueCounts[i - 1] = diff;
            }
            lastValueCount = thisValue;
        }

        numFirstDerivValuesBuckets = numValuesBuckets - 1;
        firstDerivValuesCounts = new long[numFirstDerivValuesBuckets];
        long lastValuesCount = -1;
        for (int i = 0; i < numValuesBuckets; i++) {
            long thisValue = valuesCounts[i];
            if (lastValuesCount != -1) {
                long diff = thisValue - lastValuesCount;
                firstDerivValuesCounts[i - 1] = diff;
            }
            lastValuesCount = thisValue;
        }

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx", "type").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i + 1).startArray(MULTI_VALUED_FIELD_NAME).value(i + 1)
                            .value(i + 2).endArray().field("tag", "tag" + i).endObject()));
        }

        assertAcked(prepareCreate("empty_bucket_idx").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=integer"));
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + i).setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i * 2).endObject()));
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    @Test
    public void singleValuedField() {

        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(movingAvg("movavg").subAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)))
                .execute().actionGet();

        assertSearchResponse(response);

        MovingAvg movAvg = response.getAggregations().get("movavg");
        assertThat(movAvg, notNullValue());
        assertThat(movAvg.getName(), equalTo("movavg"));
        List<? extends Histogram.Bucket> buckets = movAvg.getBuckets();
        assertThat(buckets.size(), equalTo(numFirstDerivValueBuckets));

        for (int i = 0; i < numFirstDerivValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsString(), equalTo(String.valueOf(i * interval)));
            //assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            //assertThat(bucket.getDocCount(), equalTo(0l));

        }
    }
}


