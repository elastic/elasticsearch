package org.elasticsearch.search.aggregations.reducers.acf;


import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.reducers.ReducerBuilders.acf;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.IsNull.notNullValue;


@ElasticsearchIntegrationTest.SuiteScopeTest
public class AcfTests  extends ElasticsearchIntegrationTest {

    private static final String INTERVAL_FIELD = "interval_field";
    private static final String VALUE_FIELD = "value_field";

    private static int nonRandomSize = 50;
    private static double[] linear = new double[nonRandomSize];
    private static double[] cyclic1 = new double[nonRandomSize];

    private static double[] linearACF = new double[nonRandomSize];
    private static double[] linearPaddedACF;

    private static double[] cyclic1ACF = new double[nonRandomSize];
    private static double[] cyclic1PaddedACF;


    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");

        for (int i = 0; i < nonRandomSize; i++) {
            linear[i] = i;
        }

        linearACF = circularACF(linear, true);
        linearPaddedACF = linearACF(linear, true);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < nonRandomSize; i++) {
            builders.add(client().prepareIndex("idx", "linear").setSource(newDocBuilder(i, linear[i])));
        }

        indexRandom(true, builders);




        // period of 10
        for (int i = 0; i < nonRandomSize; i++) {
            cyclic1[i] = i % 10;
        }

        cyclic1ACF = circularACF(cyclic1, true);
        cyclic1PaddedACF = linearACF(cyclic1, true);

        //padded = linearACF(Arrays.copyOf(cyclic1, pad << 1), true);
        //cyclic1PaddedACF = Arrays.copyOf(padded, nonRandomSize);

        builders = new ArrayList<>();
        for (int i = 0; i < nonRandomSize; i++) {
            builders.add(client().prepareIndex("idx", "cyclic").setSource(newDocBuilder(i, cyclic1[i])));
        }

        indexRandom(true, builders);

        ensureSearchable();

    }

    private XContentBuilder newDocBuilder(int intervalField, double valueField) throws IOException {
        return jsonBuilder().startObject()
                .field(INTERVAL_FIELD, intervalField)
                .field(VALUE_FIELD, valueField)
                .endObject();
    }

    private double[] linearACF(double[] values, boolean normalize) {
        int length = values.length;
        double[] acValues = new double[length];

        for (int h = 0; h < length; h++) {
            for (int i = 0; i < length - h; i++) {
                acValues[h] += values[i] * values[i+h] ;
            }
            acValues[h] /= length - h;
        }

        if (normalize) {
            for (int i = 1; i < length; i++) {
                acValues[i] /= acValues[0];
            }
            acValues[0] = 1;
        }


        return acValues;
    }

    private double[] circularACF(double[] values, boolean normalize) {

        double[] acValues = new double[values.length];
        for (int j = 0; j < values.length; j++) {
            for (int i = 0; i < values.length; i++) {
                acValues[j] += values[i] * values[(values.length + i - j) % values.length];
            }
        }

        if (normalize) {
            for (int i = 1; i < acValues.length; i++) {
                acValues[i] /= acValues[0];
            }
            acValues[0] = 1;
        }


        return acValues;
    }

    @Test
    public void simpleLinearCircular() {
        int interval = 1;
        int window = nonRandomSize;

        SearchResponse response = client()
                .prepareSearch("idx").setTypes("linear")
                .addAggregation(histogram("histo").field(INTERVAL_FIELD).interval(interval).minDocCount(0).extendedBounds(0L, (long) (nonRandomSize - 1))
                        .subAggregation(max("the_max").field(VALUE_FIELD)))
                .addAggregation(acf("acf").setBucketsPaths("histo>the_max").normalize(true).zeroPad(false).zeroMean(false).window(window))
                .execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<InternalHistogram.Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));

        InternalAcfValues acf = response.getAggregations().get("acf");
        assertThat(acf, notNullValue());
        assertThat(acf.getName(), equalTo("acf"));

        double[] acfValues = acf.values();
        assertThat(acfValues.length, equalTo(linearACF.length));

        for (int i = 0; i < linearACF.length; i++) {
            assertWithinError(i, acfValues[i], linearACF[i], 0.01 * (i + 1));    // multiply by i because error creeps up over lags
        }
    }

    @Test
    public void simpleLinearPaddedCircular() {
        int interval = 1;
        int window = nonRandomSize;

        SearchResponse response = client()
                .prepareSearch("idx").setTypes("linear")
                .addAggregation(histogram("histo").field(INTERVAL_FIELD).interval(interval).minDocCount(0).extendedBounds(0L, (long)(nonRandomSize - 1))
                        .subAggregation(max("the_max").field(VALUE_FIELD)))
                .addAggregation(acf("acf").setBucketsPaths("histo>the_max").normalize(true).zeroPad(true).zeroMean(false).window(window))
                .execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<InternalHistogram.Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));

        InternalAcfValues acf = response.getAggregations().get("acf");
        assertThat(acf, notNullValue());
        assertThat(acf.getName(), equalTo("acf"));

        double[] acfValues = acf.values();
        assertThat(acfValues.length, equalTo(linearPaddedACF.length));

        for (int i = 0; i < linearPaddedACF.length; i++) {
            assertWithinError(i, acfValues[i], linearPaddedACF[i], 0.05 * (i + 2));    // multiply by i because error creeps up over lags
        }
    }


    @Test
    public void simpleCyclic1CircularACF() {
        int interval = 1;
        int window = nonRandomSize;

        SearchResponse response = client()
                .prepareSearch("idx").setTypes("cyclic")
                .addAggregation(histogram("histo").field(INTERVAL_FIELD).interval(interval).minDocCount(0).extendedBounds(0L, (long)(nonRandomSize - 1))
                        .subAggregation(max("the_max").field(VALUE_FIELD)))
                .addAggregation(acf("acf").setBucketsPaths("histo>the_max").normalize(true).zeroPad(false).zeroMean(false).window(window))
                .execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<InternalHistogram.Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));

        InternalAcfValues acf = response.getAggregations().get("acf");
        assertThat(acf, notNullValue());
        assertThat(acf.getName(), equalTo("acf"));

        double[] acfValues = acf.values();
        assertThat(acfValues.length, equalTo(cyclic1ACF.length));

        for (int i = 0; i < cyclic1ACF.length; i++) {
            assertWithinError(i, acfValues[i], cyclic1ACF[i], 0.01);
        }
    }

    @Test
    public void simpleCyclic1PaddedCircular() {
        int interval = 1;
        int window = nonRandomSize;

        SearchResponse response = client()
                .prepareSearch("idx").setTypes("cyclic")
                .addAggregation(histogram("histo").field(INTERVAL_FIELD).interval(interval).minDocCount(0).extendedBounds(0L, (long)(nonRandomSize - 1))
                        .subAggregation(max("the_max").field(VALUE_FIELD)))
                .addAggregation(acf("acf").setBucketsPaths("histo>the_max").normalize(true).zeroPad(true).zeroMean(false).window(window))
                .execute().actionGet();

        assertSearchResponse(response);

        InternalHistogram<InternalHistogram.Bucket> histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));

        InternalAcfValues acf = response.getAggregations().get("acf");
        assertThat(acf, notNullValue());
        assertThat(acf.getName(), equalTo("acf"));

        double[] acfValues = acf.values();
        assertThat(acfValues.length, equalTo(cyclic1PaddedACF.length));

        for (int i = 0; i < cyclic1PaddedACF.length; i++) {
            assertWithinError(i, acfValues[i], cyclic1PaddedACF[i], 0.01);
        }
    }

    private void assertWithinError(int i, double expected, double actual, double errorMargin) {

        // Just to avoid stupid divide-by-zero situations
        expected += 0.000000001;
        actual += 0.000000001;

        double actualError = Math.abs((actual - expected ) / actual);
        assertThat("Difference between expected and actual autocorrelation was not within error bounds at position [" + i +
                "] with (", actualError, lessThan(errorMargin));
    }
}
