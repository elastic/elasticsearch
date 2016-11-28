/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.process.autodetect.output;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.quantiles.Quantiles;
import org.elasticsearch.xpack.prelert.job.results.AutodetectResult;
import org.elasticsearch.xpack.prelert.job.results.Bucket;
import org.elasticsearch.xpack.prelert.job.results.BucketInfluencer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for parsing the JSON output of autodetect
 */
public class AutodetectResultsParserTests extends ESTestCase {
    private static final double EPSILON = 0.000001;

    public static final String METRIC_OUTPUT_SAMPLE = "[{\"bucket\": {\"jobId\":\"foo\",\"timestamp\":1359450000000,"
            + "\"bucketSpan\":22, \"records\":[],"
            + "\"maxNormalizedProbability\":0, \"anomalyScore\":0,\"recordCount\":0,\"eventCount\":806,\"bucketInfluencers\":["
            + "{\"jobId\":\"foo\",\"anomalyScore\":0, \"probability\":0.0, \"influencerFieldName\":\"bucketTime\","
            + "\"initialAnomalyScore\":0.0}]}},{\"quantiles\": {\"jobId\":\"foo\", \"quantileState\":\"[normaliser 1.1, normaliser 2.1]\"}}"
            + ",{\"bucket\": {\"jobId\":\"foo\",\"timestamp\":1359453600000,\"bucketSpan\":22,"
            + "\"records\":[{\"jobId\":\"foo\",\"probability\":0.0637541,"
            + "\"byFieldName\":\"airline\",\"byFieldValue\":\"JZA\", \"typical\":[1020.08],\"actual\":[1042.14],"
            + "\"fieldName\":\"responsetime\",\"function\":\"max\",\"partitionFieldName\":\"\",\"partitionFieldValue\":\"\"},"
            + "{\"jobId\":\"foo\",\"probability\":0.00748292,\"byFieldName\":\"airline\",\"byFieldValue\":\"AMX\", "
            + "\"typical\":[20.2137],\"actual\":[22.8855],\"fieldName\":\"responsetime\",\"function\":\"max\",\"partitionFieldName\":\"\","
            + " \"partitionFieldValue\":\"\"},{\"jobId\":\"foo\",\"probability\":0.023494,\"byFieldName\":\"airline\","
            + "\"byFieldValue\":\"DAL\", \"typical\":[382.177],\"actual\":[358.934],\"fieldName\":\"responsetime\",\"function\":\"min\","
            + "\"partitionFieldName\":\"\", \"partitionFieldValue\":\"\"},{\"jobId\":\"foo\",\"probability\":0.0473552,"
            + "\"byFieldName\":\"airline\",\"byFieldValue\":\"SWA\", \"typical\":[152.148],\"actual\":[96.6425],"
            + "\"fieldName\":\"responsetime\",\"function\":\"min\",\"partitionFieldName\":\"\",\"partitionFieldValue\":\"\"}],"
            + "\"initialAnomalyScore\":0.0140005, \"anomalyScore\":20.22688, \"maxNormalizedProbability\":10.5688, \"recordCount\":4,"
            + "\"eventCount\":820,\"bucketInfluencers\":[{\"jobId\":\"foo\", \"rawAnomalyScore\":"
            + "0.0140005, \"probability\":0.01,\"influencerFieldName\":\"bucketTime\",\"initialAnomalyScore\":20.22688"
            + ",\"anomalyScore\":20.22688} ,{\"jobId\":\"foo\",\"rawAnomalyScore\":0.005, \"probability\":0.03,"
            + "\"influencerFieldName\":\"foo\",\"initialAnomalyScore\":10.5,\"anomalyScore\":10.5}]}},{\"quantiles\": {\"jobId\":\"foo\","
            + "\"quantileState\":\"[normaliser 1.2, normaliser 2.2]\"}} ,{\"flush\": {\"id\":\"testing1\"}} ,"
            + "{\"quantiles\": {\"jobId\":\"foo\", \"quantileState\":\"[normaliser 1.3, normaliser 2.3]\"}} ]";

    public static final String POPULATION_OUTPUT_SAMPLE = "[{\"timestamp\":1379590200,\"records\":[{\"probability\":1.38951e-08,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"mail.google.com\",\"function\":\"max\","
            + "\"causes\":[{\"probability\":1.38951e-08,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"mail.google.com\",\"function\":\"max\",\"typical\":[101534],\"actual\":[9.19027e+07]}],"
            + "\"normalizedProbability\":100,\"anomalyScore\":44.7324},{\"probability\":3.86587e-07,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"armmf.adobe.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":3.86587e-07,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"armmf.adobe.com\",\"function\":\"max\",\"typical\":[101534],\"actual\":[3.20093e+07]}],"
            + "\"normalizedProbability\":89.5834,\"anomalyScore\":44.7324},{\"probability\":0.00500083,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"0.docs.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.00500083,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"0.docs.google.com\",\"function\":\"max\",\"typical\":[101534],\"actual\":[6.61812e+06]}],"
            + "\"normalizedProbability\":1.19856,\"anomalyScore\":44.7324},{\"probability\":0.0152333,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"emea.salesforce.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0152333,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"emea.salesforce.com\",\"function\":\"max\",\"typical\":[101534],\"actual\":[5.36373e+06]}],"
            + "\"normalizedProbability\":0.303996,\"anomalyScore\":44.7324}],\"rawAnomalyScore\":1.30397,\"anomalyScore\":44.7324,"
            + "\"maxNormalizedProbability\":100,\"recordCount\":4,\"eventCount\":1235}" + ",{\"flush\":\"testing2\"}"
            + ",{\"timestamp\":1379590800,\"records\":[{\"probability\":1.9008e-08,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"mail.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":1.9008e-08,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"mail.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[1.1498e+08]}],"
            + "\"normalizedProbability\":93.6213,\"anomalyScore\":1.19192},{\"probability\":1.01013e-06,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"armmf.adobe.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":1.01013e-06,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"armmf.adobe.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[3.25808e+07]}],"
            + "\"normalizedProbability\":86.5825,\"anomalyScore\":1.19192},{\"probability\":0.000386185,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"0.docs.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.000386185,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"0.docs.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[3.22855e+06]}],"
            + "\"normalizedProbability\":17.1179,\"anomalyScore\":1.19192},{\"probability\":0.00208033,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"docs.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.00208033,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"docs.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[1.43328e+06]}],"
            + "\"normalizedProbability\":3.0692,\"anomalyScore\":1.19192},{\"probability\":0.00312988,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"booking2.airasia.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.00312988,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"booking2.airasia.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[1.15764e+06]}],"
            + "\"normalizedProbability\":1.99532,\"anomalyScore\":1.19192},{\"probability\":0.00379229,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.facebook.com\",\"function\":\"max\",\"causes\":["
            + "{\"probability\":0.00379229,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"www.facebook.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[1.0443e+06]}],"
            + "\"normalizedProbability\":1.62352,\"anomalyScore\":1.19192},{\"probability\":0.00623576,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.airasia.com\",\"function\":\"max\",\"causes\":["
            + "{\"probability\":0.00623576,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"www.airasia.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[792699]}],"
            + "\"normalizedProbability\":0.935134,\"anomalyScore\":1.19192},{\"probability\":0.00665308,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.google.com\",\"function\":\"max\",\"causes\":["
            + "{\"probability\":0.00665308,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"www.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[763985]}],"
            + "\"normalizedProbability\":0.868119,\"anomalyScore\":1.19192},{\"probability\":0.00709315,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"0.drive.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.00709315,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"0.drive.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[736442]}],"
            + "\"normalizedProbability\":0.805994,\"anomalyScore\":1.19192},{\"probability\":0.00755789,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"resources2.news.com.au\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.00755789,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"resources2.news.com.au\",\"function\":\"max\",\"typical\":[31356],\"actual\":[709962]}],"
            + "\"normalizedProbability\":0.748239,\"anomalyScore\":1.19192},{\"probability\":0.00834974,\"fieldName\":"
            + "\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.calypso.net.au\",\"function\":\"max\","
            + "\"causes\":[{\"probability\":0.00834974,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"www.calypso.net.au\",\"function\":\"max\",\"typical\":[31356],\"actual\":[669968]}],"
            + "\"normalizedProbability\":0.664644,\"anomalyScore\":1.19192},{\"probability\":0.0107711,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"ad.yieldmanager.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0107711,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"ad.yieldmanager.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[576067]}],"
            + "\"normalizedProbability\":0.485277,\"anomalyScore\":1.19192},{\"probability\":0.0123367,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.google-analytics.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0123367,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"www.google-analytics.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[530594]}],"
            + "\"normalizedProbability\":0.406783,\"anomalyScore\":1.19192},{\"probability\":0.0125647,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"bs.serving-sys.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0125647,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"bs.serving-sys.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[524690]}],"
            + "\"normalizedProbability\":0.396986,\"anomalyScore\":1.19192},{\"probability\":0.0141652,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.google.com.au\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0141652,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"www.google.com.au\",\"function\":\"max\",\"typical\":[31356],\"actual\":[487328]}],"
            + "\"normalizedProbability\":0.337075,\"anomalyScore\":1.19192},{\"probability\":0.0141742,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"resources1.news.com.au\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0141742,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"resources1.news.com.au\",\"function\":\"max\",\"typical\":[31356],\"actual\":[487136]}],"
            + "\"normalizedProbability\":0.336776,\"anomalyScore\":1.19192},{\"probability\":0.0145263,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"b.mail.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0145263,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"b.mail.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[479766]}],"
            + "\"normalizedProbability\":0.325385,\"anomalyScore\":1.19192},{\"probability\":0.0151447,\"fieldName\":\"sum_cs_bytes_\","
            + "\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.rei.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0151447,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.rei.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[467450]}],\"normalizedProbability\":0.306657,\"anomalyScore\":1.19192},"
            + "{\"probability\":0.0164073,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"s3.amazonaws.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0164073,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"s3.amazonaws.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[444511]}],\"normalizedProbability\":0.272805,\"anomalyScore\":1.19192},"
            + "{\"probability\":0.0201927,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"0-p-06-ash2.channel.facebook.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0201927,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"0-p-06-ash2.channel.facebook.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[389243]}],\"normalizedProbability\":0.196685,\"anomalyScore\":1.19192},"
            + "{\"probability\":0.0218721,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"booking.airasia.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0218721,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"booking.airasia.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[369509]}],\"normalizedProbability\":0.171353,"
            + "\"anomalyScore\":1.19192},{\"probability\":0.0242411,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"www.yammer.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0242411,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.yammer.com\",\"function\":\"max\","
            + "\"typical\":[31356],\"actual\":[345295]}],\"normalizedProbability\":0.141585,\"anomalyScore\":1.19192},"
            + "{\"probability\":0.0258232,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"safebrowsing-cache.google.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0258232,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"safebrowsing-cache.google.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[331051]}],\"normalizedProbability\":0.124748,\"anomalyScore\":1.19192},"
            + "{\"probability\":0.0259695,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"fbcdn-profile-a.akamaihd.net\",\"function\":\"max\",\"causes\":[{\"probability\":0.0259695,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"fbcdn-profile-a.akamaihd.net\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[329801]}],\"normalizedProbability\":0.123294,\"anomalyScore\":1.19192},"
            + "{\"probability\":0.0268874,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"www.oag.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0268874,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.oag.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[322200]}],\"normalizedProbability\":0.114537,"
            + "\"anomalyScore\":1.19192},{\"probability\":0.0279146,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"booking.qatarairways.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0279146,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"booking.qatarairways.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[314153]}],\"normalizedProbability\":0.105419,\"anomalyScore\":1.19192},"
            + "{\"probability\":0.0309351,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"resources3.news.com.au\",\"function\":\"max\",\"causes\":[{\"probability\":0.0309351,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"resources3.news.com.au\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[292918]}],\"normalizedProbability\":0.0821156,\"anomalyScore\":1.19192}"
            + ",{\"probability\":0.0335204,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"resources0.news.com.au\",\"function\":\"max\",\"causes\":[{\"probability\":0.0335204,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"resources0.news.com.au\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[277136]}],\"normalizedProbability\":0.0655063,\"anomalyScore\":1.19192}"
            + ",{\"probability\":0.0354927,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"www.southwest.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0354927,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.southwest.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[266310]}],\"normalizedProbability\":0.0544615,"
            + "\"anomalyScore\":1.19192},{\"probability\":0.0392043,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"syndication.twimg.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0392043,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"syndication.twimg.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[248276]}],\"normalizedProbability\":0.0366913,\"anomalyScore\":1.19192}"
            + ",{\"probability\":0.0400853,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\""
            + ",\"overFieldValue\":\"mts0.google.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0400853,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"mts0.google.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[244381]}],\"normalizedProbability\":0.0329562,"
            + "\"anomalyScore\":1.19192},{\"probability\":0.0407335,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"www.onthegotours.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0407335,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"www.onthegotours.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[241600]}],\"normalizedProbability\":0.0303116,"
            + "\"anomalyScore\":1.19192},{\"probability\":0.0470889,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"chatenabled.mail.google.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0470889,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"chatenabled.mail.google.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[217573]}],\"normalizedProbability\":0.00823738,"
            + "\"anomalyScore\":1.19192},{\"probability\":0.0491243,\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\","
            + "\"overFieldValue\":\"googleads.g.doubleclick.net\",\"function\":\"max\",\"causes\":[{\"probability\":0.0491243,"
            + "\"fieldName\":\"sum_cs_bytes_\",\"overFieldName\":\"cs_host\",\"overFieldValue\":\"googleads.g.doubleclick.net\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[210926]}],\"normalizedProbability\":0.00237509,"
            + "\"anomalyScore\":1.19192}],\"rawAnomalyScore\":1.26918,\"anomalyScore\":1.19192,\"maxNormalizedProbability\":93.6213,"
            + "\"recordCount\":34,\"eventCount\":1159}" + "]";

    public void testParser() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(METRIC_OUTPUT_SAMPLE.getBytes(StandardCharsets.UTF_8));
        AutodetectResultsParser parser = new AutodetectResultsParser(Settings.EMPTY, () -> ParseFieldMatcher.STRICT);
        List<AutodetectResult> results = new ArrayList<>();
        parser.parseResults(inputStream).forEachRemaining(results::add);
        List<Bucket> buckets = results.stream().map(AutodetectResult::getBucket)
                .filter(b -> b != null)
                .collect(Collectors.toList());

        assertEquals(2, buckets.size());
        assertEquals(new Date(1359450000000L), buckets.get(0).getTimestamp());
        assertEquals(0, buckets.get(0).getRecordCount());

        assertEquals(buckets.get(0).getEventCount(), 806);

        List<BucketInfluencer> bucketInfluencers = buckets.get(0).getBucketInfluencers();
        assertEquals(1, bucketInfluencers.size());
        assertEquals(0.0, bucketInfluencers.get(0).getRawAnomalyScore(), EPSILON);
        assertEquals(0.0, bucketInfluencers.get(0).getAnomalyScore(), EPSILON);
        assertEquals(0.0, bucketInfluencers.get(0).getProbability(), EPSILON);
        assertEquals("bucketTime", bucketInfluencers.get(0).getInfluencerFieldName());

        assertEquals(new Date(1359453600000L), buckets.get(1).getTimestamp());
        assertEquals(4, buckets.get(1).getRecordCount());

        assertEquals(buckets.get(1).getEventCount(), 820);
        bucketInfluencers = buckets.get(1).getBucketInfluencers();
        assertEquals(2, bucketInfluencers.size());
        assertEquals(0.0140005, bucketInfluencers.get(0).getRawAnomalyScore(), EPSILON);
        assertEquals(20.22688, bucketInfluencers.get(0).getAnomalyScore(), EPSILON);
        assertEquals(0.01, bucketInfluencers.get(0).getProbability(), EPSILON);
        assertEquals("bucketTime", bucketInfluencers.get(0).getInfluencerFieldName());
        assertEquals(0.005, bucketInfluencers.get(1).getRawAnomalyScore(), EPSILON);
        assertEquals(10.5, bucketInfluencers.get(1).getAnomalyScore(), EPSILON);
        assertEquals(0.03, bucketInfluencers.get(1).getProbability(), EPSILON);
        assertEquals("foo", bucketInfluencers.get(1).getInfluencerFieldName());

        Bucket secondBucket = buckets.get(1);

        assertEquals(0.0637541, secondBucket.getRecords().get(0).getProbability(), EPSILON);
        assertEquals("airline", secondBucket.getRecords().get(0).getByFieldName());
        assertEquals("JZA", secondBucket.getRecords().get(0).getByFieldValue());
        assertEquals(1020.08, secondBucket.getRecords().get(0).getTypical().get(0), EPSILON);
        assertEquals(1042.14, secondBucket.getRecords().get(0).getActual().get(0), EPSILON);
        assertEquals("responsetime", secondBucket.getRecords().get(0).getFieldName());
        assertEquals("max", secondBucket.getRecords().get(0).getFunction());
        assertEquals("", secondBucket.getRecords().get(0).getPartitionFieldName());
        assertEquals("", secondBucket.getRecords().get(0).getPartitionFieldValue());

        assertEquals(0.00748292, secondBucket.getRecords().get(1).getProbability(), EPSILON);
        assertEquals("airline", secondBucket.getRecords().get(1).getByFieldName());
        assertEquals("AMX", secondBucket.getRecords().get(1).getByFieldValue());
        assertEquals(20.2137, secondBucket.getRecords().get(1).getTypical().get(0), EPSILON);
        assertEquals(22.8855, secondBucket.getRecords().get(1).getActual().get(0), EPSILON);
        assertEquals("responsetime", secondBucket.getRecords().get(1).getFieldName());
        assertEquals("max", secondBucket.getRecords().get(1).getFunction());
        assertEquals("", secondBucket.getRecords().get(1).getPartitionFieldName());
        assertEquals("", secondBucket.getRecords().get(1).getPartitionFieldValue());

        assertEquals(0.023494, secondBucket.getRecords().get(2).getProbability(), EPSILON);
        assertEquals("airline", secondBucket.getRecords().get(2).getByFieldName());
        assertEquals("DAL", secondBucket.getRecords().get(2).getByFieldValue());
        assertEquals(382.177, secondBucket.getRecords().get(2).getTypical().get(0), EPSILON);
        assertEquals(358.934, secondBucket.getRecords().get(2).getActual().get(0), EPSILON);
        assertEquals("responsetime", secondBucket.getRecords().get(2).getFieldName());
        assertEquals("min", secondBucket.getRecords().get(2).getFunction());
        assertEquals("", secondBucket.getRecords().get(2).getPartitionFieldName());
        assertEquals("", secondBucket.getRecords().get(2).getPartitionFieldValue());

        assertEquals(0.0473552, secondBucket.getRecords().get(3).getProbability(), EPSILON);
        assertEquals("airline", secondBucket.getRecords().get(3).getByFieldName());
        assertEquals("SWA", secondBucket.getRecords().get(3).getByFieldValue());
        assertEquals(152.148, secondBucket.getRecords().get(3).getTypical().get(0), EPSILON);
        assertEquals(96.6425, secondBucket.getRecords().get(3).getActual().get(0), EPSILON);
        assertEquals("responsetime", secondBucket.getRecords().get(3).getFieldName());
        assertEquals("min", secondBucket.getRecords().get(3).getFunction());
        assertEquals("", secondBucket.getRecords().get(3).getPartitionFieldName());
        assertEquals("", secondBucket.getRecords().get(3).getPartitionFieldValue());

        List<Quantiles> quantiles = results.stream().map(AutodetectResult::getQuantiles)
                .filter(q -> q != null)
                .collect(Collectors.toList());
        assertEquals(3, quantiles.size());
        assertEquals("foo", quantiles.get(0).getJobId());
        assertNull(quantiles.get(0).getTimestamp());
        assertEquals("[normaliser 1.1, normaliser 2.1]", quantiles.get(0).getQuantileState());
        assertEquals("foo", quantiles.get(1).getJobId());
        assertNull(quantiles.get(1).getTimestamp());
        assertEquals("[normaliser 1.2, normaliser 2.2]", quantiles.get(1).getQuantileState());
        assertEquals("foo", quantiles.get(2).getJobId());
        assertNull(quantiles.get(2).getTimestamp());
        assertEquals("[normaliser 1.3, normaliser 2.3]", quantiles.get(2).getQuantileState());
    }

    @AwaitsFix(bugUrl = "rewrite this test so it doesn't use ~200 lines of json")
    public void testPopulationParser() throws IOException {
        InputStream inputStream = new ByteArrayInputStream(POPULATION_OUTPUT_SAMPLE.getBytes(StandardCharsets.UTF_8));
        AutodetectResultsParser parser = new AutodetectResultsParser(Settings.EMPTY, () -> ParseFieldMatcher.STRICT);
        List<AutodetectResult> results = new ArrayList<>();
        parser.parseResults(inputStream).forEachRemaining(results::add);
        List<Bucket> buckets = results.stream().map(AutodetectResult::getBucket)
                .filter(b -> b != null)
                .collect(Collectors.toList());

        assertEquals(2, buckets.size());
        assertEquals(new Date(1379590200000L), buckets.get(0).getTimestamp());
        assertEquals(4, buckets.get(0).getRecordCount());
        assertEquals(buckets.get(0).getEventCount(), 1235);

        Bucket firstBucket = buckets.get(0);
        assertEquals(1.38951e-08, firstBucket.getRecords().get(0).getProbability(), EPSILON);
        assertEquals("sum_cs_bytes_", firstBucket.getRecords().get(0).getFieldName());
        assertEquals("max", firstBucket.getRecords().get(0).getFunction());
        assertEquals("cs_host", firstBucket.getRecords().get(0).getOverFieldName());
        assertEquals("mail.google.com", firstBucket.getRecords().get(0).getOverFieldValue());
        assertNotNull(firstBucket.getRecords().get(0).getCauses());

        assertEquals(new Date(1379590800000L), buckets.get(1).getTimestamp());
        assertEquals(34, buckets.get(1).getRecordCount());
        assertEquals(buckets.get(1).getEventCount(), 1159);
    }

    public void testParse_GivenEmptyArray() throws ElasticsearchParseException, IOException {
        String json = "[]";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        AutodetectResultsParser parser = new AutodetectResultsParser(Settings.EMPTY, () -> ParseFieldMatcher.STRICT);
        assertFalse(parser.parseResults(inputStream).hasNext());
    }

    public void testParse_GivenModelSizeStats() throws ElasticsearchParseException, IOException {
        String json = "[{\"modelSizeStats\": {\"jobId\": \"foo\", \"modelBytes\":300}}]";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));

        AutodetectResultsParser parser = new AutodetectResultsParser(Settings.EMPTY, () -> ParseFieldMatcher.STRICT);
        List<AutodetectResult> results = new ArrayList<>();
        parser.parseResults(inputStream).forEachRemaining(results::add);

        assertEquals(1, results.size());
        assertEquals(300, results.get(0).getModelSizeStats().getModelBytes());
    }

    public void testParse_GivenCategoryDefinition() throws IOException {
        String json = "[{\"categoryDefinition\": {\"jobId\":\"foo\", \"categoryId\":18}}]";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        AutodetectResultsParser parser = new AutodetectResultsParser(Settings.EMPTY, () -> ParseFieldMatcher.STRICT);
        List<AutodetectResult> results = new ArrayList<>();
        parser.parseResults(inputStream).forEachRemaining(results::add);

        assertEquals(1, results.size());
        assertEquals(18, results.get(0).getCategoryDefinition().getCategoryId());
    }

    public void testParse_GivenUnknownObject() throws ElasticsearchParseException, IOException {
        String json = "[{\"unknown\":{\"id\": 18}}]";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        AutodetectResultsParser parser = new AutodetectResultsParser(Settings.EMPTY, () -> ParseFieldMatcher.STRICT);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parseResults(inputStream).forEachRemaining(a -> {}));
        assertEquals("[autodetect_result] unknown field [unknown], parser not found", e.getMessage());
    }

    public void testParse_GivenArrayContainsAnotherArray() throws ElasticsearchParseException, IOException {
        String json = "[[]]";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        AutodetectResultsParser parser = new AutodetectResultsParser(Settings.EMPTY, () -> ParseFieldMatcher.STRICT);
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
                () -> parser.parseResults(inputStream).forEachRemaining(a -> {}));
        assertEquals("unexpected token [START_ARRAY]", e.getMessage());
    }

}
