/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.BucketInfluencer;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

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

    private static final String METRIC_OUTPUT_SAMPLE = "[{\"bucket\": {\"job_id\":\"foo\",\"timestamp\":1359450000000,"
            + "\"bucket_span\":22, \"records\":[],"
            + "\"anomaly_score\":0,\"event_count\":806,\"bucket_influencers\":["
            + "{\"timestamp\":1359450000000,\"bucket_span\":22,\"job_id\":\"foo\",\"anomaly_score\":0,"
            + "\"probability\":0.0, \"influencer_field_name\":\"bucket_time\","
            + "\"initial_anomaly_score\":0.0}]}},{\"quantiles\": {\"job_id\":\"foo\", \"quantile_state\":\"[normalizer 1.1, normalizer 2" +
            ".1]\",\"timestamp\":1359450000000}}"
            + ",{\"bucket\": {\"job_id\":\"foo\",\"timestamp\":1359453600000,\"bucket_span\":22,\"records\":"
            + "[{\"timestamp\":1359453600000,\"bucket_span\":22,\"job_id\":\"foo\",\"probability\":0.0637541,"
            + "\"by_field_name\":\"airline\",\"by_field_value\":\"JZA\", \"typical\":[1020.08],\"actual\":[1042.14],"
            + "\"field_name\":\"responsetime\",\"function\":\"max\",\"partition_field_name\":\"\",\"partition_field_value\":\"\"},"
            + "{\"timestamp\":1359453600000,\"bucket_span\":22,\"job_id\":\"foo\",\"probability\":0.00748292,"
            + "\"by_field_name\":\"airline\",\"by_field_value\":\"AMX\", "
            + "\"typical\":[20.2137],\"actual\":[22.8855],\"field_name\":\"responsetime\",\"function\":\"max\","
            + "\"partition_field_name\":\"\",\"partition_field_value\":\"\"},{\"timestamp\":1359453600000,\"bucket_span\":22,"
            + "\"job_id\":\"foo\",\"probability\":0.023494,\"by_field_name\":\"airline\","
            + "\"by_field_value\":\"DAL\", \"typical\":[382.177],\"actual\":[358.934],\"field_name\":\"responsetime\",\"function\":\"min\","
            + "\"partition_field_name\":\"\", \"partition_field_value\":\"\"},{\"timestamp\":1359453600000,\"bucket_span\":22,"
            + "\"job_id\":\"foo\","
            + "\"probability\":0.0473552,\"by_field_name\":\"airline\",\"by_field_value\":\"SWA\", \"typical\":[152.148],"
            + "\"actual\":[96.6425],\"field_name\":\"responsetime\",\"function\":\"min\",\"partition_field_name\":\"\","
            + "\"partition_field_value\":\"\"}],"
            + "\"initial_anomaly_score\":0.0140005, \"anomaly_score\":20.22688,"
            + "\"event_count\":820,\"bucket_influencers\":[{\"timestamp\":1359453600000,\"bucket_span\":22,"
            + "\"job_id\":\"foo\", \"raw_anomaly_score\":0.0140005, \"probability\":0.01,\"influencer_field_name\":\"bucket_time\","
            + "\"initial_anomaly_score\":20.22688,\"anomaly_score\":20.22688} ,{\"timestamp\":1359453600000,\"bucket_span\":22,"
            + "\"job_id\":\"foo\",\"raw_anomaly_score\":0.005, \"probability\":0.03,"
            + "\"influencer_field_name\":\"foo\",\"initial_anomaly_score\":10.5,\"anomaly_score\":10.5}]}},{\"quantiles\": "
            + "{\"job_id\":\"foo\",\"timestamp\":1359453600000,"
            + "\"quantile_state\":\"[normalizer 1.2, normalizer 2.2]\"}} ,{\"flush\": {\"id\":\"testing1\"}} ,"
            + "{\"quantiles\": {\"job_id\":\"foo\",\"timestamp\":1359453600000,\"quantile_state\":\"[normalizer 1.3, normalizer 2.3]\"}} ]";

    private static final String POPULATION_OUTPUT_SAMPLE = "[{\"timestamp\":1379590200,\"records\":[{\"probability\":1.38951e-08,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"mail.google.com\","
            + "\"function\":\"max\","
            + "\"causes\":[{\"probability\":1.38951e-08,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"mail.google.com\",\"function\":\"max\",\"typical\":[101534],\"actual\":[9.19027e+07]}],"
            + "\"record_score\":100,\"anomaly_score\":44.7324},{\"probability\":3.86587e-07,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"armmf.adobe.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":3.86587e-07,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"armmf.adobe.com\",\"function\":\"max\",\"typical\":[101534],\"actual\":[3.20093e+07]}],"
            + "\"record_score\":89.5834,\"anomaly_score\":44.7324},{\"probability\":0.00500083,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"0.docs.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.00500083,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"0.docs.google.com\",\"function\":\"max\",\"typical\":[101534],\"actual\":[6.61812e+06]}],"
            + "\"record_score\":1.19856,\"anomaly_score\":44.7324},{\"probability\":0.0152333,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"emea.salesforce.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0152333,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"emea.salesforce.com\",\"function\":\"max\",\"typical\":[101534],\"actual\":[5.36373e+06]}],"
            + "\"record_score\":0.303996,\"anomaly_score\":44.7324}],\"raw_anomaly_score\":1.30397,\"anomaly_score\":44.7324,"
            + "\"event_count\":1235}" + ",{\"flush\":\"testing2\"}"
            + ",{\"timestamp\":1379590800,\"records\":[{\"probability\":1.9008e-08,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"mail.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":1.9008e-08,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"mail.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[1.1498e+08]}],"
            + "\"record_score\":93.6213,\"anomaly_score\":1.19192},{\"probability\":1.01013e-06,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"armmf.adobe.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":1.01013e-06,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"armmf.adobe.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[3.25808e+07]}],"
            + "\"record_score\":86.5825,\"anomaly_score\":1.19192},{\"probability\":0.000386185,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"0.docs.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.000386185,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"0.docs.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[3.22855e+06]}],"
            + "\"record_score\":17.1179,\"anomaly_score\":1.19192},{\"probability\":0.00208033,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"docs.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.00208033,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"docs.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[1.43328e+06]}],"
            + "\"record_score\":3.0692,\"anomaly_score\":1.19192},{\"probability\":0.00312988,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"booking2.airasia.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.00312988,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"booking2.airasia.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[1.15764e+06]}],"
            + "\"record_score\":1.99532,\"anomaly_score\":1.19192},{\"probability\":0.00379229,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.facebook.com\",\"function\":\"max\",\"causes\":["
            + "{\"probability\":0.00379229,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"www.facebook.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[1.0443e+06]}],"
            + "\"record_score\":1.62352,\"anomaly_score\":1.19192},{\"probability\":0.00623576,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.airasia.com\",\"function\":\"max\",\"causes\":["
            + "{\"probability\":0.00623576,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"www.airasia.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[792699]}],"
            + "\"record_score\":0.935134,\"anomaly_score\":1.19192},{\"probability\":0.00665308,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.google.com\",\"function\":\"max\",\"causes\":["
            + "{\"probability\":0.00665308,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"www.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[763985]}],"
            + "\"record_score\":0.868119,\"anomaly_score\":1.19192},{\"probability\":0.00709315,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"0.drive.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.00709315,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"0.drive.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[736442]}],"
            + "\"record_score\":0.805994,\"anomaly_score\":1.19192},{\"probability\":0.00755789,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"resources2.news.com.au\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.00755789,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"resources2.news.com.au\",\"function\":\"max\",\"typical\":[31356],\"actual\":[709962]}],"
            + "\"record_score\":0.748239,\"anomaly_score\":1.19192},{\"probability\":0.00834974,\"field_name\":"
            + "\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.calypso.net.au\",\"function\":\"max\","
            + "\"causes\":[{\"probability\":0.00834974,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"www.calypso.net.au\",\"function\":\"max\",\"typical\":[31356],\"actual\":[669968]}],"
            + "\"record_score\":0.664644,\"anomaly_score\":1.19192},{\"probability\":0.0107711,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"ad.yieldmanager.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0107711,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"ad.yieldmanager.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[576067]}],"
            + "\"record_score\":0.485277,\"anomaly_score\":1.19192},{\"probability\":0.0123367,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.google-analytics.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0123367,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"www.google-analytics.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[530594]}],"
            + "\"record_score\":0.406783,\"anomaly_score\":1.19192},{\"probability\":0.0125647,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"bs.serving-sys.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0125647,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"bs.serving-sys.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[524690]}],"
            + "\"record_score\":0.396986,\"anomaly_score\":1.19192},{\"probability\":0.0141652,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.google.com.au\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0141652,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"www.google.com.au\",\"function\":\"max\",\"typical\":[31356],\"actual\":[487328]}],"
            + "\"record_score\":0.337075,\"anomaly_score\":1.19192},{\"probability\":0.0141742,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"resources1.news.com.au\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0141742,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"resources1.news.com.au\",\"function\":\"max\",\"typical\":[31356],\"actual\":[487136]}],"
            + "\"record_score\":0.336776,\"anomaly_score\":1.19192},{\"probability\":0.0145263,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"b.mail.google.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0145263,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"b.mail.google.com\",\"function\":\"max\",\"typical\":[31356],\"actual\":[479766]}],"
            + "\"record_score\":0.325385,\"anomaly_score\":1.19192},{\"probability\":0.0151447,\"field_name\":\"sum_cs_bytes_\","
            + "\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.rei.com\",\"function\":\"max\",\"causes\":[{"
            + "\"probability\":0.0151447,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.rei" +
            ".com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[467450]}],\"record_score\":0.306657,\"anomaly_score\":1" +
            ".19192},"
            + "{\"probability\":0.0164073,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"s3.amazonaws.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0164073,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"s3.amazonaws.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[444511]}],\"record_score\":0.272805,\"anomaly_score\":1" +
            ".19192},"
            + "{\"probability\":0.0201927,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"0-p-06-ash2.channel.facebook.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0201927,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"0-p-06-ash2.channel.facebook.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[389243]}],\"record_score\":0.196685,\"anomaly_score\":1" +
            ".19192},"
            + "{\"probability\":0.0218721,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"booking.airasia.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0218721,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"booking.airasia.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[369509]}],\"record_score\":0.171353,"
            + "\"anomaly_score\":1.19192},{\"probability\":0.0242411,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"www.yammer.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0242411,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.yammer.com\"," +
            "\"function\":\"max\","
            + "\"typical\":[31356],\"actual\":[345295]}],\"record_score\":0.141585,\"anomaly_score\":1.19192},"
            + "{\"probability\":0.0258232,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"safebrowsing-cache.google.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0258232,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"safebrowsing-cache.google.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[331051]}],\"record_score\":0.124748,\"anomaly_score\":1" +
            ".19192},"
            + "{\"probability\":0.0259695,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"fbcdn-profile-a.akamaihd.net\",\"function\":\"max\",\"causes\":[{\"probability\":0.0259695,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"fbcdn-profile-a.akamaihd.net\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[329801]}],\"record_score\":0.123294,\"anomaly_score\":1" +
            ".19192},"
            + "{\"probability\":0.0268874,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"www.oag.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0268874,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.oag.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[322200]}],\"record_score\":0.114537,"
            + "\"anomaly_score\":1.19192},{\"probability\":0.0279146,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"booking.qatarairways.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0279146,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"booking.qatarairways.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[314153]}],\"record_score\":0.105419,\"anomaly_score\":1" +
            ".19192},"
            + "{\"probability\":0.0309351,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"resources3.news.com.au\",\"function\":\"max\",\"causes\":[{\"probability\":0.0309351,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"resources3.news.com.au\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[292918]}],\"record_score\":0.0821156,\"anomaly_score\":1" +
            ".19192}"
            + ",{\"probability\":0.0335204,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"resources0.news.com.au\",\"function\":\"max\",\"causes\":[{\"probability\":0.0335204,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"resources0.news.com.au\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[277136]}],\"record_score\":0.0655063,\"anomaly_score\":1" +
            ".19192}"
            + ",{\"probability\":0.0354927,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"www.southwest.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0354927,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.southwest.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[266310]}],\"record_score\":0.0544615,"
            + "\"anomaly_score\":1.19192},{\"probability\":0.0392043,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"syndication.twimg.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0392043,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"syndication.twimg.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[248276]}],\"record_score\":0.0366913,\"anomaly_score\":1" +
            ".19192}"
            + ",{\"probability\":0.0400853,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\""
            + ",\"over_field_value\":\"mts0.google.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0400853,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"mts0.google.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[244381]}],\"record_score\":0.0329562,"
            + "\"anomaly_score\":1.19192},{\"probability\":0.0407335,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"www.onthegotours.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0407335,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"www.onthegotours.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[241600]}],\"record_score\":0.0303116,"
            + "\"anomaly_score\":1.19192},{\"probability\":0.0470889,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"chatenabled.mail.google.com\",\"function\":\"max\",\"causes\":[{\"probability\":0.0470889,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"chatenabled.mail.google.com\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[217573]}],\"record_score\":0.00823738,"
            + "\"anomaly_score\":1.19192},{\"probability\":0.0491243,\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\","
            + "\"over_field_value\":\"googleads.g.doubleclick.net\",\"function\":\"max\",\"causes\":[{\"probability\":0.0491243,"
            + "\"field_name\":\"sum_cs_bytes_\",\"over_field_name\":\"cs_host\",\"over_field_value\":\"googleads.g.doubleclick.net\","
            + "\"function\":\"max\",\"typical\":[31356],\"actual\":[210926]}],\"record_score\":0.00237509,"
            + "\"anomaly_score\":1.19192}],\"raw_anomaly_score\":1.26918,\"anomaly_score\":1.19192,"
            + "\"event_count\":1159}" + "]";

    public void testParser() throws IOException {
        try (InputStream inputStream = new ByteArrayInputStream(METRIC_OUTPUT_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            AutodetectResultsParser parser = new AutodetectResultsParser();
            List<AutodetectResult> results = new ArrayList<>();
            parser.parseResults(inputStream).forEachRemaining(results::add);
            List<Bucket> buckets = results.stream().map(AutodetectResult::getBucket)
                .filter(b -> b != null)
                .collect(Collectors.toList());

            assertEquals(2, buckets.size());
            assertEquals(new Date(1359450000000L), buckets.get(0).getTimestamp());

            assertEquals(buckets.get(0).getEventCount(), 806);

            List<BucketInfluencer> bucketInfluencers = buckets.get(0).getBucketInfluencers();
            assertEquals(1, bucketInfluencers.size());
            assertEquals(0.0, bucketInfluencers.get(0).getRawAnomalyScore(), EPSILON);
            assertEquals(0.0, bucketInfluencers.get(0).getAnomalyScore(), EPSILON);
            assertEquals(0.0, bucketInfluencers.get(0).getProbability(), EPSILON);
            assertEquals("bucket_time", bucketInfluencers.get(0).getInfluencerFieldName());

            assertEquals(new Date(1359453600000L), buckets.get(1).getTimestamp());

            assertEquals(buckets.get(1).getEventCount(), 820);
            bucketInfluencers = buckets.get(1).getBucketInfluencers();
            assertEquals(2, bucketInfluencers.size());
            assertEquals(0.0140005, bucketInfluencers.get(0).getRawAnomalyScore(), EPSILON);
            assertEquals(20.22688, bucketInfluencers.get(0).getAnomalyScore(), EPSILON);
            assertEquals(0.01, bucketInfluencers.get(0).getProbability(), EPSILON);
            assertEquals("bucket_time", bucketInfluencers.get(0).getInfluencerFieldName());
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
            assertEquals(new Date(1359450000000L), quantiles.get(0).getTimestamp());
            assertEquals("[normalizer 1.1, normalizer 2.1]", quantiles.get(0).getQuantileState());
            assertEquals("foo", quantiles.get(1).getJobId());
            assertEquals(new Date(1359453600000L), quantiles.get(1).getTimestamp());
            assertEquals("[normalizer 1.2, normalizer 2.2]", quantiles.get(1).getQuantileState());
            assertEquals("foo", quantiles.get(2).getJobId());
            assertEquals(new Date(1359453600000L), quantiles.get(2).getTimestamp());
            assertEquals("[normalizer 1.3, normalizer 2.3]", quantiles.get(2).getQuantileState());
        }
    }

    @AwaitsFix(bugUrl = "rewrite this test so it doesn't use ~200 lines of json")
    public void testPopulationParser() throws IOException {
        try (InputStream inputStream = new ByteArrayInputStream(POPULATION_OUTPUT_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            AutodetectResultsParser parser = new AutodetectResultsParser();
            List<AutodetectResult> results = new ArrayList<>();
            parser.parseResults(inputStream).forEachRemaining(results::add);
            List<Bucket> buckets = results.stream().map(AutodetectResult::getBucket)
                .filter(b -> b != null)
                .collect(Collectors.toList());

            assertEquals(2, buckets.size());
            assertEquals(new Date(1379590200000L), buckets.get(0).getTimestamp());
            assertEquals(buckets.get(0).getEventCount(), 1235);

            Bucket firstBucket = buckets.get(0);
            assertEquals(1.38951e-08, firstBucket.getRecords().get(0).getProbability(), EPSILON);
            assertEquals("sum_cs_bytes_", firstBucket.getRecords().get(0).getFieldName());
            assertEquals("max", firstBucket.getRecords().get(0).getFunction());
            assertEquals("cs_host", firstBucket.getRecords().get(0).getOverFieldName());
            assertEquals("mail.google.com", firstBucket.getRecords().get(0).getOverFieldValue());
            assertNotNull(firstBucket.getRecords().get(0).getCauses());

            assertEquals(new Date(1379590800000L), buckets.get(1).getTimestamp());
            assertEquals(buckets.get(1).getEventCount(), 1159);
        }
    }

    public void testParse_GivenEmptyArray() throws ElasticsearchParseException, IOException {
        String json = "[]";
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            AutodetectResultsParser parser = new AutodetectResultsParser();
            assertFalse(parser.parseResults(inputStream).hasNext());
        }
    }

    public void testParse_GivenModelSizeStats() throws ElasticsearchParseException, IOException {
        String json = "[{\"model_size_stats\": {\"job_id\": \"foo\", \"model_bytes\":300}}]";
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {

            AutodetectResultsParser parser = new AutodetectResultsParser();
            List<AutodetectResult> results = new ArrayList<>();
            parser.parseResults(inputStream).forEachRemaining(results::add);

            assertEquals(1, results.size());
            assertEquals(300, results.get(0).getModelSizeStats().getModelBytes());
        }
    }

    public void testParse_GivenCategoryDefinition() throws IOException {
        String json = "[{\"category_definition\": {\"job_id\":\"foo\", \"category_id\":18}}]";
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            AutodetectResultsParser parser = new AutodetectResultsParser();
            List<AutodetectResult> results = new ArrayList<>();
            parser.parseResults(inputStream).forEachRemaining(results::add);


            assertEquals(1, results.size());
            assertEquals(18, results.get(0).getCategoryDefinition().getCategoryId());
        }
    }

    public void testParse_GivenUnknownObject() throws ElasticsearchParseException, IOException {
        String json = "[{\"unknown\":{\"id\": 18}}]";
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            AutodetectResultsParser parser = new AutodetectResultsParser();
            XContentParseException e = expectThrows(XContentParseException.class,
                () -> parser.parseResults(inputStream).forEachRemaining(a -> {
                }));
            assertEquals("[1:3] [autodetect_result] unknown field [unknown], parser not found", e.getMessage());
        }
    }

    public void testParse_GivenArrayContainsAnotherArray() throws ElasticsearchParseException, IOException {
        String json = "[[]]";
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            AutodetectResultsParser parser = new AutodetectResultsParser();
            ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
                () -> parser.parseResults(inputStream).forEachRemaining(a -> {
                }));
            assertEquals("unexpected token [START_ARRAY]", e.getMessage());
        }
    }

    /**
     * Ensure that we do not accept NaN values
     */
    public void testParsingExceptionNaN() {
        String json = "[{\"bucket\": {\"job_id\":\"foo\",\"timestamp\":1359453600000,\"bucket_span\":10,\"records\":"
                + "[{\"timestamp\":1359453600000,\"bucket_span\":10,\"job_id\":\"foo\",\"probability\":NaN,"
                + "\"by_field_name\":\"airline\",\"by_field_value\":\"JZA\", \"typical\":[1020.08],\"actual\":[0],"
                + "\"field_name\":\"responsetime\",\"function\":\"max\",\"partition_field_name\":\"\",\"partition_field_value\":\"\"}]}}]";
        InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        AutodetectResultsParser parser = new AutodetectResultsParser();

        expectThrows(XContentParseException.class,
                () -> parser.parseResults(inputStream).forEachRemaining(a -> {}));
    }
}
