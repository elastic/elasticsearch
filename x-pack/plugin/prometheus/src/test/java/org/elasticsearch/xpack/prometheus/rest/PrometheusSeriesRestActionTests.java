/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PrometheusSeriesRestActionTests extends ESTestCase {

    public void testParseQueryStringMatchSelectors() {
        String qs = "match%5B%5D=up&match%5B%5D=http_requests_total&start=1609746000&end=1609749600";
        var params = RestUtils.decodeQueryStringMulti(qs, 0);
        assertThat(params.get("match[]").size(), is(2));
        assertThat(params.get("match[]").get(0), equalTo("up"));
        assertThat(params.get("match[]").get(1), equalTo("http_requests_total"));
        assertThat(params.get("start").get(0), equalTo("1609746000"));
        assertThat(params.get("end").get(0), equalTo("1609749600"));
    }

    public void testParseQueryStringEmpty() {
        assertThat(RestUtils.decodeQueryStringMulti("", 0).isEmpty(), is(true));
    }

    public void testParseQueryStringUrlEncodedSelector() {
        // match[]=up{job="prometheus"} → URL-encoded
        String qs = "match%5B%5D=up%7Bjob%3D%22prometheus%22%7D";
        var params = RestUtils.decodeQueryStringMulti(qs, 0);
        assertThat(params.get("match[]").get(0), equalTo("up{job=\"prometheus\"}"));
    }

}
