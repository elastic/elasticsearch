/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Header values come from each client's source code: the meta header builders and User-Agent formats in the official
 * Elasticsearch clients, Kibana's default headers, the Grafana plugin SDK's User-Agent middleware, and curl.
 */
public class DirectClientTests extends ESTestCase {

    private static final String KIBANA_META = "es=9.5.1,js=22.11.0,t=9.1.1,hc=22.11.0";

    public void testPythonMetaHeader() {
        assertThat(classifyMeta("es=9.5.0,py=3.12.1,t=9.4.2,ur=2.2.3"), is(DirectClient.PYTHON));
        assertThat(classifyMeta("es=9.5.0,py=3.12.1,t=9.4.2,ai=3.10.5,h=bp"), is(DirectClient.PYTHON));
    }

    public void testJavascriptMetaHeader() {
        assertThat(classifyMeta("es=9.6.0,js=22.11.0,t=9.1.1,un=22.11.0"), is(DirectClient.JAVASCRIPT));
        assertThat(classifyMeta("et=9.1.1,js=22.11.0"), is(DirectClient.JAVASCRIPT));
    }

    public void testJavaMetaHeader() {
        assertThat(classifyMeta("es=9.2.0,jv=21,t=9.2.0,hl=2,hc=5.4"), is(DirectClient.JAVA));
        assertThat(classifyMeta("es=,jv=17,t=,hc="), is(DirectClient.JAVA));
        assertThat(classifyMeta("es=9.2.0,jv=21,t=9.2.0,hl=2,hc=5.4,kt=2.0"), is(DirectClient.JAVA));
    }

    public void testGoMetaHeader() {
        assertThat(classifyMeta("es=9.1.0,go=1.23.2,t=8.6.0,hc=1.23.2,hl=1"), is(DirectClient.GO));
        assertThat(classifyMeta("foo=bar,es=9.1.0,go=1.23.2,t=8.6.0,hc=1.23.2"), is(DirectClient.GO));
    }

    public void testDotnetMetaHeader() {
        assertThat(classifyMeta("es=9.1.0,a=0,net=8.0.10,so=8.0.10,t=0.10.0"), is(DirectClient.DOTNET));
    }

    public void testRubyMetaHeader() {
        assertThat(classifyMeta("es=9.1.0,rb=3.3.0,t=8.4.0,fd=2.9.0,nh=0.4.1"), is(DirectClient.RUBY));
    }

    public void testJRubyMetaHeaderIsRubyNotJava() {
        assertThat(classifyMeta("es=9.1.0,rb=3.1.4,t=8.4.0,jv=21,jr=9.4.8.0,mc=0.9.1"), is(DirectClient.RUBY));
    }

    public void testPhpMetaHeader() {
        assertThat(classifyMeta("es=9.5.0,php=8.3.12,t=9.0.0,a=0,gu=7.9.2"), is(DirectClient.PHP));
        assertThat(classifyMeta("es=9.0.0-p1,php=8.3.12,t=9.0.0,a=1"), is(DirectClient.PHP));
    }

    public void testRustMetaHeader() {
        assertThat(classifyMeta("es=9.0.0p,rs=1.81.0,t=9.0.0p,tls=r"), is(DirectClient.RUST));
    }

    public void testOfficialClientUserAgentWithoutMetaHeader() {
        assertThat(classifyUserAgent("elasticsearch-py/9.5.0 (Python/3.12.1; elastic-transport/9.4.2)"), is(DirectClient.PYTHON));
        assertThat(
            classifyUserAgent("elasticsearch-js/9.6.0 (linux 6.1.0-x64; Node.js 22.11.0; Transport 9.1.1)"),
            is(DirectClient.JAVASCRIPT)
        );
        assertThat(classifyUserAgent("elastic-java/9.2.0 (Java/21.0.4)"), is(DirectClient.JAVA));
        assertThat(classifyUserAgent("elasticsearch-java/9.2.0 (Java/21.0.4)"), is(DirectClient.JAVA));
        assertThat(classifyUserAgent("go-elasticsearch/9.1.0 (linux amd64; Go 1.23.2)"), is(DirectClient.GO));
        assertThat(
            classifyUserAgent("elasticsearch-net/9.1.0+abc (Linux 6.1.0; .NET 8.0.10; Elastic.Clients.Elasticsearch)"),
            is(DirectClient.DOTNET)
        );
        assertThat(
            classifyUserAgent("elasticsearch-ruby/9.1.0; elastic-transport-ruby/8.4.0; RUBY_VERSION: 3.3.0; linux x86_64"),
            is(DirectClient.RUBY)
        );
        assertThat(classifyUserAgent("elasticsearch-php/9.5.0 (Linux 6.1.0; PHP 8.3.12)"), is(DirectClient.PHP));
        assertThat(classifyUserAgent("elasticsearch-rs/9.0.0"), is(DirectClient.RUST));
    }

    public void testMetaHeaderWinsOverOfficialClientUserAgent() {
        assertThat(
            DirectClient.classify(null, "es=9.5.0,py=3.12.1,t=9.4.2,ur=2.2.3", "elasticsearch-js/9.6.0 (linux; Node.js 22.11.0)"),
            is(DirectClient.PYTHON)
        );
    }

    public void testGrafana() {
        assertThat(classifyUserAgent("Grafana/13.3.0 (linux; amd64) prometheus/13.2.6"), is(DirectClient.GRAFANA));
        assertThat(classifyUserAgent("Grafana/12.3.0 (linux; amd64) elasticsearch/12.9.4"), is(DirectClient.GRAFANA));
        assertThat(classifyUserAgent("Grafana prometheus/13.2.6"), is(DirectClient.GRAFANA));
        assertThat(classifyUserAgent("Grafana/13.3.0"), is(DirectClient.GRAFANA));
    }

    public void testGrafanaWinsOverClientMetaHeader() {
        assertThat(
            DirectClient.classify(null, "es=9.1.0,go=1.23.2,t=8.6.0,hc=1.23.2", "Grafana/13.3.0 (linux; amd64) elasticsearch/12.9.4"),
            is(DirectClient.GRAFANA)
        );
    }

    public void testCurl() {
        assertThat(classifyUserAgent("curl/8.7.1"), is(DirectClient.CURL));
    }

    public void testKibanaIsNotADirectClient() {
        assertThat(DirectClient.classify("kibana", KIBANA_META, "Kibana/9.5.1"), is(DirectClient.ELASTIC_PRODUCT));
    }

    public void testKibanaUserAgentWithoutProductOriginIsNotADirectClient() {
        assertThat(DirectClient.classify(null, KIBANA_META, "Kibana/9.5.1"), is(DirectClient.ELASTIC_PRODUCT));
    }

    public void testAnyProductOriginIsNotADirectClient() {
        assertThat(
            DirectClient.classify(
                "logstash-filter-elasticsearch",
                "es=9.1.0,rb=3.1.4,t=8.4.0,jv=21,jr=9.4.8.0,mc=0.9.1",
                "logstash/9.1.0 (OS=Linux-6.1.0-amd64; JVM=Eclipse Adoptium-21.0.4) logstash-filter-elasticsearch/4.1.0"
            ),
            is(DirectClient.ELASTIC_PRODUCT)
        );
        assertThat(
            DirectClient.classify("beats", null, "Elastic-Filebeat/9.1.0 (linux; amd64; abc123; 2026-01-01 00:00:00 +0000 UTC)"),
            is(DirectClient.ELASTIC_PRODUCT)
        );
        assertThat(
            DirectClient.classify("kibana", null, "Grafana/13.3.0 (linux; amd64) prometheus/13.2.6"),
            is(DirectClient.ELASTIC_PRODUCT)
        );
    }

    public void testMissingHeaders() {
        assertThat(DirectClient.classify(null, null, null), is(DirectClient.OTHER));
        assertThat(DirectClient.classify("", "", ""), is(DirectClient.OTHER));
        assertThat(DirectClient.classify("   ", "   ", "   "), is(DirectClient.OTHER));
    }

    public void testBlankProductOriginIsIgnored() {
        assertThat(DirectClient.classify(" ", "es=9.5.0,py=3.12.1,t=9.4.2,ur=2.2.3", null), is(DirectClient.PYTHON));
    }

    public void testUnknownUserAgent() {
        assertThat(classifyUserAgent("python-requests/2.32.3"), is(DirectClient.OTHER));
        assertThat(classifyUserAgent("Go-http-client/1.1"), is(DirectClient.OTHER));
        assertThat(classifyUserAgent("PostmanRuntime/7.39.0"), is(DirectClient.OTHER));
        assertThat(classifyUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"), is(DirectClient.OTHER));
    }

    public void testMetaHeaderWithoutLanguageFallsBackToUserAgent() {
        assertThat(DirectClient.classify(null, "es=9.1.0,t=9.1.0", "curl/8.7.1"), is(DirectClient.CURL));
        assertThat(DirectClient.classify(null, "ent=8.0.0,zz=1", null), is(DirectClient.OTHER));
    }

    public void testNearMissNamesAreNotClassified() {
        assertThat(classifyUserAgent("Grafanaxyz/1.0"), is(DirectClient.OTHER));
        assertThat(classifyUserAgent("NotGrafana/1.0"), is(DirectClient.OTHER));
        assertThat(classifyUserAgent("xcurl/8.7.1"), is(DirectClient.OTHER));
        assertThat(classifyUserAgent("curlx/1.0"), is(DirectClient.OTHER));
        assertThat(classifyUserAgent("curl"), is(DirectClient.OTHER));
        assertThat(classifyUserAgent("elasticsearch-pyx/1.0"), is(DirectClient.OTHER));
        assertThat(classifyUserAgent("Kibanana/9.5.1"), is(DirectClient.OTHER));
        assertThat(classifyMeta("es=9.1.0,pyx=3.12.1"), is(DirectClient.OTHER));
        assertThat(classifyMeta("es=9.1.0,xpy=3.12.1"), is(DirectClient.OTHER));
        assertThat(classifyMeta("es=py,t=js"), is(DirectClient.OTHER));
    }

    public void testMalformedMetaHeaderDoesNotThrow() {
        for (String meta : List.of(",,,", "=", "py", "=py", "==,,==", "es", "\u0000\u0001\u007f", "es=9,éè=1", "es\n=9,t=1")) {
            assertThat(meta, classifyMeta(meta), is(DirectClient.OTHER));
        }
    }

    public void testLanguageKeyWithEmptyValueIsClassified() {
        assertThat(classifyMeta("es=9.1.0,py="), is(DirectClient.PYTHON));
    }

    public void testVeryLongHeaders() {
        String padding = "x".repeat(1024 * 1024);
        assertThat(classifyMeta("es=9.5.0,py=3.12.1,z=" + padding), is(DirectClient.PYTHON));
        assertThat(classifyMeta("es=" + padding + ",py=3.12.1"), is(DirectClient.OTHER));
        assertThat(classifyUserAgent("curl/" + padding), is(DirectClient.CURL));
        assertThat(classifyUserAgent(padding + "curl/8.7.1"), is(DirectClient.OTHER));
    }

    public void testMetaHeaderScanStopsAtLimit() {
        String fitsExactly = "es=" + "x".repeat(DirectClient.MAX_SCANNED_CHARS - "es=".length() - ",py=".length()) + ",py=";
        assertThat(fitsExactly.length(), is(DirectClient.MAX_SCANNED_CHARS));
        assertThat(classifyMeta(fitsExactly + "3.12.1"), is(DirectClient.PYTHON));
        assertThat(classifyMeta("x" + fitsExactly + "3.12.1"), is(DirectClient.OTHER));
    }

    public void testRandomInputNeverThrows() {
        for (int i = 0; i < 1000; i++) {
            String productOrigin = randomBoolean() ? null : randomRealisticUnicodeOfLengthBetween(0, 64);
            String meta = randomBoolean() ? null : randomMetaLikeString();
            String userAgent = randomBoolean() ? null : randomUnicodeOfLengthBetween(0, 1024);
            assertThat(DirectClient.classify(productOrigin, meta, userAgent), notNullValue());
        }
    }

    public void testClassifyRestRequest() {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of(
                DirectClient.CLIENT_META_HEADER,
                List.of("es=9.5.0,py=3.12.1,t=9.4.2,ur=2.2.3"),
                DirectClient.USER_AGENT_HEADER,
                List.of("elasticsearch-py/9.5.0 (Python/3.12.1; elastic-transport/9.4.2)")
            )
        ).build();
        assertThat(DirectClient.classify(request), is(DirectClient.PYTHON));
    }

    public void testClassifyRestRequestFromKibana() {
        FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of(
                Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER,
                List.of("kibana"),
                DirectClient.CLIENT_META_HEADER,
                List.of(KIBANA_META),
                DirectClient.USER_AGENT_HEADER,
                List.of("Kibana/9.5.1")
            )
        ).build();
        assertThat(DirectClient.classify(request), is(DirectClient.ELASTIC_PRODUCT));
    }

    public void testClassifyRestRequestWithoutHeaders() {
        assertThat(DirectClient.classify(new FakeRestRequest()), is(DirectClient.OTHER));
    }

    public void testMetricNamesAreTheClosedList() {
        assertThat(
            Arrays.stream(DirectClient.values()).map(DirectClient::toString).toList(),
            containsInAnyOrder(
                "python",
                "javascript",
                "java",
                "go",
                "dotnet",
                "ruby",
                "php",
                "rust",
                "grafana",
                "curl",
                "elastic_product",
                "other"
            )
        );
    }

    private static DirectClient classifyMeta(String meta) {
        return DirectClient.classify(null, meta, null);
    }

    private static DirectClient classifyUserAgent(String userAgent) {
        return DirectClient.classify(null, null, userAgent);
    }

    private static String randomMetaLikeString() {
        StringBuilder sb = new StringBuilder();
        int pairs = between(0, 10);
        for (int i = 0; i < pairs; i++) {
            if (i > 0) {
                sb.append(randomFrom(",", ",,", "", "="));
            }
            sb.append(randomFrom("es", "py", "js", "jv", "", "=", randomAlphaOfLengthBetween(1, 4), randomUnicodeOfLengthBetween(0, 4)));
            sb.append(randomFrom("=", "", "=="));
            sb.append(randomUnicodeOfLengthBetween(0, 8));
        }
        return sb.toString();
    }
}
