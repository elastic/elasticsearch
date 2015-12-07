/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.http;

import org.elasticsearch.test.ESTestCase;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.hamcrest.CoreMatchers.equalTo;


public class HttpExporterUtilsTests extends ESTestCase {

    public void testHostParsing() throws MalformedURLException, URISyntaxException {
        URL url = HttpExporterUtils.parseHostWithPath("localhost:9200", "");
        verifyUrl(url, "http", "localhost", 9200, "/");

        url = HttpExporterUtils.parseHostWithPath("localhost", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk");

        url = HttpExporterUtils.parseHostWithPath("http://localhost:9200", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk");

        url = HttpExporterUtils.parseHostWithPath("http://localhost", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk");

        url = HttpExporterUtils.parseHostWithPath("https://localhost:9200", "_bulk");
        verifyUrl(url, "https", "localhost", 9200, "/_bulk");

        url = HttpExporterUtils.parseHostWithPath("https://boaz-air.local:9200", "_bulk");
        verifyUrl(url, "https", "boaz-air.local", 9200, "/_bulk");

        url = HttpExporterUtils.parseHostWithPath("localhost:9200/suburl", "");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/");

        url = HttpExporterUtils.parseHostWithPath("localhost/suburl", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/_bulk");

        url = HttpExporterUtils.parseHostWithPath("http://localhost:9200/suburl/suburl1", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/suburl1/_bulk");

        url = HttpExporterUtils.parseHostWithPath("https://localhost:9200/suburl", "_bulk");
        verifyUrl(url, "https", "localhost", 9200, "/suburl/_bulk");

        url = HttpExporterUtils.parseHostWithPath("https://server_with_underscore:9300", "_bulk");
        verifyUrl(url, "https", "server_with_underscore", 9300, "/_bulk");

        url = HttpExporterUtils.parseHostWithPath("server_with_underscore:9300", "_bulk");
        verifyUrl(url, "http", "server_with_underscore", 9300, "/_bulk");

        url = HttpExporterUtils.parseHostWithPath("server_with_underscore", "_bulk");
        verifyUrl(url, "http", "server_with_underscore", 9200, "/_bulk");

        url = HttpExporterUtils.parseHostWithPath("https://server-dash:9300", "_bulk");
        verifyUrl(url, "https", "server-dash", 9300, "/_bulk");

        url = HttpExporterUtils.parseHostWithPath("server-dash:9300", "_bulk");
        verifyUrl(url, "http", "server-dash", 9300, "/_bulk");

        url = HttpExporterUtils.parseHostWithPath("server-dash", "_bulk");
        verifyUrl(url, "http", "server-dash", 9200, "/_bulk");
    }

    void verifyUrl(URL url, String protocol, String host, int port, String path) throws URISyntaxException {
        assertThat(url.getProtocol(), equalTo(protocol));
        assertThat(url.getHost(), equalTo(host));
        assertThat(url.getPort(), equalTo(port));
        assertThat(url.toURI().getPath(), equalTo(path));
    }
}
