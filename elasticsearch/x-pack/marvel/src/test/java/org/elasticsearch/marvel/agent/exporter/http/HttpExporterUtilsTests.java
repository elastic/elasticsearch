/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.http;

import org.elasticsearch.Version;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.support.VersionUtils;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils.MARVEL_VERSION_FIELD;
import static org.hamcrest.CoreMatchers.equalTo;


public class HttpExporterUtilsTests extends ESTestCase {
    public void testLoadTemplate() {
        byte[] template = MarvelTemplateUtils.loadDefaultTemplate();
        assertNotNull(template);
        assertThat(template.length, Matchers.greaterThan(0));
    }

    public void testParseTemplateVersionFromByteArrayTemplate() throws IOException {
        byte[] template = MarvelTemplateUtils.loadDefaultTemplate();
        assertNotNull(template);

        Version version = MarvelTemplateUtils.parseTemplateVersion(template);
        assertNotNull(version);
    }

    public void testParseTemplateVersionFromStringTemplate() throws IOException {
        List<String> templates = new ArrayList<>();
        templates.add("{\"marvel_version\": \"1.4.0.Beta1\"}");
        templates.add("{\"marvel_version\": \"1.6.2-SNAPSHOT\"}");
        templates.add("{\"marvel_version\": \"1.7.1\"}");
        templates.add("{\"marvel_version\": \"2.0.0-beta1\"}");
        templates.add("{\"marvel_version\": \"2.0.0\"}");
        templates.add("{  \"template\": \".marvel*\",  \"settings\": {    \"marvel_version\": \"2.0.0-beta1-SNAPSHOT\", \"index.number_of_shards\": 1 } }");

        for (String template : templates) {
            Version version = MarvelTemplateUtils.parseTemplateVersion(template);
            assertNotNull(version);
        }

        Version version = MarvelTemplateUtils.parseTemplateVersion("{\"marvel.index_format\": \"7\"}");
        assertNull(version);
    }

    public void testParseVersion() throws IOException {
        assertNotNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD, "{\"marvel_version\": \"2.0.0-beta1\"}"));
        assertNotNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD, "{\"marvel_version\": \"2.0.0\"}"));
        assertNotNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD, "{\"marvel_version\": \"1.5.2\"}"));
        assertNotNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD, "{  \"template\": \".marvel*\",  \"settings\": {    \"marvel_version\": \"2.0.0-beta1-SNAPSHOT\", \"index.number_of_shards\": 1 } }"));
        assertNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD, "{\"marvel.index_format\": \"7\"}"));
        assertNull(VersionUtils.parseVersion(MARVEL_VERSION_FIELD + "unkown", "{\"marvel_version\": \"1.5.2\"}"));
    }

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
