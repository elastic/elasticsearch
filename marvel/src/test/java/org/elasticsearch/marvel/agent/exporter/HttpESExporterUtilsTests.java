/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;


public class HttpESExporterUtilsTests extends ESTestCase {

    @Test
    public void testLoadTemplate() {
        byte[] template = HttpESExporterUtils.loadDefaultTemplate();
        assertNotNull(template);
        assertThat(template.length, Matchers.greaterThan(0));
    }

    @Test
    public void testParseTemplateVersionFromByteArrayTemplate() throws IOException {
        byte[] template = HttpESExporterUtils.loadDefaultTemplate();
        assertNotNull(template);

        Version version = HttpESExporterUtils.parseTemplateVersion(template);
        assertNotNull(version);
    }

    @Test
    public void testParseTemplateVersionFromStringTemplate() throws IOException {
        List<String> templates = new ArrayList<>();
        templates.add("{\"marvel_version\": \"1.4.0.Beta1\"}");
        templates.add("{\"marvel_version\": \"1.6.2-SNAPSHOT\"}");
        templates.add("{\"marvel_version\": \"1.7.1\"}");
        templates.add("{\"marvel_version\": \"2.0.0-beta1\"}");
        templates.add("{\"marvel_version\": \"2.0.0\"}");
        templates.add("{  \"template\": \".marvel*\",  \"settings\": {    \"marvel_version\": \"2.0.0-beta1-SNAPSHOT\", \"index.number_of_shards\": 1 } }");

        for (String template : templates) {
            Version version = HttpESExporterUtils.parseTemplateVersion(template);
            assertNotNull(version);
        }

        Version version = HttpESExporterUtils.parseTemplateVersion("{\"marvel.index_format\": \"7\"}");
        assertNull(version);
    }

    @Test
    public void testParseVersion() throws IOException {
        assertNotNull(HttpESExporterUtils.parseVersion(HttpESExporterUtils.MARVEL_VERSION_FIELD, "{\"marvel_version\": \"2.0.0-beta1\"}"));
        assertNotNull(HttpESExporterUtils.parseVersion(HttpESExporterUtils.MARVEL_VERSION_FIELD, "{\"marvel_version\": \"2.0.0\"}"));
        assertNotNull(HttpESExporterUtils.parseVersion(HttpESExporterUtils.MARVEL_VERSION_FIELD, "{\"marvel_version\": \"1.5.2\"}"));
        assertNotNull(HttpESExporterUtils.parseVersion(HttpESExporterUtils.MARVEL_VERSION_FIELD, "{  \"template\": \".marvel*\",  \"settings\": {    \"marvel_version\": \"2.0.0-beta1-SNAPSHOT\", \"index.number_of_shards\": 1 } }"));
        assertNull(HttpESExporterUtils.parseVersion(HttpESExporterUtils.MARVEL_VERSION_FIELD, "{\"marvel.index_format\": \"7\"}"));
        assertNull(HttpESExporterUtils.parseVersion(HttpESExporterUtils.MARVEL_VERSION_FIELD + "unkown", "{\"marvel_version\": \"1.5.2\"}"));
    }


    @Test
    public void testHostParsing() throws MalformedURLException, URISyntaxException {
        URL url = HttpESExporterUtils.parseHostWithPath("localhost:9200", "");
        verifyUrl(url, "http", "localhost", 9200, "/");

        url = HttpESExporterUtils.parseHostWithPath("localhost", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk");

        url = HttpESExporterUtils.parseHostWithPath("http://localhost:9200", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk");

        url = HttpESExporterUtils.parseHostWithPath("http://localhost", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk");

        url = HttpESExporterUtils.parseHostWithPath("https://localhost:9200", "_bulk");
        verifyUrl(url, "https", "localhost", 9200, "/_bulk");

        url = HttpESExporterUtils.parseHostWithPath("https://boaz-air.local:9200", "_bulk");
        verifyUrl(url, "https", "boaz-air.local", 9200, "/_bulk");

        url = HttpESExporterUtils.parseHostWithPath("boaz:test@localhost:9200", "");
        verifyUrl(url, "http", "localhost", 9200, "/", "boaz:test");

        url = HttpESExporterUtils.parseHostWithPath("boaz:test@localhost", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk", "boaz:test");

        url = HttpESExporterUtils.parseHostWithPath("http://boaz:test@localhost:9200", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk", "boaz:test");

        url = HttpESExporterUtils.parseHostWithPath("http://boaz:test@localhost", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk", "boaz:test");

        url = HttpESExporterUtils.parseHostWithPath("https://boaz:test@localhost:9200", "_bulk");
        verifyUrl(url, "https", "localhost", 9200, "/_bulk", "boaz:test");

        url = HttpESExporterUtils.parseHostWithPath("boaz:test@localhost:9200/suburl", "");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/", "boaz:test");

        url = HttpESExporterUtils.parseHostWithPath("boaz:test@localhost:9200/suburl/", "");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/", "boaz:test");

        url = HttpESExporterUtils.parseHostWithPath("localhost/suburl", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/_bulk");

        url = HttpESExporterUtils.parseHostWithPath("http://boaz:test@localhost:9200/suburl/suburl1", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/suburl1/_bulk", "boaz:test");

        url = HttpESExporterUtils.parseHostWithPath("http://boaz:test@localhost/suburl", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/_bulk", "boaz:test");

        url = HttpESExporterUtils.parseHostWithPath("https://boaz:test@localhost:9200/suburl", "_bulk");
        verifyUrl(url, "https", "localhost", 9200, "/suburl/_bulk", "boaz:test");

        url = HttpESExporterUtils.parseHostWithPath("https://user:test@server_with_underscore:9300", "_bulk");
        verifyUrl(url, "https", "server_with_underscore", 9300, "/_bulk", "user:test");

        url = HttpESExporterUtils.parseHostWithPath("user:test@server_with_underscore:9300", "_bulk");
        verifyUrl(url, "http", "server_with_underscore", 9300, "/_bulk", "user:test");

        url = HttpESExporterUtils.parseHostWithPath("server_with_underscore:9300", "_bulk");
        verifyUrl(url, "http", "server_with_underscore", 9300, "/_bulk");

        url = HttpESExporterUtils.parseHostWithPath("server_with_underscore", "_bulk");
        verifyUrl(url, "http", "server_with_underscore", 9200, "/_bulk");

        url = HttpESExporterUtils.parseHostWithPath("https://user:test@server-dash:9300", "_bulk");
        verifyUrl(url, "https", "server-dash", 9300, "/_bulk", "user:test");

        url = HttpESExporterUtils.parseHostWithPath("user:test@server-dash:9300", "_bulk");
        verifyUrl(url, "http", "server-dash", 9300, "/_bulk", "user:test");

        url = HttpESExporterUtils.parseHostWithPath("server-dash:9300", "_bulk");
        verifyUrl(url, "http", "server-dash", 9300, "/_bulk");

        url = HttpESExporterUtils.parseHostWithPath("server-dash", "_bulk");
        verifyUrl(url, "http", "server-dash", 9200, "/_bulk");
    }

    @Test
    public void sanitizeUrlPadTest() throws UnsupportedEncodingException {
        String pwd = URLEncoder.encode(randomRealisticUnicodeOfCodepointLengthBetween(3, 20), "UTF-8");
        String[] inputs = new String[]{
                "https://boaz:" + pwd + "@hostname:9200",
                "http://boaz:" + pwd + "@hostname:9200",
                "boaz:" + pwd + "@hostname",
                "boaz:" + pwd + "@hostname/hello",
                "Parse exception in [boaz:" + pwd + "@hostname:9200,boaz1:" + pwd + "@hostname \n" +
                        "caused: by exception ,boaz1:" + pwd + "@hostname",
                "failed to upload index template, stopping export\n" +
                        "java.lang.RuntimeException: failed to load/verify index template\n" +
                        "    at org.elasticsearch.marvel.agent.exporter.ESExporter.checkAndUploadIndexTemplate(ESExporter.java:525)\n" +
                        "    at org.elasticsearch.marvel.agent.exporter.ESExporter.openExportingConnection(ESExporter.java:213)\n" +
                        "    at org.elasticsearch.marvel.agent.exporter.ESExporter.exportXContent(ESExporter.java:285)\n" +
                        "    at org.elasticsearch.marvel.agent.exporter.ESExporter.exportClusterStats(ESExporter.java:206)\n" +
                        "    at org.elasticsearch.marvel.agent.AgentService$ExportingWorker.exportClusterStats(AgentService.java:288)\n" +
                        "    at org.elasticsearch.marvel.agent.AgentService$ExportingWorker.run(AgentService.java:245)\n" +
                        "    at java.lang.Thread.run(Thread.java:745)\n" +
                        "Caused by: java.io.IOException: Server returned HTTP response code: 401 for URL: http://marvel_exporter:" + pwd + "@localhost:9200/_template/marvel\n" +
                        "    at sun.reflect.GeneratedConstructorAccessor3.createMarvelDoc(Unknown Source)\n" +
                        "    at sun.reflect.DelegatingConstructorAccessorImpl.createMarvelDoc(DelegatingConstructorAccessorImpl.java:45)\n" +
                        "    at java.lang.reflect.Constructor.createMarvelDoc(Constructor.java:526)\n" +
                        "    at sun.net.www.protocol.http.HttpURLConnection$6.run(HttpURLConnection.java:1675)\n" +
                        "    at sun.net.www.protocol.http.HttpURLConnection$6.run(HttpURLConnection.java:1673)\n" +
                        "    at java.security.AccessController.doPrivileged(Native Method)\n" +
                        "    at sun.net.www.protocol.http.HttpURLConnection.getChainedException(HttpURLConnection.java:1671)\n" +
                        "    at sun.net.www.protocol.http.HttpURLConnection.getInputStream(HttpURLConnection.java:1244)\n" +
                        "    at org.elasticsearch.marvel.agent.exporter.ESExporter.checkAndUploadIndexTemplate(ESExporter.java:519)\n" +
                        "    ... 6 more\n" +
                        "Caused by: java.io.IOException: Server returned HTTP response code: 401 for URL: http://marvel_exporter:" + pwd + "@localhost:9200/_template/marvel\n" +
                        "    at sun.net.www.protocol.http.HttpURLConnection.getInputStream(HttpURLConnection.java:1626)\n" +
                        "    at java.net.HttpURLConnection.getResponseCode(HttpURLConnection.java:468)\n" +
                        "    at org.elasticsearch.marvel.agent.exporter.ESExporter.checkAndUploadIndexTemplate(ESExporter.java:514)\n" +
                        "    ... 6 more"
        };

        for (String input : inputs) {
            String sanitized = HttpESExporterUtils.santizeUrlPwds(input);
            assertThat(sanitized, not(containsString(pwd)));
        }
    }

    void verifyUrl(URL url, String protocol, String host, int port, String path) throws URISyntaxException {
        assertThat(url.getProtocol(), equalTo(protocol));
        assertThat(url.getHost(), equalTo(host));
        assertThat(url.getPort(), equalTo(port));
        assertThat(url.toURI().getPath(), equalTo(path));
    }

    void verifyUrl(URL url, String protocol, String host, int port, String path, String userInfo) throws URISyntaxException {
        verifyUrl(url, protocol, host, port, path);
        assertThat(url.getUserInfo(), equalTo(userInfo));

    }
}
