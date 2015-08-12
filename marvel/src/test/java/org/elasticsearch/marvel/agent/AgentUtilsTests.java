/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent;

import org.elasticsearch.marvel.agent.support.AgentUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;


public class AgentUtilsTests extends ESTestCase {

    @Test
    public void testVersionIsExtractableFromIndexTemplate() throws IOException {
        byte[] template = StreamsUtils.copyToBytesFromClasspath("/marvel_index_template.json");
        MatcherAssert.assertThat(AgentUtils.parseIndexVersionFromTemplate(template), Matchers.greaterThan(0));
    }

    @Test
    public void testHostParsing() throws MalformedURLException, URISyntaxException {
        URL url = AgentUtils.parseHostWithPath("localhost:9200", "");
        verifyUrl(url, "http", "localhost", 9200, "/");

        url = AgentUtils.parseHostWithPath("localhost", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk");

        url = AgentUtils.parseHostWithPath("http://localhost:9200", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk");

        url = AgentUtils.parseHostWithPath("http://localhost", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk");

        url = AgentUtils.parseHostWithPath("https://localhost:9200", "_bulk");
        verifyUrl(url, "https", "localhost", 9200, "/_bulk");

        url = AgentUtils.parseHostWithPath("https://boaz-air.local:9200", "_bulk");
        verifyUrl(url, "https", "boaz-air.local", 9200, "/_bulk");

        url = AgentUtils.parseHostWithPath("boaz:test@localhost:9200", "");
        verifyUrl(url, "http", "localhost", 9200, "/", "boaz:test");

        url = AgentUtils.parseHostWithPath("boaz:test@localhost", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk", "boaz:test");

        url = AgentUtils.parseHostWithPath("http://boaz:test@localhost:9200", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk", "boaz:test");

        url = AgentUtils.parseHostWithPath("http://boaz:test@localhost", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/_bulk", "boaz:test");

        url = AgentUtils.parseHostWithPath("https://boaz:test@localhost:9200", "_bulk");
        verifyUrl(url, "https", "localhost", 9200, "/_bulk", "boaz:test");

        url = AgentUtils.parseHostWithPath("boaz:test@localhost:9200/suburl", "");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/", "boaz:test");

        url = AgentUtils.parseHostWithPath("boaz:test@localhost:9200/suburl/", "");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/", "boaz:test");

        url = AgentUtils.parseHostWithPath("localhost/suburl", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/_bulk");

        url = AgentUtils.parseHostWithPath("http://boaz:test@localhost:9200/suburl/suburl1", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/suburl1/_bulk", "boaz:test");

        url = AgentUtils.parseHostWithPath("http://boaz:test@localhost/suburl", "_bulk");
        verifyUrl(url, "http", "localhost", 9200, "/suburl/_bulk", "boaz:test");

        url = AgentUtils.parseHostWithPath("https://boaz:test@localhost:9200/suburl", "_bulk");
        verifyUrl(url, "https", "localhost", 9200, "/suburl/_bulk", "boaz:test");

        url = AgentUtils.parseHostWithPath("https://user:test@server_with_underscore:9300", "_bulk");
        verifyUrl(url, "https", "server_with_underscore", 9300, "/_bulk", "user:test");

        url = AgentUtils.parseHostWithPath("user:test@server_with_underscore:9300", "_bulk");
        verifyUrl(url, "http", "server_with_underscore", 9300, "/_bulk", "user:test");

        url = AgentUtils.parseHostWithPath("server_with_underscore:9300", "_bulk");
        verifyUrl(url, "http", "server_with_underscore", 9300, "/_bulk");

        url = AgentUtils.parseHostWithPath("server_with_underscore", "_bulk");
        verifyUrl(url, "http", "server_with_underscore", 9200, "/_bulk");

        url = AgentUtils.parseHostWithPath("https://user:test@server-dash:9300", "_bulk");
        verifyUrl(url, "https", "server-dash", 9300, "/_bulk", "user:test");

        url = AgentUtils.parseHostWithPath("user:test@server-dash:9300", "_bulk");
        verifyUrl(url, "http", "server-dash", 9300, "/_bulk", "user:test");

        url = AgentUtils.parseHostWithPath("server-dash:9300", "_bulk");
        verifyUrl(url, "http", "server-dash", 9300, "/_bulk");

        url = AgentUtils.parseHostWithPath("server-dash", "_bulk");
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
            String sanitized = AgentUtils.santizeUrlPwds(input);
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
