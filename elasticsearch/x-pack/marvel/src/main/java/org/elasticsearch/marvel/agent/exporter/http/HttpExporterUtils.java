/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.http;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

public class HttpExporterUtils {

    public static URL parseHostWithPath(String host, String path) throws URISyntaxException, MalformedURLException {

        if (!host.contains("://")) {
            // prefix with http
            host = "http://" + host;
        }
        if (!host.endsWith("/")) {
            // make sure we can safely resolves sub paths and not replace parent folders
            host = host + "/";
        }

        URL hostUrl = new URL(host);

        if (hostUrl.getPort() == -1) {
            // url has no port, default to 9200 - sadly we need to rebuild..
            StringBuilder newUrl = new StringBuilder(hostUrl.getProtocol() + "://");
            newUrl.append(hostUrl.getHost()).append(":9200").append(hostUrl.toURI().getPath());
            hostUrl = new URL(newUrl.toString());

        }
        return new URL(hostUrl, path);

    }

}
