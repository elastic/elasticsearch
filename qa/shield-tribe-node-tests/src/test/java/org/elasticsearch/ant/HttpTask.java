/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.ant;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.elasticsearch.common.Base64;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class HttpTask extends Task {

    private String uri;
    private String method;
    private String body;

    private String username;
    private String password;

    @Override
    public void execute() throws BuildException {
        int responseCode = executeHttpRequest();
        getProject().log("response code=" + responseCode);
    }

    protected int executeHttpRequest() {
        try {
            URI uri = new URI(this.uri);
            URL url = uri.toURL();
            getProject().log("url=" + url);
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            if (method != null) {
                urlConnection.setRequestMethod(method);
            }
            if (username != null) {
                String basicAuth = "Basic " + Base64.encodeBytes((username + ":" + password).getBytes(StandardCharsets.UTF_8));
                urlConnection.setRequestProperty("Authorization", basicAuth);
            }
            if (body != null) {
                urlConnection.setDoOutput(true);
                urlConnection.setRequestProperty("Accept-Charset", StandardCharsets.UTF_8.name());
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8.name());
                urlConnection.setRequestProperty("Content-Length", String.valueOf(bytes.length));
                urlConnection.getOutputStream().write(bytes);
                urlConnection.getOutputStream().close();
            }
            urlConnection.connect();
            int responseCode = urlConnection.getResponseCode();
            urlConnection.disconnect();
            return responseCode;
        } catch (Exception e) {
            throw new BuildException(e);
        }
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
