package org.elasticsearch.river.couchdb.http;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.*;
import static org.elasticsearch.common.collect.Maps.*;
import static org.elasticsearch.common.io.Streams.*;

public class Http {

    public HttpResult put(URI uri) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");

        InputStreamReader reader = new InputStreamReader(connection.getInputStream());
        String result = copyToString(reader);

        return new HttpResult(uri, "PUT", connection.getResponseCode(), responseHeadersFrom(connection), result);
    }

    private List<Header> responseHeadersFrom(HttpURLConnection httpCon) {
        List<Header> headers = newArrayList();
        for (Map.Entry<String, List<String>> responseHeader : httpCon.getHeaderFields().entrySet()) {
            for (String headerValue : responseHeader.getValue()) {

                String name = responseHeader.getKey();
                if (name != null) {
                    headers.add(new Header(name, headerValue));
                }
            }
        }
        return headers;
    }

    private void setContentTypeOn(HttpURLConnection connection, String contentType) {
        connection.setRequestProperty("Content-Type", contentType);
    }

    public HttpResult put(URI uri, String content, String contentType) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        setContentTypeOn(connection, contentType);

        OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());

        try {
            out.write(content);
        }
        finally {
            out.close();
        }

        int responseCode = connection.getResponseCode();

        InputStreamReader reader = new InputStreamReader(connection.getInputStream());
        try {
            String result = copyToString(reader);
            return new HttpResult(uri, "PUT", responseCode, responseHeadersFrom(connection), result);
        }
        finally {
            reader.close();
        }

    }

    public static class Header {
        public final String name;
        public final String value;

        public Header(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }

    public static class HttpResult {
        public final String body;
        public final URI uri;
        public final int status;
        public final Map<String, Header> headers = newHashMap();
        public final String method;

        public HttpResult(URI url, String method, int status, List<Header> headers, String body) {
            this.status = status;
            this.method = method;
            this.uri = url;
            this.body = body;
            for (Header header : headers) {
                this.headers.put(header.name.toLowerCase(), header);
            }
        }

        public Header header(String name) {
            return headers.get(name.toLowerCase());
        }

        public boolean hasCacheExpiryHeader() {
            return headers.containsKey("Expires".toLowerCase());
        }

        public boolean hasLastModifiedHeader() {
            return headers.containsKey("Last-Modified".toLowerCase());
        }

        public boolean ok() {
            return status == HttpURLConnection.HTTP_OK || status == HttpURLConnection.HTTP_CREATED;
        }

        public boolean notFound() {
            return status == HttpURLConnection.HTTP_NOT_FOUND;
        }

        public boolean error() {
            return status >= HttpURLConnection.HTTP_INTERNAL_ERROR;
        }

        public boolean badGateway() {
            return status == HttpURLConnection.HTTP_BAD_GATEWAY;
        }

        public boolean inConflict() {
            return status == HttpURLConnection.HTTP_CONFLICT;
        }

        public boolean badRequest() {
            return status == HttpURLConnection.HTTP_BAD_REQUEST;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("HttpResult[");

            sb.append("url=");
            sb.append(uri);
            sb.append(",\nstatus=");
            sb.append(status);
            sb.append("\nheaders=");
            for (Header header : headers.values()) {
                sb.append("\n\t[name=");
                sb.append(header.name);
                sb.append(",value=");
                sb.append(header.value);
                sb.append("]");
            }
            sb.append(",\nbody=");
            sb.append(body);
            sb.append("]");

            return sb.toString();
        }

        public String contentType() {
            return header("Content-Type").value;
        }

    }
}
