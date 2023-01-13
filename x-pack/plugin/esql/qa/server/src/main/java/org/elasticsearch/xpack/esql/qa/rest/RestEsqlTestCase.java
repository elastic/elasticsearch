/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;

public class RestEsqlTestCase extends ESRestTestCase {

    public static class RequestObjectBuilder {
        private final XContentBuilder builder;
        private boolean isBuilt = false;

        public RequestObjectBuilder() throws IOException {
            this(randomFrom(XContentType.values()));
        }

        public RequestObjectBuilder(XContentType type) throws IOException {
            builder = XContentBuilder.builder(type, emptySet(), emptySet());
            builder.startObject();
        }

        public RequestObjectBuilder query(String query) throws IOException {
            builder.field("query", query);
            return this;
        }

        public RequestObjectBuilder columnar(boolean columnar) throws IOException {
            builder.field("columnar", columnar);
            return this;
        }

        public RequestObjectBuilder timeZone(ZoneId zoneId) throws IOException {
            builder.field("time_zone", zoneId);
            return this;
        }

        public RequestObjectBuilder pragmas(Settings pragmas) throws IOException {
            builder.startObject("pragma");
            pragmas.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return this;
        }

        public RequestObjectBuilder build() throws IOException {
            if (isBuilt == false) {
                builder.endObject();
                isBuilt = true;
            }
            return this;
        }

        public OutputStream getOutputStream() throws IOException {
            if (isBuilt == false) {
                throw new IllegalStateException("object not yet built");
            }
            builder.flush();
            return builder.getOutputStream();
        }

        public XContentType contentType() {
            return builder.contentType();
        }

        public static RequestObjectBuilder jsonBuilder() throws IOException {
            return new RequestObjectBuilder(XContentType.JSON);
        }
    }

    public void testGetAnswer() throws IOException {
        Map<String, Object> answer = runEsql(builder().query("row a = 1, b = 2").build());
        assertEquals(2, answer.size());
        Map<String, String> colA = Map.of("name", "a", "type", "integer");
        Map<String, String> colB = Map.of("name", "b", "type", "integer");
        assertEquals(List.of(colA, colB), answer.get("columns"));
        assertEquals(List.of(List.of(1, 2)), answer.get("values"));
    }

    public void testUseUnknownIndex() throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> runEsql(builder().query("from doesNotExist").build()));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(e.getMessage(), containsString("verification_exception"));
        assertThat(e.getMessage(), containsString("Unknown index [doesNotExist]"));
    }

    public void testColumnarMode() throws IOException {
        int docCount = randomIntBetween(3, 10);
        bulkLoadTestData(docCount);

        boolean columnar = randomBoolean();
        var query = builder().query("from test | project keyword, integer");
        if (columnar || randomBoolean()) {
            query.columnar(columnar);
        }
        Map<String, Object> answer = runEsql(query.build());

        Map<String, String> colKeyword = Map.of("name", "keyword", "type", "keyword");
        Map<String, String> colInteger = Map.of("name", "integer", "type", "integer");
        assertEquals(List.of(colKeyword, colInteger), answer.get("columns"));

        if (columnar) {
            List<String> valKeyword = new ArrayList<>();
            List<Integer> valInteger = new ArrayList<>();
            for (int i = 0; i < docCount; i++) {
                valKeyword.add("keyword" + i);
                valInteger.add(i);
            }
            assertEquals(List.of(valKeyword, valInteger), answer.get("values"));
        } else {
            List<Object> rows = new ArrayList<>();
            for (int i = 0; i < docCount; i++) {
                rows.add(List.of("keyword" + i, i));
            }
            assertEquals(rows, answer.get("values"));
        }
    }

    public static Map<String, Object> runEsql(RequestObjectBuilder requestObject) throws IOException {
        Request request = new Request("POST", "/_esql");
        request.addParameter("error_trace", "true");
        String mediaType = requestObject.contentType().mediaTypeWithoutParameters();

        try (ByteArrayOutputStream bos = (ByteArrayOutputStream) requestObject.getOutputStream()) {
            request.setEntity(new NByteArrayEntity(bos.toByteArray(), ContentType.getByMimeType(mediaType)));
        }

        RequestOptions.Builder options = request.getOptions().toBuilder();
        if (randomBoolean()) {
            options.addHeader("Accept", mediaType);
        } else {
            request.addParameter("format", requestObject.contentType().queryParameter());
        }
        options.addHeader("Content-Type", mediaType);
        request.setOptions(options);

        Response response = client().performRequest(request);
        HttpEntity entity = response.getEntity();
        try (InputStream content = entity.getContent()) {
            XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
            assertEquals(requestObject.contentType(), xContentType);
            return XContentHelper.convertToMap(xContentType.xContent(), content, false);
        }
    }

    private static void bulkLoadTestData(int count) throws IOException {
        Request request = new Request("PUT", "/test");
        request.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "keyword": {
                    "type": "keyword"
                  },
                  "integer": {
                    "type": "integer"
                  }
                }
              }
            }""");
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());

        request = new Request("POST", "/test/_bulk");
        request.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < count; i++) {
            bulk.append(org.elasticsearch.core.Strings.format("""
                {"index":{"_id":"%s"}}
                {"keyword":"keyword%s", "integer":%s}
                """, i, i, i));
        }
        request.setJsonEntity(bulk.toString());
        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    private static RequestObjectBuilder builder() throws IOException {
        return new RequestObjectBuilder();
    }
}
