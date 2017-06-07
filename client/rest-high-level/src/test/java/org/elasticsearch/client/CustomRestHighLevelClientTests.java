/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.client.ESRestHighLevelClientTestCase.execute;
import static org.elasticsearch.client.Request.REQUEST_BODY_CONTENT_TYPE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Test and demonstrates how {@link RestHighLevelClient} can be extended to support custom requests and responses.
 */
public class CustomRestHighLevelClientTests extends ESTestCase {

    private static final String ENDPOINT = "/_custom";

    private CustomRestClient restHighLevelClient;

    @Before
    public void iniClients() throws IOException {
        if (restHighLevelClient == null) {
            final RestClient restClient = mock(RestClient.class);
            restHighLevelClient = new CustomRestClient(restClient);

            doAnswer(mock -> performRequest((HttpEntity) mock.getArguments()[3]))
                    .when(restClient)
                    .performRequest(eq(HttpGet.METHOD_NAME), eq(ENDPOINT), anyMapOf(String.class, String.class), anyObject(), anyVararg());

            doAnswer(mock -> performRequestAsync((HttpEntity) mock.getArguments()[3], (ResponseListener) mock.getArguments()[4]))
                    .when(restClient)
                    .performRequestAsync(eq(HttpGet.METHOD_NAME), eq(ENDPOINT), anyMapOf(String.class, String.class),
                            any(HttpEntity.class), any(ResponseListener.class), anyVararg());
        }
    }

    public void testCustomRequest() throws IOException {
        final int length = randomIntBetween(1, 10);

        CustomRequest customRequest = new CustomRequest();
        customRequest.setValue(randomAlphaOfLength(length));

        CustomResponse customResponse = execute(customRequest, restHighLevelClient::custom, restHighLevelClient::customAsync);
        assertEquals(length, customResponse.getLength());
        assertEquals(expectedStatus(length), customResponse.status());
    }

    private Void performRequestAsync(HttpEntity httpEntity, ResponseListener responseListener) {
        try {
            responseListener.onSuccess(performRequest(httpEntity));
        } catch (IOException e) {
            responseListener.onFailure(e);
        }
        return null;
    }

    private Response performRequest(HttpEntity httpEntity) throws IOException {
        try (XContentParser parser = createParser(REQUEST_BODY_CONTENT_TYPE.xContent(), httpEntity.getContent())) {
            CustomRequest request = CustomRequest.fromXContent(parser);

            int length = request.getValue() != null ? request.getValue().length() : -1;
            CustomResponse response = new CustomResponse(length);

            ProtocolVersion protocol = new ProtocolVersion("HTTP", 1, 1);
            RestStatus status = response.status();
            HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocol, status.getStatus(), status.name()));

            BytesRef bytesRef = XContentHelper.toXContent(response, XContentType.JSON, false).toBytesRef();
            httpResponse.setEntity(new ByteArrayEntity(bytesRef.bytes, ContentType.APPLICATION_JSON));

            RequestLine requestLine = new BasicRequestLine(HttpGet.METHOD_NAME, ENDPOINT, protocol);
            return new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
        }
    }

    /**
     * A custom high level client that provides methods to execute a custom request and get its associate response back.
     */
    static class CustomRestClient extends RestHighLevelClient {

        private CustomRestClient(RestClient restClient) {
            super(restClient);
        }

        public CustomResponse custom(CustomRequest customRequest, Header... headers) throws IOException {
            return performRequest(customRequest, this::toRequest, this::toResponse, emptySet(), headers);
        }

        public void customAsync(CustomRequest customRequest, ActionListener<CustomResponse> listener, Header... headers) {
            performRequestAsync(customRequest, this::toRequest, this::toResponse, listener, emptySet(), headers);
        }

        Request toRequest(CustomRequest customRequest) throws IOException {
            BytesRef source = XContentHelper.toXContent(customRequest, REQUEST_BODY_CONTENT_TYPE, false).toBytesRef();
            ContentType contentType = ContentType.create(REQUEST_BODY_CONTENT_TYPE.mediaType());
            HttpEntity entity = new ByteArrayEntity(source.bytes, source.offset, source.length, contentType);
            return new Request(HttpGet.METHOD_NAME, ENDPOINT, emptyMap(), entity);
        }

        CustomResponse toResponse(Response response) throws IOException {
            return parseEntity(response.getEntity(), CustomResponse::fromXContent);
        }
    }

    static class CustomRequest extends ActionRequest implements ToXContentObject {

        private String value;

        public CustomRequest() {
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (Strings.hasLength(value) == false) {
                return ValidateActions.addValidationError("value is missing", null);
            }
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("value", value).endObject();
        }

        private static final ObjectParser<CustomRequest, Void> PARSER = new ObjectParser<>("custom_request", CustomRequest::new);
        static {
            PARSER.declareString(CustomRequest::setValue, new ParseField("value"));
        }

        static CustomRequest fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    static class CustomResponse extends ActionResponse implements StatusToXContentObject {

        private final int length;

        CustomResponse(int length) {
            this.length = length;
        }

        public int getLength() {
            return length;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("length", length).endObject();
        }

        @Override
        public RestStatus status() {
            return expectedStatus(getLength());
        }

        private static final ConstructingObjectParser<CustomResponse, Void> PARSER =
                new ConstructingObjectParser<>("custom_response", args -> new CustomResponse((int) args[0]));
        static {
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField("length"));
        }

        static CustomResponse fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }

    private static RestStatus expectedStatus(int length) {
        return length > 5 ? RestStatus.OK :  RestStatus.BAD_REQUEST;
    }
}