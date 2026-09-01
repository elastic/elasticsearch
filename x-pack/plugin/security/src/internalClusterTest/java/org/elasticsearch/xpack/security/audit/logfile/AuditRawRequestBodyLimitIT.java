/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * End-to-end check that a protobuf request whose body exceeds the audit size limit is rejected with HTTP 413 by the security
 * interceptor before the target handler runs.
 */
@ClusterScope(scope = TEST, numDataNodes = 1)
public class AuditRawRequestBodyLimitIT extends SecurityIntegTestCase {

    private static final String ROUTE = "/_test/protobuf_accept";
    private static final String CONTENT_TYPE = "application/x-protobuf";

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), ProtobufAcceptingTestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("xpack.security.audit.enabled", true)
            .put(LoggingAuditTrail.INCLUDE_REQUEST_BODY.getKey(), true)
            .put(LoggingAuditTrail.MAX_REQUEST_BODY_SIZE.getKey(), "10b")
            .putList(LoggingAuditTrail.INCLUDE_EVENT_SETTINGS.getKey(), "authentication_success")
            .build();
    }

    public void testProtobufBodyExceedingLimitReturns413() {
        final byte[] body = randomByteArrayOfLength(randomIntBetween(64, 256));
        final Request request = new Request("POST", ROUTE);
        request.setEntity(new ByteArrayEntity(body, ContentType.create(CONTENT_TYPE)));
        request.setOptions(authOptions());

        final ResponseException ex = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(413));
        assertThat(ex.getMessage(), containsString("audit body size limit"));
        assertThat(ex.getMessage(), containsString(LoggingAuditTrail.MAX_REQUEST_BODY_SIZE.getKey()));
    }

    public void testProtobufBodyWithinLimitReaches200Handler() throws Exception {
        // base64 length of 1-6 source bytes is 4 or 8 chars, both within the 10b MAX_REQUEST_BODY_SIZE
        final byte[] body = randomByteArrayOfLength(randomIntBetween(1, 6));
        final Request request = new Request("POST", ROUTE);
        request.setEntity(new ByteArrayEntity(body, ContentType.create(CONTENT_TYPE)));
        request.setOptions(authOptions());

        final Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    private static RequestOptions authOptions() {
        final RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader(
            "Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(
                SecuritySettingsSource.ES_TEST_ROOT_USER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
            )
        );
        return options.build();
    }

    /**
     * Test-only handler that accepts {@code application/x-protobuf} so the request reaches the security interceptor rather than
     * being rejected upstream by {@link org.elasticsearch.rest.RestController} as an unsupported media type.
     */
    public static class ProtobufAcceptingTestPlugin extends Plugin implements ActionPlugin {
        @Override
        public Collection<RestHandler> getRestHandlers(
            RestHandlersServices restHandlersServices,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
        ) {
            return List.of(new BaseRestHandler() {
                @Override
                public String getName() {
                    return "test_protobuf_accept";
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.POST, ROUTE));
                }

                @Override
                public boolean mediaTypesValid(RestRequest request) {
                    return request.getParsedContentType() != null
                        && CONTENT_TYPE.equals(request.getParsedContentType().mediaTypeWithoutParameters());
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    return channel -> channel.sendResponse(new RestResponse(RestStatus.OK, CONTENT_TYPE, new BytesArray(new byte[0])));
                }
            });
        }
    }
}
