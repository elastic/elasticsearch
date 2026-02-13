/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class TransformHeadersTests extends AbstractSerializingTransformTestCase<TransformHeaders> {

    @Override
    protected TransformHeaders createTestInstance() {
        return randomHeaders();
    }

    @Override
    protected TransformHeaders mutateInstance(TransformHeaders instance) {
        Map<String, String> mutated = new HashMap<>(instance.allHeaders());
        mutated.put("mutated-" + randomAlphaOfLength(5), randomAlphaOfLength(10));
        return TransformHeaders.fromMap(mutated);
    }

    @Override
    protected TransformHeaders mutateInstanceForVersion(TransformHeaders instance, TransportVersion version) {
        return new TransformHeaders(instance.headers(), version.supports(TransformHeaders.WITH_UIAM_TOKEN) ? instance.uiamToken() : null);
    }

    @Override
    protected Writeable.Reader<TransformHeaders> instanceReader() {
        return TransformHeaders::new;
    }

    @Override
    protected TransformHeaders doParseInstance(XContentParser parser) throws IOException {
        Map<String, Object> map = parser.map();
        if (map.containsKey("headers")) {
            @SuppressWarnings("unchecked")
            Map<String, String> headers = (Map<String, String>) map.get("headers");
            return TransformHeaders.fromMap(headers);
        }
        return TransformHeaders.EMPTY;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return TO_XCONTENT_PARAMS;
    }

    private static TransformHeaders randomHeaders() {
        Map<String, String> headers = new HashMap<>();
        if (randomBoolean()) {
            headers.put(AuthenticationField.AUTHENTICATION_KEY, randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            headers.put(AuthenticationServiceField.RUN_AS_USER_HEADER, randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            headers.put(SecondaryAuthentication.THREAD_CTX_KEY, randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            headers.put("custom-" + randomAlphaOfLength(5), randomAlphaOfLength(10));
        }
        if (headers.isEmpty()) {
            headers.put("custom-" + randomAlphaOfLength(5), randomAlphaOfLength(10));
        }
        return TransformHeaders.fromMap(headers);
    }

    public void testEmpty() {
        TransformHeaders empty = TransformHeaders.fromMap(null);
        assertThat(empty, is(sameInstance(TransformHeaders.EMPTY)));
        assertThat(empty.headers(), is(anEmptyMap()));
        assertThat(empty.allHeaders(), is(anEmptyMap()));

        empty = TransformHeaders.fromMap(Collections.emptyMap());
        assertThat(empty, is(sameInstance(TransformHeaders.EMPTY)));
    }

    public void testFromMapPreservesAllHeaders() {
        Map<String, String> allHeaders = Map.of(
            AuthenticationField.AUTHENTICATION_KEY,
            "auth_value",
            AuthenticationServiceField.RUN_AS_USER_HEADER,
            "run_as_value",
            "custom-header",
            "custom_value"
        );

        TransformHeaders headers = TransformHeaders.fromMap(allHeaders);

        assertThat(headers.headers(), is(equalTo(allHeaders)));
        assertThat(headers.uiamToken(), is(nullValue()));
    }

    public void testFromMapAllSecurityHeaders() {
        Map<String, String> allHeaders = Map.of(
            AuthenticationField.AUTHENTICATION_KEY,
            "auth_value",
            AuthenticationServiceField.RUN_AS_USER_HEADER,
            "run_as_value",
            SecondaryAuthentication.THREAD_CTX_KEY,
            "secondary_value"
        );

        TransformHeaders headers = TransformHeaders.fromMap(allHeaders);
        assertThat(headers.headers(), is(equalTo(allHeaders)));
    }

    public void testFromMapAllNonSecurityHeaders() {
        Map<String, String> allHeaders = Map.of("custom-a", "value-a", "custom-b", "value-b");

        TransformHeaders headers = TransformHeaders.fromMap(allHeaders);
        assertThat(headers.headers(), is(equalTo(allHeaders)));
    }

    public void testAllHeaders() {
        Map<String, String> flat = Map.of(AuthenticationField.AUTHENTICATION_KEY, "auth_value", "custom-header", "custom_value");

        TransformHeaders headers = TransformHeaders.fromMap(flat);
        Map<String, String> combined = headers.allHeaders();
        assertThat(combined, is(equalTo(flat)));
    }

    public void testAllHeadersReturnsSameInstance() {
        Map<String, String> flat = Map.of(AuthenticationField.AUTHENTICATION_KEY, "auth_value");
        TransformHeaders headers = TransformHeaders.fromMap(flat);
        assertThat(headers.allHeaders(), is(sameInstance(headers.headers())));
    }

    public void testToXContentExcludeGenerated() throws IOException {
        Map<String, String> flat = Map.of("key", "value");
        TransformHeaders headers = TransformHeaders.fromMap(flat);

        ToXContent.Params params = new ToXContent.MapParams(Map.of(TransformField.EXCLUDE_GENERATED, "true"));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        headers.toXContent(builder, params);
        builder.endObject();

        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry(), null, BytesReference.bytes(builder).streamInput())
        ) {
            Map<String, Object> map = parser.map();
            assertThat(map, is(anEmptyMap()));
        }
    }

    public void testDefensiveCopy() {
        var mutableHeaders = new HashMap<String, String>();
        mutableHeaders.put(AuthenticationField.AUTHENTICATION_KEY, "auth_value");
        mutableHeaders.put("custom", "value");

        TransformHeaders headers = new TransformHeaders(mutableHeaders, null);

        mutableHeaders.put("extra", "extra_value");

        assertThat(headers.headers().size(), is(2));
    }

    public void testSerializationPreSplitHeaders() throws IOException {
        TransportVersion preSplitVersion = TransportVersionUtils.getPreviousVersion(TransformHeaders.WITH_UIAM_TOKEN);

        TransformHeaders original = new TransformHeaders(
            Map.of(AuthenticationField.AUTHENTICATION_KEY, "auth_value", "custom-header", "custom_value"),
            "uiamToken"
        );

        TransformHeaders deserialized = copyWriteable(original, getNamedWriteableRegistry(), instanceReader(), preSplitVersion);

        assertThat(deserialized.headers(), is(equalTo(original.headers())));
        assertThat(deserialized.uiamToken(), is(nullValue()));
    }

    public void testSerializationPostSplitHeaders() throws IOException {
        TransformHeaders original = new TransformHeaders(
            Map.of(AuthenticationField.AUTHENTICATION_KEY, "auth_value", "custom-header", "custom_value"),
            "uiamToken"
        );

        TransformHeaders deserialized = copyWriteable(original, getNamedWriteableRegistry(), instanceReader(), TransportVersion.current());

        assertThat(deserialized.headers(), is(equalTo(original.headers())));
        assertThat(deserialized.allHeaders(), is(equalTo(original.allHeaders())));
        assertThat(deserialized.uiamToken(), is(equalTo(original.uiamToken())));
    }
}
