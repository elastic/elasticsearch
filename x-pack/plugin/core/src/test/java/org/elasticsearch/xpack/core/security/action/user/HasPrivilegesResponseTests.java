/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.protocol.AbstractHlrcStreamableXContentTestCase;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class HasPrivilegesResponseTests
    extends AbstractHlrcStreamableXContentTestCase<HasPrivilegesResponse, org.elasticsearch.client.security.HasPrivilegesResponse> {

    public void testSerializationV64OrV65() throws IOException {
        final HasPrivilegesResponse original = randomResponse();
        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_4_0, Version.V_6_5_1);
        final HasPrivilegesResponse copy = serializeAndDeserialize(original, version);

        assertThat(copy.isCompleteMatch(), equalTo(original.isCompleteMatch()));
        assertThat(copy.getClusterPrivileges().entrySet(), Matchers.emptyIterable());
        assertThat(copy.getIndexPrivileges(), equalTo(original.getIndexPrivileges()));
        assertThat(copy.getApplicationPrivileges(), equalTo(original.getApplicationPrivileges()));
    }

    public void testSerializationV63() throws IOException {
        final HasPrivilegesResponse original = randomResponse();
        final HasPrivilegesResponse copy = serializeAndDeserialize(original, Version.V_6_3_0);

        assertThat(copy.isCompleteMatch(), equalTo(original.isCompleteMatch()));
        assertThat(copy.getClusterPrivileges().entrySet(), Matchers.emptyIterable());
        assertThat(copy.getIndexPrivileges(), equalTo(original.getIndexPrivileges()));
        assertThat(copy.getApplicationPrivileges(), equalTo(Collections.emptyMap()));
    }

    public void testToXContent() throws Exception {
        final HasPrivilegesResponse response = new HasPrivilegesResponse("daredevil", false,
            Collections.singletonMap("manage", true),
            Arrays.asList(
                new HasPrivilegesResponse.ResourcePrivileges("staff",
                    MapBuilder.<String, Boolean>newMapBuilder(new LinkedHashMap<>())
                        .put("read", true).put("index", true).put("delete", false).put("manage", false).map()),
                new HasPrivilegesResponse.ResourcePrivileges("customers",
                    MapBuilder.<String, Boolean>newMapBuilder(new LinkedHashMap<>())
                        .put("read", true).put("index", true).put("delete", true).put("manage", false).map())
            ), Collections.emptyMap());

        final XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        BytesReference bytes = BytesReference.bytes(builder);

        final String json = bytes.utf8ToString();
        assertThat(json, equalTo("{" +
            "\"username\":\"daredevil\"," +
            "\"has_all_requested\":false," +
            "\"cluster\":{\"manage\":true}," +
            "\"index\":{" +
            "\"customers\":{\"read\":true,\"index\":true,\"delete\":true,\"manage\":false}," +
            "\"staff\":{\"read\":true,\"index\":true,\"delete\":false,\"manage\":false}" +
            "}," +
            "\"application\":{}" +
            "}"));
    }

    @Override
    protected boolean supportsUnknownFields() {
        // Because we have nested objects with { string : boolean }, unknown fields cause parsing problems
        return false;
    }

    @Override
    protected HasPrivilegesResponse createBlankInstance() {
        return new HasPrivilegesResponse();
    }

    @Override
    protected HasPrivilegesResponse createTestInstance() {
        return randomResponse();
    }

    @Override
    public org.elasticsearch.client.security.HasPrivilegesResponse doHlrcParseInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.security.HasPrivilegesResponse.fromXContent(parser);
    }

    @Override
    public HasPrivilegesResponse convertHlrcToInternal(org.elasticsearch.client.security.HasPrivilegesResponse hlrc) {
        return new HasPrivilegesResponse(
            hlrc.getUsername(),
            hlrc.hasAllRequested(),
            hlrc.getClusterPrivileges(),
            toResourcePrivileges(hlrc.getIndexPrivileges()),
            hlrc.getApplicationPrivileges().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> toResourcePrivileges(e.getValue())))
            );
    }

    private static List<HasPrivilegesResponse.ResourcePrivileges> toResourcePrivileges(Map<String, Map<String, Boolean>> map) {
        return map.entrySet().stream()
            .map(e -> new HasPrivilegesResponse.ResourcePrivileges(e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    }

    private HasPrivilegesResponse serializeAndDeserialize(HasPrivilegesResponse original, Version version) throws IOException {
        logger.info("Test serialize/deserialize with version {}", version);
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        original.writeTo(out);

        final HasPrivilegesResponse copy = new HasPrivilegesResponse();
        final StreamInput in = out.bytes().streamInput();
        in.setVersion(version);
        copy.readFrom(in);
        assertThat(in.read(), equalTo(-1));
        return copy;
    }

    private HasPrivilegesResponse randomResponse() {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final Map<String, Boolean> cluster = new HashMap<>();
        for (String priv : randomArray(1, 6, String[]::new, () -> randomAlphaOfLengthBetween(3, 12))) {
            cluster.put(priv, randomBoolean());
        }
        final Collection<HasPrivilegesResponse.ResourcePrivileges> index = randomResourcePrivileges();
        final Map<String, Collection<HasPrivilegesResponse.ResourcePrivileges>> application = new HashMap<>();
        for (String app : randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 6).toLowerCase(Locale.ROOT))) {
            application.put(app, randomResourcePrivileges());
        }
        return new HasPrivilegesResponse(username, randomBoolean(), cluster, index, application);
    }

    private Collection<HasPrivilegesResponse.ResourcePrivileges> randomResourcePrivileges() {
        final Collection<HasPrivilegesResponse.ResourcePrivileges> list = new ArrayList<>();
        // Use hash set to force a unique set of resources
        for (String resource : Sets.newHashSet(randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(2, 6)))) {
            final Map<String, Boolean> privileges = new HashMap<>();
            for (String priv : randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(3, 8))) {
                privileges.put(priv, randomBoolean());
            }
            list.add(new HasPrivilegesResponse.ResourcePrivileges(resource, privileges));
        }
        return list;
    }
}
