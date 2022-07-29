/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ProfileHasPrivilegesResponseTests extends AbstractWireSerializingTestCase<ProfileHasPrivilegesResponse> {

    @Override
    protected Writeable.Reader<ProfileHasPrivilegesResponse> instanceReader() {
        return ProfileHasPrivilegesResponse::new;
    }

    @Override
    protected ProfileHasPrivilegesResponse createTestInstance() {
        return new ProfileHasPrivilegesResponse(
            randomUnique(() -> randomAlphaOfLengthBetween(0, 5), randomIntBetween(0, 5)),
            randomUnique(() -> randomAlphaOfLengthBetween(0, 5), randomIntBetween(0, 5))
        );
    }

    @Override
    protected ProfileHasPrivilegesResponse mutateInstance(ProfileHasPrivilegesResponse instance) throws IOException {
        return randomFrom(
            new ProfileHasPrivilegesResponse(newMutatedSet(instance.hasPrivilegeUids()), instance.errorUids()),
            new ProfileHasPrivilegesResponse(instance.hasPrivilegeUids(), newMutatedSet(instance.errorUids())),
            new ProfileHasPrivilegesResponse(newMutatedSet(instance.hasPrivilegeUids()), newMutatedSet(instance.errorUids()))
        );
    }

    public void testToXContent() throws IOException {
        final ProfileHasPrivilegesResponse response = createTestInstance();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final Map<String, Object> responseMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();

        if (response.errorUids().isEmpty()) {
            assertThat(responseMap, equalTo(Map.of("has_privilege_uids", new ArrayList<>(response.hasPrivilegeUids()))));
        } else {
            assertThat(
                responseMap,
                equalTo(
                    Map.of(
                        "has_privilege_uids",
                        new ArrayList<>(response.hasPrivilegeUids()),
                        "error_uids",
                        new ArrayList<>(response.errorUids())
                    )
                )
            );
        }
    }

    private Set<String> newMutatedSet(Set<String> in) {
        Set<String> mutated = new HashSet<>(in);
        if (randomBoolean()) {
            mutated = new HashSet<>(randomSubsetOf(mutated));
        }
        if (randomBoolean()) {
            mutated.addAll(randomList(5, () -> randomAlphaOfLengthBetween(0, 5)));
        }
        if (mutated.equals(in)) {
            // try again
            return newMutatedSet(in);
        }
        return mutated;
    }
}
