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
import org.elasticsearch.core.Strings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

public class ProfileHasPrivilegesResponseTests extends AbstractWireSerializingTestCase<ProfileHasPrivilegesResponse> {

    @Override
    protected Writeable.Reader<ProfileHasPrivilegesResponse> instanceReader() {
        return ProfileHasPrivilegesResponse::new;
    }

    @Override
    protected ProfileHasPrivilegesResponse createTestInstance() {
        return new ProfileHasPrivilegesResponse(
            randomUnique(() -> randomAlphaOfLengthBetween(0, 5), randomIntBetween(0, 5)),
            randomErrors()
        );
    }

    @Override
    protected ProfileHasPrivilegesResponse mutateInstance(ProfileHasPrivilegesResponse instance) {
        return randomFrom(
            new ProfileHasPrivilegesResponse(newMutatedSet(instance.hasPrivilegeUids()), instance.errors()),
            new ProfileHasPrivilegesResponse(instance.hasPrivilegeUids(), randomValueOtherThan(instance.errors(), this::randomErrors)),
            new ProfileHasPrivilegesResponse(
                newMutatedSet(instance.hasPrivilegeUids()),
                randomValueOtherThan(instance.errors(), this::randomErrors)
            )
        );
    }

    public void testToXContent() throws IOException {
        final ProfileHasPrivilegesResponse response = createTestInstance();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final Map<String, Object> responseMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();

        if (response.errors().isEmpty()) {
            assertThat(responseMap, equalTo(Map.of("has_privilege_uids", new ArrayList<>(response.hasPrivilegeUids()))));
        } else {
            assertThat(responseMap, hasEntry("has_privilege_uids", List.copyOf(response.hasPrivilegeUids())));
            @SuppressWarnings("unchecked")
            final Map<String, Object> errorsMap = (Map<String, Object>) responseMap.get("errors");
            assertThat(errorsMap.get("count"), equalTo(response.errors().size()));
            @SuppressWarnings("unchecked")
            final Map<String, Object> detailsMap = (Map<String, Object>) errorsMap.get("details");
            assertThat(detailsMap.keySet(), equalTo(response.errors().keySet()));

            detailsMap.forEach((k, v) -> {
                final String errorString;
                final Exception e = response.errors().get(k);
                if (e instanceof IllegalArgumentException illegalArgumentException) {
                    errorString = Strings.format("""
                        {
                          "type": "illegal_argument_exception",
                          "reason": "%s"
                        }""", illegalArgumentException.getMessage());
                } else if (e instanceof ResourceNotFoundException resourceNotFoundException) {
                    errorString = Strings.format("""
                        {
                          "type": "resource_not_found_exception",
                          "reason": "%s"
                        }""", resourceNotFoundException.getMessage());
                } else if (e instanceof ElasticsearchException elasticsearchException) {
                    errorString = Strings.format("""
                        {
                          "type": "exception",
                          "reason": "%s",
                          "caused_by": {
                            "type": "illegal_argument_exception",
                            "reason": "%s"
                          }
                        }""", elasticsearchException.getMessage(), elasticsearchException.getCause().getMessage());
                } else {
                    throw new IllegalArgumentException("unknown exception type: " + e);
                }
                assertThat(v, equalTo(XContentHelper.convertToMap(JsonXContent.jsonXContent, errorString, false)));
            });
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

    private Map<String, Exception> randomErrors() {
        final Map<String, Exception> errors = new TreeMap<>();
        final Supplier<Exception> randomExceptionSupplier = () -> randomFrom(
            new IllegalArgumentException(randomAlphaOfLengthBetween(0, 18)),
            new ResourceNotFoundException(randomAlphaOfLengthBetween(0, 18)),
            new ElasticsearchException(randomAlphaOfLengthBetween(0, 18), new IllegalArgumentException(randomAlphaOfLengthBetween(0, 18)))
        );
        IntStream.range(0, randomIntBetween(0, 3)).forEach(i -> errors.put(randomAlphaOfLength(20) + i, randomExceptionSupplier.get()));
        return errors;
    }
}
