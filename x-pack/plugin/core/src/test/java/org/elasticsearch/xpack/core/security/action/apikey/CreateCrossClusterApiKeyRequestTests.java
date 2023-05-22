/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CreateCrossClusterApiKeyRequestTests extends AbstractWireSerializingTestCase<CreateCrossClusterApiKeyRequest> {

    private String access;
    private CrossClusterApiKeyRoleDescriptorBuilder roleDescriptorBuilder;

    @Before
    public void init() throws IOException {
        access = randomCrossClusterApiKeyAccessField();
        roleDescriptorBuilder = CrossClusterApiKeyRoleDescriptorBuilder.parse(access);
    }

    @Override
    protected Writeable.Reader<CreateCrossClusterApiKeyRequest> instanceReader() {
        return CreateCrossClusterApiKeyRequest::new;
    }

    @Override
    protected CreateCrossClusterApiKeyRequest createTestInstance() {
        return new CreateCrossClusterApiKeyRequest(
            randomAlphaOfLengthBetween(3, 8),
            roleDescriptorBuilder,
            randomExpiration(),
            randomMetadata()
        );
    }

    @Override
    protected CreateCrossClusterApiKeyRequest mutateInstance(CreateCrossClusterApiKeyRequest instance) throws IOException {
        switch (randomIntBetween(1, 4)) {
            case 1 -> {
                return new CreateCrossClusterApiKeyRequest(
                    randomValueOtherThan(instance.getName(), () -> randomAlphaOfLengthBetween(3, 8)),
                    roleDescriptorBuilder,
                    instance.getExpiration(),
                    instance.getMetadata()
                );
            }
            case 2 -> {
                return new CreateCrossClusterApiKeyRequest(
                    instance.getName(),
                    CrossClusterApiKeyRoleDescriptorBuilder.parse(
                        randomValueOtherThan(access, CreateCrossClusterApiKeyRequestTests::randomCrossClusterApiKeyAccessField)
                    ),
                    instance.getExpiration(),
                    instance.getMetadata()
                );
            }
            case 3 -> {
                return new CreateCrossClusterApiKeyRequest(
                    instance.getName(),
                    roleDescriptorBuilder,
                    randomValueOtherThan(instance.getExpiration(), CreateCrossClusterApiKeyRequestTests::randomExpiration),
                    instance.getMetadata()
                );
            }
            default -> {
                return new CreateCrossClusterApiKeyRequest(
                    instance.getName(),
                    roleDescriptorBuilder,
                    instance.getExpiration(),
                    randomValueOtherThan(instance.getMetadata(), CreateCrossClusterApiKeyRequestTests::randomMetadata)
                );
            }
        }
    }

    private static TimeValue randomExpiration() {
        return randomFrom(TimeValue.timeValueHours(randomIntBetween(1, 999)), null);
    }

    private static Map<String, Object> randomMetadata() {
        return randomFrom(
            randomMap(
                0,
                3,
                () -> new Tuple<>(
                    randomAlphaOfLengthBetween(3, 8),
                    randomFrom(randomAlphaOfLengthBetween(3, 8), randomInt(), randomBoolean())
                )
            ),
            null
        );
    }

    private static final List<String> ACCESS_CANDIDATES = List.of("""
        {
          "search": [ {"names": ["logs"]} ]
        }""", """
        {
          "search": [ {"names": ["logs"], "query": "abc" } ]
        }""", """
        {
          "search": [ {"names": ["logs"], "field_security": {"grant": ["*"], "except": ["private"]} } ]
        }""", """
        {
          "search": [ {"names": ["logs"], "query": "abc", "field_security": {"grant": ["*"], "except": ["private"]} } ]
        }""", """
        {
          "replication": [ {"names": ["archive"], "allow_restricted_indices": true } ]
        }""", """
        {
          "replication": [ {"names": ["archive"]} ]
        }""", """
        {
          "search": [ {"names": ["logs"]} ],
          "replication": [ {"names": ["archive"]} ]
        }""");

    public static String randomCrossClusterApiKeyAccessField() {
        return randomFrom(ACCESS_CANDIDATES);
    }
}
