/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;

public class QueryApiKeyResponseTests extends AbstractWireSerializingTestCase<QueryApiKeyResponse> {

    @Override
    protected Writeable.Reader<QueryApiKeyResponse> instanceReader() {
        return QueryApiKeyResponse::new;
    }

    @Override
    protected QueryApiKeyResponse createTestInstance() {
        final List<QueryApiKeyResponse.Item> items = randomList(0, 3, this::randomItem);
        return new QueryApiKeyResponse(randomIntBetween(items.size(), 100), items);
    }

    @Override
    protected QueryApiKeyResponse mutateInstance(QueryApiKeyResponse instance) {
        final List<QueryApiKeyResponse.Item> items = Arrays.stream(instance.getItems()).collect(Collectors.toCollection(ArrayList::new));
        switch (randomIntBetween(0, 3)) {
            case 0:
                items.add(randomItem());
                return new QueryApiKeyResponse(instance.getTotal(), items);
            case 1:
                if (false == items.isEmpty()) {
                    return new QueryApiKeyResponse(instance.getTotal(), items.subList(1, items.size()));
                } else {
                    items.add(randomItem());
                    return new QueryApiKeyResponse(instance.getTotal(), items);
                }
            case 2:
                if (false == items.isEmpty()) {
                    final int index = randomIntBetween(0, items.size() - 1);
                    items.set(index, randomItem());
                } else {
                    items.add(randomItem());
                }
                return new QueryApiKeyResponse(instance.getTotal(), items);
            default:
                return new QueryApiKeyResponse(instance.getTotal() + 1, items);
        }
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    ConfigurableClusterPrivilege.class,
                    ConfigurableClusterPrivileges.ManageApplicationPrivileges.WRITEABLE_NAME,
                    ConfigurableClusterPrivileges.ManageApplicationPrivileges::createFrom
                ),
                new NamedWriteableRegistry.Entry(
                    ConfigurableClusterPrivilege.class,
                    ConfigurableClusterPrivileges.WriteProfileDataPrivileges.WRITEABLE_NAME,
                    ConfigurableClusterPrivileges.WriteProfileDataPrivileges::createFrom
                )
            )
        );
    }

    private QueryApiKeyResponse.Item randomItem() {
        return new QueryApiKeyResponse.Item(randomApiKeyInfo(), randomSortValues());
    }

    private ApiKey randomApiKeyInfo() {
        final String name = randomAlphaOfLengthBetween(3, 8);
        final String id = randomAlphaOfLength(22);
        final String username = randomAlphaOfLengthBetween(3, 8);
        final String realm_name = randomAlphaOfLengthBetween(3, 8);
        final Instant creation = Instant.ofEpochMilli(randomMillisUpToYear9999());
        final Instant expiration = randomBoolean() ? Instant.ofEpochMilli(randomMillisUpToYear9999()) : null;
        final Map<String, Object> metadata = ApiKeyTests.randomMetadata();
        final List<RoleDescriptor> roleDescriptors = randomFrom(randomUniquelyNamedRoleDescriptors(0, 3), null);
        return new ApiKey(
            name,
            id,
            creation,
            expiration,
            false,
            username,
            realm_name,
            metadata,
            roleDescriptors,
            randomUniquelyNamedRoleDescriptors(1, 3)
        );
    }

    private Object[] randomSortValues() {
        if (randomBoolean()) {
            return null;
        } else {
            return randomArray(1, 3, Object[]::new, () -> randomFrom(42, 42L, "key-1", "2021-01-01T00:00:00.177Z", randomBoolean()));
        }
    }
}
