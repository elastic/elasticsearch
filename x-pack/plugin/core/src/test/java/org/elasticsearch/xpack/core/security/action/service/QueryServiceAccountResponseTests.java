/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class QueryServiceAccountResponseTests extends ESTestCase {

    public void testToXContentReportsAPageOfAccountsInOrderWithTheirSortValues() throws IOException {
        final QueryServiceAccountResponse response = new QueryServiceAccountResponse(
            42,
            List.of(
                new QueryServiceAccountResponse.Item(
                    new ServiceAccountInfo.UserManaged("apps/worker_1", List.of("role-a", "role-b"), true),
                    new Object[] { "apps/worker_1" }
                ),
                new QueryServiceAccountResponse.Item(new ServiceAccountInfo.UserManaged("apps/worker_2", List.of(), false), null)
            )
        );

        final Map<String, Object> responseMap = toMap(response);

        // The principal is a field rather than the item's key, so that the items can stay a list in the sorted order.
        assertThat(
            responseMap,
            equalTo(
                Map.of(
                    "total",
                    42,
                    "count",
                    2,
                    "service_accounts",
                    List.of(
                        Map.of(
                            "username",
                            "apps/worker_1",
                            "type",
                            "user_managed",
                            "roles",
                            List.of("role-a", "role-b"),
                            "enabled",
                            true,
                            "_sort",
                            List.of("apps/worker_1")
                        ),
                        Map.of("username", "apps/worker_2", "type", "user_managed", "roles", List.of(), "enabled", false)
                    )
                )
            )
        );
    }

    public void testToXContentOfNoAccounts() throws IOException {
        assertThat(toMap(QueryServiceAccountResponse.EMPTY), equalTo(Map.of("total", 0, "count", 0, "service_accounts", List.of())));
    }

    public void testEqualityIncludesTheSortValues() {
        final ServiceAccountInfo.UserManaged info = new ServiceAccountInfo.UserManaged("apps/worker_1", List.of("role-a"), true);
        final QueryServiceAccountResponse.Item sorted = new QueryServiceAccountResponse.Item(info, new Object[] { "apps/worker_1" });
        assertThat(sorted, equalTo(new QueryServiceAccountResponse.Item(info, new Object[] { "apps/worker_1" })));
        assertThat(sorted.hashCode(), equalTo(new QueryServiceAccountResponse.Item(info, new Object[] { "apps/worker_1" }).hashCode()));
        assertFalse(sorted.equals(new QueryServiceAccountResponse.Item(info, null)));
        assertFalse(sorted.equals(new QueryServiceAccountResponse.Item(info, new Object[] { "apps/worker_2" })));
    }

    private static Map<String, Object> toMap(QueryServiceAccountResponse response) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }
}
