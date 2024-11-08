/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class QueryRoleResponse extends ActionResponse implements ToXContentObject {

    public static final QueryRoleResponse EMPTY = new QueryRoleResponse(0, List.of());

    private final long total;
    private final List<QueryRoleResponse.Item> foundRoleDescriptors;

    public QueryRoleResponse(long total, List<Item> foundRoleDescriptors) {
        this.total = total;
        Objects.requireNonNull(foundRoleDescriptors, "found_role_descriptor must be provided");
        this.foundRoleDescriptors = foundRoleDescriptors;
    }

    public long getTotal() {
        return total;
    }

    public List<Item> getRoleDescriptors() {
        return foundRoleDescriptors;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("total", total).field("count", foundRoleDescriptors.size()).field("roles", foundRoleDescriptors);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryRoleResponse that = (QueryRoleResponse) o;
        return total == that.total && Objects.equals(foundRoleDescriptors, that.foundRoleDescriptors);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(total);
        result = 31 * result + Objects.hash(foundRoleDescriptors);
        return result;
    }

    @Override
    public String toString() {
        return "QueryRoleResponse{total=" + total + ", items=" + foundRoleDescriptors + "}";
    }

    public record Item(RoleDescriptor roleDescriptor, @Nullable Object[] sortValues) implements ToXContentObject {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            // The role name is not normally stored in the role document (it is part of the doc id),
            // so the "toXContent" method doesn't include it.
            // But, for the query role API, we'd like to return the role name together with the
            // other details of the role descriptor (in the same object).
            assert Strings.isNullOrEmpty(roleDescriptor.getName()) == false;
            builder.field("name", roleDescriptor.getName());
            roleDescriptor.innerToXContent(builder, params, false);
            if (sortValues != null && sortValues.length > 0) {
                builder.array("_sort", sortValues);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "Item{roleDescriptor=" + roleDescriptor + ", sortValues=" + Arrays.toString(sortValues) + "}";
        }
    }

    public record QueryRoleResult(long total, List<Item> items) {
        public static final QueryRoleResult EMPTY = new QueryRoleResult(0, List.of());
    }
}
