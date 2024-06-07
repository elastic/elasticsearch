/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public final class QueryRoleResponse extends ActionResponse implements ToXContentObject {

    public static final QueryRoleResponse EMPTY = new QueryRoleResponse(0, List.of(), List.of());

    private final long total;
    private final List<QueryRoleResponse.Item> foundRoleDescriptorList;

    public QueryRoleResponse(long total, Collection<RoleDescriptor> foundRoleDescriptors, Collection<Object[]> sortValues) {
        this.total = total;
        Objects.requireNonNull(foundRoleDescriptors, "found_role_descriptor must be provided");
        Objects.requireNonNull(sortValues, "sort_values must be provided");
        if (foundRoleDescriptors.size() != sortValues.size()) {
            throw new IllegalStateException("Each role descriptor must be associated to a (nullable) sort value");
        }
        int size = foundRoleDescriptors.size();
        this.foundRoleDescriptorList = new ArrayList<>(size);
        Iterator<RoleDescriptor> roleDescriptorIterator = foundRoleDescriptors.iterator();
        Iterator<Object[]> sortValueIterator = sortValues.iterator();
        while (roleDescriptorIterator.hasNext()) {
            if (false == sortValueIterator.hasNext()) {
                throw new IllegalStateException("Each role descriptor must be associated to a (nullable) sort value");
            }
            this.foundRoleDescriptorList.add(new QueryRoleResponse.Item(roleDescriptorIterator.next(), sortValueIterator.next()));
        }
    }

    public long getTotal() {
        return total;
    }

    public List<QueryRoleResponse.Item> getRoleDescriptorList() {
        return foundRoleDescriptorList;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("total", total).field("count", foundRoleDescriptorList.size()).field("roles", foundRoleDescriptorList);
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
        return total == that.total && Objects.equals(foundRoleDescriptorList, that.foundRoleDescriptorList);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(total);
        result = 31 * result + Objects.hash(foundRoleDescriptorList);
        return result;
    }

    @Override
    public String toString() {
        return "QueryRoleResponse{total=" + total + ", items=" + foundRoleDescriptorList + "}";
    }

    public record Item(RoleDescriptor roleDescriptor, @Nullable Object[] sortValues) implements ToXContentObject {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            roleDescriptor.innerToXContent(builder, params, false, false);
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
}
