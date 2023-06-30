/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.ItemUsage;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetLifecycleAction extends ActionType<GetLifecycleAction.Response> {
    public static final GetLifecycleAction INSTANCE = new GetLifecycleAction();
    public static final String NAME = "cluster:admin/ilm/get";

    protected GetLifecycleAction() {
        super(NAME, GetLifecycleAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private List<LifecyclePolicyResponseItem> policies;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.policies = in.readList(LifecyclePolicyResponseItem::new);
        }

        public Response(List<LifecyclePolicyResponseItem> policies) {
            this.policies = policies;
        }

        public List<LifecyclePolicyResponseItem> getPolicies() {
            return policies;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (LifecyclePolicyResponseItem item : policies) {
                builder.startObject(item.getLifecyclePolicy().getName());
                builder.field("version", item.getVersion());
                builder.field("modified_date", item.getModifiedDate());
                builder.field("policy", item.getLifecyclePolicy());
                builder.field("in_use_by", item.getUsage());
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(policies);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policies);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(policies, other.policies);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }

    }

    public static class Request extends AcknowledgedRequest<Request> {
        private String[] policyNames;

        public Request(String... policyNames) {
            if (policyNames == null) {
                throw new IllegalArgumentException("ids cannot be null");
            }
            this.policyNames = policyNames;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            policyNames = in.readStringArray();
        }

        public Request() {
            policyNames = Strings.EMPTY_ARRAY;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "get-lifecycle-task", parentTaskId, headers);
        }

        public String[] getPolicyNames() {
            return policyNames;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(policyNames);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(policyNames);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Arrays.equals(policyNames, other.policyNames);
        }

    }

    public static class LifecyclePolicyResponseItem implements Writeable {
        private final LifecyclePolicy lifecyclePolicy;
        private final long version;
        private final String modifiedDate;
        private final ItemUsage usage;

        public LifecyclePolicyResponseItem(LifecyclePolicy lifecyclePolicy, long version, String modifiedDate, ItemUsage usage) {
            this.lifecyclePolicy = lifecyclePolicy;
            this.version = version;
            this.modifiedDate = modifiedDate;
            this.usage = usage;
        }

        LifecyclePolicyResponseItem(StreamInput in) throws IOException {
            this.lifecyclePolicy = new LifecyclePolicy(in);
            this.version = in.readVLong();
            this.modifiedDate = in.readString();
            this.usage = new ItemUsage(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            lifecyclePolicy.writeTo(out);
            out.writeVLong(version);
            out.writeString(modifiedDate);
            this.usage.writeTo(out);
        }

        public LifecyclePolicy getLifecyclePolicy() {
            return lifecyclePolicy;
        }

        public long getVersion() {
            return version;
        }

        public String getModifiedDate() {
            return modifiedDate;
        }

        public ItemUsage getUsage() {
            return usage;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lifecyclePolicy, version, modifiedDate, usage);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            LifecyclePolicyResponseItem other = (LifecyclePolicyResponseItem) obj;
            return Objects.equals(lifecyclePolicy, other.lifecyclePolicy)
                && Objects.equals(version, other.version)
                && Objects.equals(modifiedDate, other.modifiedDate)
                && Objects.equals(usage, other.usage);
        }
    }

}
