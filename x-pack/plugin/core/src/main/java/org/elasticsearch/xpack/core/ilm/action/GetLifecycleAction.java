/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.cluster.metadata.ItemUsage;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetLifecycleAction extends ActionType<GetLifecycleAction.Response> {
    public static final GetLifecycleAction INSTANCE = new GetLifecycleAction();
    public static final String NAME = "cluster:admin/ilm/get";

    protected GetLifecycleAction() {
        super(NAME);
    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {

        private final List<LifecyclePolicyResponseItem> policies;

        public Response(List<LifecyclePolicyResponseItem> policies) {
            this.policies = policies;
        }

        public List<LifecyclePolicyResponseItem> getPolicies() {
            return policies;
        }

        /**
         * NB prior to 9.1 this was a TransportMasterNodeAction so for BwC we must remain able to write these responses until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(policies);
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

        @Override
        public Iterator<ToXContent> toXContentChunked(ToXContent.Params outerParams) {
            return Iterators.concat(
                Iterators.single((builder, params) -> builder.startObject()),
                Iterators.map(policies.iterator(), policy -> (b, p) -> {
                    b.startObject(policy.getLifecyclePolicy().getName());
                    b.field("version", policy.getVersion());
                    b.field("modified_date", policy.getModifiedDate());
                    b.field("policy", policy.getLifecyclePolicy());
                    b.field("in_use_by", policy.getUsage());
                    b.endObject();
                    return b;
                }),
                Iterators.single((b, p) -> b.endObject())
            );
        }
    }

    public static class Request extends LocalClusterStateRequest {
        private final String[] policyNames;

        public Request(TimeValue masterNodeTimeout, String... policyNames) {
            super(masterNodeTimeout);
            if (policyNames == null) {
                throw new IllegalArgumentException("ids cannot be null");
            }
            this.policyNames = policyNames;
        }

        /**
         * NB prior to 9.1 this was a TransportMasterNodeAction so for BwC we must remain able to read these requests until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        public Request(StreamInput in) throws IOException {
            super(in, false);
            // This used to be an AcknowledgedRequest so we need to read the ack timeout for BwC.
            in.readTimeValue();
            policyNames = in.readStringArray();
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "get-lifecycle-task", parentTaskId, headers);
        }

        public String[] getPolicyNames() {
            return policyNames;
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

        /**
         * NB prior to 9.1 this was a TransportMasterNodeAction so for BwC we must remain able to write these responses until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
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
