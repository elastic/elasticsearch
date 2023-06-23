/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

public class PrevalidateNodeRemovalRequest extends MasterNodeReadRequest<PrevalidateNodeRemovalRequest> {

    public static final String VALIDATION_ERROR_MSG_ONLY_ONE_QUERY_PARAM =
        "request must contain only one of the parameters 'names', 'ids', or 'external_ids'";
    public static final String VALIDATION_ERROR_MSG_NO_QUERY_PARAM =
        "request must contain one of the parameters 'names', 'ids', or 'external_ids'";

    private final String[] names;
    private final String[] ids;
    private final String[] externalIds;
    private TimeValue timeout = TimeValue.timeValueSeconds(30);

    private PrevalidateNodeRemovalRequest(Builder builder) {
        this.names = builder.names;
        this.ids = builder.ids;
        this.externalIds = builder.externalIds;
    }

    public PrevalidateNodeRemovalRequest(final StreamInput in) throws IOException {
        super(in);
        names = in.readStringArray();
        ids = in.readStringArray();
        externalIds = in.readStringArray();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            timeout = in.readTimeValue();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(names);
        out.writeStringArray(ids);
        out.writeStringArray(externalIds);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            out.writeTimeValue(timeout);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        var nonEmptyParams = Stream.of(names, ids, externalIds).filter(a -> a != null && a.length > 0).toList();
        if (nonEmptyParams.isEmpty()) {
            var e = new ActionRequestValidationException();
            e.addValidationError(VALIDATION_ERROR_MSG_NO_QUERY_PARAM);
            return e;
        }
        if (nonEmptyParams.size() > 1) {
            var e = new ActionRequestValidationException();
            e.addValidationError(VALIDATION_ERROR_MSG_ONLY_ONE_QUERY_PARAM);
            return e;
        }
        return null;
    }

    public String[] getNames() {
        return names;
    }

    public String[] getIds() {
        return ids;
    }

    public String[] getExternalIds() {
        return externalIds;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public PrevalidateNodeRemovalRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        if (masterNodeTimeout == DEFAULT_MASTER_NODE_TIMEOUT) {
            masterNodeTimeout = timeout;
        }
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof PrevalidateNodeRemovalRequest == false) return false;
        PrevalidateNodeRemovalRequest other = (PrevalidateNodeRemovalRequest) o;
        return Arrays.equals(names, other.names) && Arrays.equals(ids, other.ids) && Arrays.equals(externalIds, other.externalIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(names), Arrays.hashCode(ids), Arrays.hashCode(externalIds));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String[] names = Strings.EMPTY_ARRAY;
        private String[] ids = Strings.EMPTY_ARRAY;
        private String[] externalIds = Strings.EMPTY_ARRAY;

        public Builder setNames(String... names) {
            Objects.requireNonNull(names);
            this.names = names;
            return this;
        }

        public Builder setIds(String... ids) {
            Objects.requireNonNull(ids);
            this.ids = ids;
            return this;
        }

        public Builder setExternalIds(String... externalIds) {
            Objects.requireNonNull(externalIds);
            this.externalIds = externalIds;
            return this;
        }

        public PrevalidateNodeRemovalRequest build() {
            return new PrevalidateNodeRemovalRequest(this);
        }
    }
}
