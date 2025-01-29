/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A response to an action which updated the cluster state, but needs to report whether any relevant nodes failed to apply the update. For
 * instance, a {@link org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest} may update a mapping in the index metadata, but
 * one or more data nodes may fail to acknowledge the new mapping within the ack timeout. If this happens then clients must accept that
 * subsequent requests that rely on the mapping update may return errors from the lagging data nodes.
 * <p>
 * Actions which return a payload-free acknowledgement of success should generally prefer to use {@link ActionResponse.Empty} instead of
 * {@link AcknowledgedResponse}, and other listeners should generally prefer {@link Void}.
 */
public class AcknowledgedResponse extends ActionResponse implements IsAcknowledgedSupplier, ToXContentObject {

    public static final AcknowledgedResponse TRUE = new AcknowledgedResponse(true);

    public static final AcknowledgedResponse FALSE = new AcknowledgedResponse(false);

    public static final String ACKNOWLEDGED_KEY = "acknowledged";

    protected final boolean acknowledged;

    public static AcknowledgedResponse readFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? TRUE : FALSE;
    }

    protected AcknowledgedResponse(StreamInput in) throws IOException {
        super(in);
        acknowledged = in.readBoolean();
    }

    public static AcknowledgedResponse of(boolean acknowledged) {
        return acknowledged ? TRUE : FALSE;
    }

    protected AcknowledgedResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    /**
     * @return whether the update was acknowledged by all the relevant nodes in the cluster.
     */
    @Override
    public final boolean isAcknowledged() {
        return acknowledged;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACKNOWLEDGED_KEY, isAcknowledged());
        addCustomFields(builder, params);
        builder.endObject();
        return builder;
    }

    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {}

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AcknowledgedResponse that = (AcknowledgedResponse) o;
        return isAcknowledged() == that.isAcknowledged();
    }

    @Override
    public int hashCode() {
        return Objects.hash(isAcknowledged());
    }
}
