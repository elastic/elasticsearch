/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.util.List;

/**
 * The home cluster's answer to a {@link ResolveAbstractionSchemaRequest}: the abstraction's real output attributes,
 * resolved through the home cluster's own umbrella (view body analyzed, dataset footer read on the home node). The
 * coordinator uses these to build the {@code Boundary.REMOTE} leaf's {@code output()}.
 *
 * <p>{@link Attribute} is {@code NamedWriteable}, so the schema rides the named-writeable registry directly — the same
 * currency {@code EsqlResolveSchemaAction.Response} uses to cross the wire. No {@code IndexResolution} / expanded body
 * crosses; only the resolved columns.
 */
final class ResolveAbstractionSchemaResponse extends ActionResponse {

    private final List<Attribute> attributes;

    ResolveAbstractionSchemaResponse(List<Attribute> attributes) {
        this.attributes = attributes;
    }

    ResolveAbstractionSchemaResponse(StreamInput in) throws IOException {
        this.attributes = in.readNamedWriteableCollectionAsList(Attribute.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableCollection(attributes);
    }

    List<Attribute> attributes() {
        return attributes;
    }
}
