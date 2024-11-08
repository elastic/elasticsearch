/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.util.List;

/**
 * Configuration for a {@code JOIN} style operation.
 * @param matchFields fields either from the left or right fields which decide which side is kept
 * @param leftFields matched with the right fields
 * @param rightFields matched with the left fields
 */
public record JoinConfig(JoinType type, List<Attribute> matchFields, List<Attribute> leftFields, List<Attribute> rightFields)
    implements
        Writeable {
    public JoinConfig(StreamInput in) throws IOException {
        this(
            JoinType.readFrom(in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        type.writeTo(out);
        out.writeNamedWriteableCollection(matchFields);
        out.writeNamedWriteableCollection(leftFields);
        out.writeNamedWriteableCollection(rightFields);
    }

    public boolean expressionsResolved() {
        return Resolvables.resolved(matchFields) && Resolvables.resolved(leftFields) && Resolvables.resolved(rightFields);
    }
}
