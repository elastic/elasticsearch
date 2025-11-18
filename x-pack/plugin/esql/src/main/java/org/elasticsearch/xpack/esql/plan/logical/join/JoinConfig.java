/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.io.IOException;
import java.util.List;

/**
 * Configuration of a join operation.
 * We support equi-joins on a list of fields, as well as expression based joins.
 * For list of fields based joins, the left and right lists must be of the same size and match positionally.
 * For expression based joins, the join conditions are expressed as a boolean expression.
 * The join on condition is stored in the {@code joinOnConditions} field.
 * We support one or more binary expressions (e.g. {@code ==, <, >, <=, >=, !=}) combined with {@code AND}.
 * One side of each binary expression must be an attribute from the left side of the join
 * and the other side an attribute from the side of the join child.
 * Those are populated in the {@code leftFields} and {@code rightFields} lists respectively.
 * Notice however that {@code leftFields} and {@code rightFields} might have different size if a field is reused
 * (e.g. {@code left_a == right_b AND left_a = right_c}).
 * In addition, you can AND an optional Lucene pushable expression containing references to the right side of the join only.
 * This expression can contain OR and NOT nodes, as those operators are Lucene pushable.
 * @param type        type of join
 * @param leftFields  fields from the left child to join on
 * @param rightFields fields from the right child to join on
 * @param joinOnConditions join conditions for expression based joins. If null, we assume equi-join on the left/right fields
 */
public record JoinConfig(JoinType type, List<Attribute> leftFields, List<Attribute> rightFields, @Nullable Expression joinOnConditions)
    implements
        Writeable {

    private static final TransportVersion ESQL_LOOKUP_JOIN_ON_EXPRESSION = TransportVersion.fromName("esql_lookup_join_on_expression");

    public JoinConfig(StreamInput in) throws IOException {
        this(JoinTypes.readFrom(in), readLeftFields(in), in.readNamedWriteableCollectionAsList(Attribute.class), readJoinConditions(in));
    }

    private static List<Attribute> readLeftFields(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_EXPRESSION) == false) {
            // For BWC, the left fields were written twice (once as match fields)
            // We read the first set and ignore them.
            in.readNamedWriteableCollectionAsList(Attribute.class);
        }
        return in.readNamedWriteableCollectionAsList(Attribute.class);
    }

    private static Expression readJoinConditions(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_EXPRESSION)) {
            return in.readOptionalNamedWriteable(Expression.class);
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        type.writeTo(out);
        if (out.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_EXPRESSION) == false) {
            out.writeNamedWriteableCollection(leftFields);
        }
        out.writeNamedWriteableCollection(leftFields);
        out.writeNamedWriteableCollection(rightFields);
        if (out.getTransportVersion().supports(ESQL_LOOKUP_JOIN_ON_EXPRESSION)) {
            out.writeOptionalNamedWriteable(joinOnConditions);
        } else if (joinOnConditions != null) {
            throw new IllegalArgumentException("LOOKUP JOIN with ON conditions is not supported on remote node");
        }
    }

    public boolean expressionsResolved() {
        return type.resolved()
            && Resolvables.resolved(leftFields)
            && Resolvables.resolved(rightFields)
            && (joinOnConditions == null || joinOnConditions.resolved());
    }

    @Override
    public String toString() {
        return "JoinConfig["
            + "type="
            + type
            + ", "
            + "leftFields="
            + leftFields
            + ", "
            + "rightFields="
            + rightFields
            + ", "
            + "joinOnConditions="
            + joinOnConditions
            + ']';
    }

}
