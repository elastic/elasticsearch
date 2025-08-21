/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Configuration for a {@code JOIN} style operation.
 */
// TODO: this class needs refactoring into a more general form (expressions) since it's currently contains
// both the condition (equi-join) between the left and right field as well as the output of the join keys
// which makes sense only for USING clause - which is better resolved in the analyzer (based on the names)
// hence why for now the attributes are set inside the analyzer
public final class JoinConfig implements Writeable {
    private final JoinType type;
    private final List<Attribute> matchFields;
    private final List<Attribute> leftFields;
    private final List<Attribute> rightFields;
    private final Expression joinOnConditions;

    /**
     * @param matchFields fields either from the left or right fields which decide which side is kept
     * @param leftFields  matched with the right fields
     * @param rightFields matched with the left fields
     */
    public JoinConfig(
        JoinType type,
        List<Attribute> matchFields,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Expression joinOnConditions
    ) {
        this.type = type;
        this.matchFields = matchFields;
        this.leftFields = leftFields;
        this.rightFields = rightFields;
        this.joinOnConditions = joinOnConditions;
    }

    public JoinConfig(StreamInput in) throws IOException {
        this(
            JoinTypes.readFrom(in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            readJoinConditions(in)
        );
    }

    private static Expression readJoinConditions(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_LOOKUP_JOIN_ON_EXPRESSION)) {
            return in.readOptionalNamedWriteable(Expression.class);
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        type.writeTo(out);
        out.writeNamedWriteableCollection(matchFields);
        out.writeNamedWriteableCollection(leftFields);
        out.writeNamedWriteableCollection(rightFields);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_LOOKUP_JOIN_ON_EXPRESSION)) {
            out.writeOptionalNamedWriteable(joinOnConditions);
        } else if (joinOnConditions != null) {
            throw new EsqlIllegalArgumentException("LOOKUP JOIN with ON conditions is not supported on remote node");
        }
    }

    public boolean expressionsResolved() {
        return type.resolved()
            && Resolvables.resolved(matchFields)
            && Resolvables.resolved(leftFields)
            && Resolvables.resolved(rightFields)
            && (joinOnConditions == null || joinOnConditions.resolved());
    }

    public JoinType type() {
        return type;
    }

    public List<Attribute> matchFields() {
        return matchFields;
    }

    public List<Attribute> leftFields() {
        return leftFields;
    }

    public List<Attribute> rightFields() {
        return rightFields;
    }

    public Expression joinOnConditions() {
        return joinOnConditions;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (JoinConfig) obj;
        return Objects.equals(this.type, that.type)
            && Objects.equals(this.matchFields, that.matchFields)
            && Objects.equals(this.leftFields, that.leftFields)
            && Objects.equals(this.rightFields, that.rightFields)
            && Objects.equals(this.joinOnConditions, that.joinOnConditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, matchFields, leftFields, rightFields, joinOnConditions);
    }

    @Override
    public String toString() {
        return "JoinConfig["
            + "type="
            + type
            + ", "
            + "matchFields="
            + matchFields
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
