/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

import java.util.List;

/** Identifies a join across plan rewrites by the ids of its ordered join keys. */
public record JoinKey(List<NameId> leftIds, List<NameId> rightIds) {
    public JoinKey {
        leftIds = List.copyOf(leftIds);
        rightIds = List.copyOf(rightIds);
    }

    /** Builds a key shared by the search and mapper representations of a join. */
    public static JoinKey of(Join join) {
        return new JoinKey(ids(join.config().leftFields()), ids(join.config().rightFields()));
    }

    private static List<NameId> ids(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::id).toList();
    }
}
