/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.ColocatedJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Parsing of the {@code JOIN <index> ON <field>} command (POC, snapshot only). The parser builds a plain INNER {@link Join};
 * the co-located strategy is only chosen on the coordinator before mapping.
 */
public class ColocatedJoinParserTests extends AbstractStatementParserTests {

    public void testBareJoin() {
        assumeTrue("colocated join is snapshot only", EsqlCapabilities.Cap.COLOCATED_JOIN_V1.isEnabled());
        var plan = query("FROM events | JOIN sessions ON session_id");
        var join = as(plan, Join.class);
        assertThat(join, not(instanceOf(ColocatedJoin.class)));
        assertTrue(join.isCandicateColocatedJoin());
        assertThat(as(join.left(), UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("events"));
        var right = as(join.right(), UnresolvedRelation.class);
        assertThat(right.indexPattern().indexPattern(), equalTo("sessions"));
        assertThat(right.indexMode(), equalTo(IndexMode.STANDARD));
        assertThat(join.config().type(), equalTo(JoinTypes.INNER));
        assertThat(join.executesOn(), equalTo(ExecuteLocation.ANY));
        assertThat(join.config().leftFields(), hasSize(1));
        assertThat(as(join.config().leftFields().get(0), UnresolvedAttribute.class).name(), equalTo("session_id"));
        assertThat(join.config().rightFields(), equalTo(join.config().leftFields()));
    }

    public void testJoinFollowedByWhere() {
        assumeTrue("colocated join is snapshot only", EsqlCapabilities.Cap.COLOCATED_JOIN_V1.isEnabled());
        var plan = query("FROM events | JOIN sessions ON session_id | WHERE country == \"DE\"");
        var filter = as(plan, Filter.class);
        assertTrue(as(filter.child(), Join.class).isCandicateColocatedJoin());
    }

    public void testRejectsWildcardAndRemote() {
        assumeTrue("colocated join is snapshot only", EsqlCapabilities.Cap.COLOCATED_JOIN_V1.isEnabled());
        expectError("FROM events | JOIN sess* ON session_id", "* is not allowed in JOIN");
        expectError("FROM events | JOIN remote:sessions ON session_id", "remote clusters are not supported with JOIN");
    }

    public void testRejectsMultipleOrExpressionKeys() {
        assumeTrue("colocated join is snapshot only", EsqlCapabilities.Cap.COLOCATED_JOIN_V1.isEnabled());
        expectError("FROM events | JOIN sessions ON session_id, user_id", "supports exactly one field");
        expectError("FROM events | JOIN sessions ON session_id == user_id", "only supports a single field name");
    }
}
