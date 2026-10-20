/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.AbstractLogicalPlanSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationSerializationTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation.ANY;
import static org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation.COORDINATOR;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;
import static org.hamcrest.Matchers.containsString;

public class MaterializedSubqueryJoinSerializationTests extends AbstractLogicalPlanSerializationTests<MaterializedSubqueryJoin> {
    @Override
    protected MaterializedSubqueryJoin createTestInstance() {
        return randomMaterializedSubqueryJoin();
    }

    @Override
    protected MaterializedSubqueryJoin mutateInstance(MaterializedSubqueryJoin instance) throws IOException {
        LogicalPlan left = instance.left();
        LocalRelation right = (LocalRelation) instance.right();
        JoinConfig config = instance.config();
        switch (between(0, 1)) {
            case 0 -> left = randomValueOtherThan(left, () -> randomChild(0));
            case 1 -> right = randomValueOtherThan(right, LocalRelationSerializationTests::randomLocalRelation);
            default -> throw new UnsupportedOperationException();
        }
        return new MaterializedSubqueryJoin(instance.source(), left, right, config, instance.executesOn());
    }

    public void testRewritesPreserveExecuteLocation() {
        MaterializedSubqueryJoin join = new MaterializedSubqueryJoin(
            Source.EMPTY,
            randomChild(0),
            LocalRelationSerializationTests.randomLocalRelation(),
            LEFT,
            randomFieldAttributes(1, 10, false),
            randomFieldAttributes(1, 10, false),
            null,
            COORDINATOR
        );

        assertEquals(COORDINATOR, join.replaceChildren(join.left(), join.right()).executesOn());
        assertEquals(COORDINATOR, join.withConfig(join.config()).executesOn());
    }

    public void testCannotSerializeToNodeWithoutSubqueryJoinSupport() throws IOException {
        MaterializedSubqueryJoin join = randomMaterializedSubqueryJoin();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.fromId(Analyzer.ESQL_JOIN_SUBQUERY.id() - 1));
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> join.writeTo(out));
            assertThat(error.getMessage(), containsString("without subquery join support"));
        }
    }

    public void testSerializationRequiresLocalRelationOnRight() throws IOException {
        LocalRelation left = LocalRelationSerializationTests.randomLocalRelation();
        Project right = new Project(Source.EMPTY, left, List.of());
        MaterializedSubqueryJoin join = new MaterializedSubqueryJoin(Source.EMPTY, left, right, LEFT, List.of(), List.of(), null, ANY);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> join.writeTo(out));
            assertThat(error.getMessage(), containsString("must have a LocalRelation on the right"));
        }
    }

    private static MaterializedSubqueryJoin randomMaterializedSubqueryJoin() {
        return new MaterializedSubqueryJoin(
            randomSource(),
            randomChild(0),
            LocalRelationSerializationTests.randomLocalRelation(),
            LEFT,
            randomFieldAttributes(1, 10, false),
            randomFieldAttributes(1, 10, false),
            null,
            ANY
        );
    }
}
