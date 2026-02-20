/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class UnresolvedExternalRelationTests extends ESTestCase {

    public void testUnresolvedExternalRelationBasic() {
        Source source = Source.EMPTY;
        Expression tablePath = Literal.keyword(source, "s3://bucket/table");
        Map<String, Expression> params = new HashMap<>();

        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(source, tablePath, params);

        assertFalse("UnresolvedExternalRelation should not be resolved", relation.resolved());
        assertFalse("UnresolvedExternalRelation should not have expressions resolved", relation.expressionsResolved());
        assertThat("Output should be empty", relation.output(), hasSize(0));
        assertThat("Table path should match", relation.tablePath(), equalTo(tablePath));
        assertThat("Params should match", relation.params(), equalTo(params));
        assertThat("Unresolved message should contain table path", relation.unresolvedMessage(), containsString("s3://bucket/table"));
        assertThat("String representation should contain EXTERNAL", relation.toString(), containsString("EXTERNAL"));
    }

    public void testUnresolvedExternalRelationWithParams() {
        Source source = Source.EMPTY;
        Expression tablePath = Literal.keyword(source, "s3://bucket/warehouse/testdb.users");
        Map<String, Expression> params = new HashMap<>();
        params.put("access_key", Literal.keyword(source, "AKIAIOSFODNN7EXAMPLE"));
        params.put("secret_key", Literal.keyword(source, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));

        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(source, tablePath, params);

        assertThat("Params should contain access_key", relation.params().containsKey("access_key"), equalTo(true));
        assertThat("Params should contain secret_key", relation.params().containsKey("secret_key"), equalTo(true));
        assertThat("Params should have 2 entries", relation.params().size(), equalTo(2));
    }

    public void testUnresolvedExternalRelationEquality() {
        Source source = Source.EMPTY;
        Expression tablePath1 = Literal.keyword(source, "s3://bucket/table1");
        Expression tablePath2 = Literal.keyword(source, "s3://bucket/table2");
        Map<String, Expression> params1 = new HashMap<>();
        Map<String, Expression> params2 = new HashMap<>();
        params2.put("key", Literal.keyword(source, "value"));

        UnresolvedExternalRelation relation1 = new UnresolvedExternalRelation(source, tablePath1, params1);
        UnresolvedExternalRelation relation2 = new UnresolvedExternalRelation(source, tablePath1, params1);
        UnresolvedExternalRelation relation3 = new UnresolvedExternalRelation(source, tablePath2, params1);
        UnresolvedExternalRelation relation4 = new UnresolvedExternalRelation(source, tablePath1, params2);

        assertThat("Same path and params should be equal", relation1, equalTo(relation2));
        assertNotEquals("Different path should not be equal", relation1, relation3);
        assertNotEquals("Different params should not be equal", relation1, relation4);
    }

    public void testUnresolvedExternalRelationUnresolvableInterface() {
        Source source = Source.EMPTY;
        Expression tablePath = Literal.keyword(source, "s3://bucket/data.parquet");
        Map<String, Expression> params = new HashMap<>();

        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(source, tablePath, params);

        // Test Unresolvable interface methods
        assertFalse("Should not be resolved", relation.resolved());
        assertNotNull("Should have unresolved message", relation.unresolvedMessage());
        assertThat("Unresolved message should be descriptive", relation.unresolvedMessage(), containsString("s3://bucket/data.parquet"));
    }

    public void testUnresolvedExternalRelationLeafPlan() {
        Source source = Source.EMPTY;
        Expression tablePath = Literal.keyword(source, "s3://bucket/table");
        Map<String, Expression> params = new HashMap<>();

        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(source, tablePath, params);

        // Test LeafPlan characteristics
        assertThat("Node properties should contain tablePath", relation.nodeProperties(), hasSize(1));
        assertThat("Node properties should contain tablePath", relation.nodeProperties().get(0), equalTo(tablePath));
    }
}
