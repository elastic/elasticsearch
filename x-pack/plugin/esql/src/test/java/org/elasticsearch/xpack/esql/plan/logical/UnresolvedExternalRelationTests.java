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
        Map<String, Object> config = new HashMap<>();

        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(source, tablePath, config);

        assertFalse("UnresolvedExternalRelation should not be resolved", relation.resolved());
        assertFalse("UnresolvedExternalRelation should not have expressions resolved", relation.expressionsResolved());
        assertThat("Output should be empty", relation.output(), hasSize(0));
        assertThat("Table path should match", relation.tablePath(), equalTo(tablePath));
        assertThat("Config should match", relation.config(), equalTo(config));
        assertThat("Unresolved message should contain table path", relation.unresolvedMessage(), containsString("s3://bucket/table"));
        assertThat("String representation should contain EXTERNAL", relation.toString(), containsString("EXTERNAL"));
    }

    public void testUnresolvedExternalRelationWithConfig() {
        Source source = Source.EMPTY;
        Expression tablePath = Literal.keyword(source, "s3://bucket/warehouse/testdb.users");
        Map<String, Object> config = new HashMap<>();
        config.put("access_key", "AKIAIOSFODNN7EXAMPLE");
        config.put("secret_key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(source, tablePath, config);

        assertThat("Config should contain access_key", relation.config().containsKey("access_key"), equalTo(true));
        assertThat("Config should contain secret_key", relation.config().containsKey("secret_key"), equalTo(true));
        assertThat("Config should have 2 entries", relation.config().size(), equalTo(2));
    }

    public void testUnresolvedExternalRelationEquality() {
        Source source = Source.EMPTY;
        Expression tablePath1 = Literal.keyword(source, "s3://bucket/table1");
        Expression tablePath2 = Literal.keyword(source, "s3://bucket/table2");
        Map<String, Object> config1 = new HashMap<>();
        Map<String, Object> config2 = new HashMap<>();
        config2.put("key", "value");

        UnresolvedExternalRelation relation1 = new UnresolvedExternalRelation(source, tablePath1, config1);
        UnresolvedExternalRelation relation2 = new UnresolvedExternalRelation(source, tablePath1, config1);
        UnresolvedExternalRelation relation3 = new UnresolvedExternalRelation(source, tablePath2, config1);
        UnresolvedExternalRelation relation4 = new UnresolvedExternalRelation(source, tablePath1, config2);

        assertThat("Same path and config should be equal", relation1, equalTo(relation2));
        assertNotEquals("Different path should not be equal", relation1, relation3);
        assertNotEquals("Different config should not be equal", relation1, relation4);
    }

    public void testUnresolvedExternalRelationUnresolvableInterface() {
        Source source = Source.EMPTY;
        Expression tablePath = Literal.keyword(source, "s3://bucket/data.parquet");
        Map<String, Object> config = new HashMap<>();

        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(source, tablePath, config);

        // Test Unresolvable interface methods
        assertFalse("Should not be resolved", relation.resolved());
        assertNotNull("Should have unresolved message", relation.unresolvedMessage());
        assertThat("Unresolved message should be descriptive", relation.unresolvedMessage(), containsString("s3://bucket/data.parquet"));
    }

    public void testUnresolvedExternalRelationLeafPlan() {
        Source source = Source.EMPTY;
        Expression tablePath = Literal.keyword(source, "s3://bucket/table");
        Map<String, Object> config = new HashMap<>();

        UnresolvedExternalRelation relation = new UnresolvedExternalRelation(source, tablePath, config);

        // Test LeafPlan characteristics
        assertThat("Node properties should contain tablePath", relation.nodeProperties(), hasSize(1));
        assertThat("Node properties should contain tablePath", relation.nodeProperties().get(0), equalTo(tablePath));
    }
}
