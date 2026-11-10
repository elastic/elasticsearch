/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionArgument;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionInvocation;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * What this leaf is allowed to say about itself. A map argument can carry a credential, and both
 * {@code nodeProperties()} and {@code toString()} reach {@code EXPLAIN} output, so neither may render one.
 * The sibling leaf {@code UnresolvedExternalRelation} pins the same property for the same reason.
 */
public class UnresolvedTableFunctionRelationTests extends ESTestCase {

    private static final String SECRET = "AKIAIOSFODNN7EXAMPLE";

    private static UnresolvedTableFunctionRelation leafCarryingASecret() {
        return leafCarryingASecret(Source.EMPTY, List.of());
    }

    private static UnresolvedTableFunctionRelation leafCarryingASecret(Source source, List<NamedExpression> metadataFields) {
        return new UnresolvedTableFunctionRelation(
            source,
            new TableFunctionInvocation(
                Source.EMPTY,
                "somefunc",
                List.of(
                    new TableFunctionArgument.Name(Source.EMPTY, "a_name"),
                    new TableFunctionArgument.NamedValues(Source.EMPTY, Map.of("access_key", SECRET))
                )
            ),
            metadataFields
        );
    }

    public void testNodePropertiesCarryTheNameAndNothingElse() {
        UnresolvedTableFunctionRelation leaf = leafCarryingASecret();

        assertThat(leaf.nodeProperties().size(), equalTo(1));
        assertThat(leaf.nodeProperties().get(0), equalTo("somefunc"));
        assertThat(leaf.nodeProperties().toString(), not(containsString(SECRET)));
    }

    public void testToStringDoesNotRenderTheArguments() {
        assertThat(leafCarryingASecret().toString(), not(containsString(SECRET)));
    }

    /** It still has to be identifiable, or an EXPLAIN of a query with two calls reads the same for both. */
    public void testToStringNamesTheFunction() {
        assertThat(leafCarryingASecret().toString(), containsString("SOMEFUNC(...)"));
    }

    /**
     * The METADATA suffix is the one part of {@code toString} that renders a list the user wrote, so the redaction
     * claim is weaker there than where the arguments are dropped outright.
     */
    public void testToStringNamesTheMetadataItCarries() {
        List<NamedExpression> metadataFields = List.of(MetadataAttribute.create(Source.EMPTY, MetadataAttribute.INDEX));
        UnresolvedTableFunctionRelation leaf = leafCarryingASecret(Source.EMPTY, metadataFields);

        assertThat(leaf.toString(), containsString("SOMEFUNC(...) METADATA"));
        assertThat(leaf.toString(), containsString(MetadataAttribute.INDEX));
        assertThat(leaf.toString(), not(containsString(SECRET)));
    }

    /**
     * Equality ignores the source, so two leaves written at different positions compare equal — and a hash that did
     * not ignore it would put them in different buckets of a map that is meant to find either from the other.
     */
    public void testEqualLeavesHashEqualEvenAtDifferentPositions() {
        UnresolvedTableFunctionRelation here = leafCarryingASecret(new Source(1, 5, "SOMEFUNC(...)"), List.of());
        UnresolvedTableFunctionRelation elsewhere = leafCarryingASecret(new Source(9, 2, "SOMEFUNC(...)"), List.of());

        assertThat(here, equalTo(elsewhere));
        assertThat(here.hashCode(), equalTo(elsewhere.hashCode()));
    }

    /**
     * It says it is unresolved, with no output and a message naming the function. This is what makes a skipped
     * rewrite fail the query instead of reading nothing: every stage that walks a plan asks these.
     */
    public void testItReportsItselfUnresolvedAndNamesTheFunction() {
        UnresolvedTableFunctionRelation leaf = leafCarryingASecret();

        assertThat(leaf.resolved(), equalTo(false));
        assertThat(leaf.expressionsResolved(), equalTo(false));
        assertThat(leaf.output(), empty());
        assertThat(leaf.unresolvedMessage(), equalTo("Unresolved table function [somefunc]"));
    }

    /**
     * The leaf refuses to serialize, and that refusal is load-bearing elsewhere: it is the argument
     * {@code TableFunctionArgument} gives for not being {@code Writeable} at all. The path is unreachable today —
     * the resolver substitutes the relation during analysis, so no plan carrying one is ever sent — which is why
     * nothing noticed that neither method had a test.
     */
    public void testItRefusesToSerialize() {
        UnresolvedTableFunctionRelation relation = leafCarryingASecret();

        expectThrows(UnsupportedOperationException.class, () -> relation.writeTo(null));
        expectThrows(UnsupportedOperationException.class, relation::getWriteableName);
    }
}
