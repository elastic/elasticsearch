/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.plan.AbstractNodeSerializationTests.randomSource;
import static org.elasticsearch.xpack.esql.plan.logical.EsRelationSerializationTests.randomEsRelation;
import static org.elasticsearch.xpack.esql.plan.logical.local.LocalRelationSerializationTests.randomLocalRelation;

/**
 * Needed to override the reflective tests in {@link org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests} because
 * {@link MetadataManipulationFunction} is not safe to construct with random arguments:
 * <ul>
 * <li>The public constructor reads the destination-label name from {@code parameters.get(0)} by casting it to a
 * {@link Literal}, so the first parameter must be a keyword literal rather than an arbitrary expression.</li>
 * <li>The constructor mints a fresh {@link MetadataManipulationFunction#destination() destination} attribute (with a new
 * {@code NameId}). Copies and mutations go through the node's own lifecycle ({@code replaceChild} /
 * {@code transformPropertiesOnly}, which reuse the private constructor) so the destination is threaded through unchanged,
 * rather than through the public constructor, which would re-mint a different id.</li>
 * </ul>
 * Provides {@link #randomMetadataManipulationFunction()} for {@link org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests}
 * to delegate to when it needs to build this node (directly or as a nested child).
 */
public class MetadataManipulationFunctionTests extends AbstractNodeTestCase<MetadataManipulationFunction, LogicalPlan> {

    public static MetadataManipulationFunction randomMetadataManipulationFunction() {
        Source source = randomSource();
        return new MetadataManipulationFunction(source, randomChildWithOutput(), randomDefinition(), randomParameters(source));
    }

    /**
     * The node's {@link MetadataManipulationFunction#output()} delegates to its child, and enclosing nodes (e.g.
     * {@code ResolvingProject}) call {@code output()} at construction. A relation always has a computable output, whereas a
     * generic random child may be a {@code Lookup} with an unresolved table whose {@code output()} throws - so restrict the
     * child to relations to keep the node safe to build in the reflective tree tests.
     */
    private static LogicalPlan randomChildWithOutput() {
        return randomBoolean() ? randomEsRelation() : randomLocalRelation();
    }

    private static PromqlFunctionDefinition randomDefinition() {
        return randomBoolean() ? PromqlBuiltinFunctionDefinitions.LABEL_REPLACE : PromqlBuiltinFunctionDefinitions.LABEL_JOIN;
    }

    private static PromqlFunctionDefinition otherDefinition(PromqlFunctionDefinition current) {
        return current == PromqlBuiltinFunctionDefinitions.LABEL_REPLACE
            ? PromqlBuiltinFunctionDefinitions.LABEL_JOIN
            : PromqlBuiltinFunctionDefinitions.LABEL_REPLACE;
    }

    /**
     * The node reads {@code parameters.get(0)} as the destination-label literal at construction, so the first parameter
     * must be a keyword {@link Literal}; the remaining keyword arguments are modeled as keyword literals too, matching the
     * analyzed shape of {@code label_replace} / {@code label_join}.
     */
    private static List<Expression> randomParameters(Source source) {
        List<Expression> parameters = new ArrayList<>();
        int count = between(1, 4);
        for (int i = 0; i < count; i++) {
            parameters.add(Literal.keyword(source, randomAlphaOfLength(5)));
        }
        return parameters;
    }

    @Override
    protected MetadataManipulationFunction randomInstance() {
        return randomMetadataManipulationFunction();
    }

    @Override
    protected MetadataManipulationFunction mutate(MetadataManipulationFunction instance) {
        Supplier<MetadataManipulationFunction> option = randomFrom(
            List.of(
                () -> instance.replaceChild(randomValueOtherThan(instance.child(), () -> randomChildWithOutput())),
                () -> (MetadataManipulationFunction) instance.transformPropertiesOnly(
                    Object.class,
                    p -> Objects.equals(p, instance.definition()) ? otherDefinition(instance.definition()) : p
                )
            )
        );
        return option.get();
    }

    @Override
    protected MetadataManipulationFunction copy(MetadataManipulationFunction instance) {
        // replaceChild with the same child reuses the private constructor, preserving the minted destination attribute, so
        // the result is equal to (but not the same instance as) the original - unlike the public constructor.
        return instance.replaceChild(instance.child());
    }

    @Override
    public void testTransform() {
        MetadataManipulationFunction node = randomMetadataManipulationFunction();

        PromqlFunctionDefinition newDefinition = otherDefinition(node.definition());
        MetadataManipulationFunction transformed = (MetadataManipulationFunction) node.transformPropertiesOnly(
            Object.class,
            p -> Objects.equals(p, node.definition()) ? newDefinition : p
        );
        assertEquals(node.source(), transformed.source());
        assertEquals(node.child(), transformed.child());
        assertEquals(newDefinition, transformed.definition());
        assertEquals(node.parameters(), transformed.parameters());
        assertEquals(node.destination(), transformed.destination());
    }

    @Override
    public void testReplaceChildren() {
        MetadataManipulationFunction node = randomMetadataManipulationFunction();
        LogicalPlan newChild = randomValueOtherThan(node.child(), () -> randomChildWithOutput());

        MetadataManipulationFunction replaced = node.replaceChild(newChild);
        assertEquals(node.source(), replaced.source());
        assertEquals(newChild, replaced.child());
        assertEquals(node.definition(), replaced.definition());
        assertEquals(node.parameters(), replaced.parameters());
        assertEquals(node.destination(), replaced.destination());
    }
}
