/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.hasSize;

/** Compiler-only nodes need parser-created binding identities, rather than independent reflective constructor arguments. */
public class SequenceMatchOutputNodeTests extends ESTestCase {
    private static final EsqlParser PARSER = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));

    /** A declaration's stage alias table survives node copying and cannot be mutated after parsing. */
    public void testDeclarationCopiesPreserveStageAliases() {
        Sequence sequence = parsed().collect(Sequence.class).getFirst();
        Map<String, Integer> names = new HashMap<>(Map.of("download", 1, "execution", 2));
        var declaration = new SequenceMatchOutput.Declaration(Source.EMPTY, sequence, 2, names);
        names.clear();
        assertEquals(Map.of("download", 1, "execution", 2), declaration.stageNames());
        expectThrows(UnsupportedOperationException.class, () -> declaration.stageNames().put("other", 1));
        LogicalPlan replacement = new Filter(Source.EMPTY, sequence, Literal.TRUE);
        var copy = declaration.replaceChild(replacement);
        assertEquals(replacement, copy.child());
        assertEquals(declaration.stageNames(), copy.stageNames());
        assertPreservedProperties(declaration, copy);
        assertEquals(copy, declaration.replaceChildren(List.of(replacement)));
        assertEquals(1, declaration.transformPropertiesOnly(Integer.class, ignored -> 1).nodeProperties().get(1));
        assertEquals(declaration.stageNames(), declaration.nodeProperties().get(2));
    }

    /** Copies must keep protected identity and MEMBER provenance, including when an ordinary field has the same display name. */
    public void testReferenceCopiesPreserveProtectedIdentity() {
        List<SequenceMatchOutput.BindingReference> references = new ArrayList<>();
        parsed().forEachExpressionDown(SequenceMatchOutput.BindingReference.class, references::add);
        assertTrue(references.stream().anyMatch(SequenceMatchOutput.MemberReference.class::isInstance));
        for (var reference : references) {
            NameId replacementId = new NameId();
            var copy = reference.withId(replacementId);
            assertEquals(reference.getClass(), copy.getClass());
            assertEquals(reference.name(), copy.name());
            assertEquals(replacementId, copy.id());
            assertEquals(copy, reference.transformPropertiesOnly(NameId.class, ignored -> replacementId));
            Source location = SourceTests.randomSource();
            var relocated = reference.withLocation(location);
            assertEquals(reference.getClass(), relocated.getClass());
            assertEquals(reference.id(), relocated.id());
            assertEquals(location, relocated.source());
            if (reference instanceof SequenceMatchOutput.MemberReference member) {
                var memberCopy = (SequenceMatchOutput.MemberReference) copy;
                assertEquals(0, memberCopy.stage());
                assertEquals("key", memberCopy.field());
                assertEquals(member.stage(), memberCopy.stage());
                assertEquals(member.field(), memberCopy.field());
            }
            Attribute sameName = new ReferenceAttribute(Source.EMPTY, null, reference.name(), DataType.KEYWORD);
            assertSame(reference, reference.resolve(List.of(sameName)));
            Attribute matching = new ReferenceAttribute(Source.EMPTY, null, "different_display_name", DataType.KEYWORD).withId(
                reference.id()
            );
            assertEquals(matching.withLocation(reference.source()), reference.resolve(List.of(sameName, matching)));
        }
    }

    /** Child replacement and property copying must retain the shared context while NULLIFY-style resolution is still open. */
    public void testPlanCopiesPreserveSequenceContext() {
        LogicalPlan plan = parsed();
        var scopes = plan.collect(SequenceMatchOutput.Scope.class);
        assertThat(scopes, hasSize(1));
        var scope = scopes.getFirst();
        assertEquals(1, scope.primary());
        Sequence sequence = (Sequence) scope.child();
        var stages = new ArrayList<>(sequence.children());
        SequenceStage first = (SequenceStage) stages.getFirst();
        stages.set(0, first.replaceChild(new Filter(first.source(), first.child(), Literal.TRUE)));
        LogicalPlan replacement = sequence.replaceChildren(stages);
        var scopeCopy = scope.replaceChild(replacement);
        assertEquals(scope.primary(), scopeCopy.primary());
        assertEquals(replacement, scopeCopy.child());
        assertPreservedProperties(scope, scopeCopy);
        assertEquals(scopeCopy, scope.replaceChildren(List.of(replacement)));
        assertEquals(scope.resolve(true).nodeProperties(), scope.transformPropertiesOnly(Boolean.class, ignored -> true).nodeProperties());
        assertEquals(scope.resolve(true).replaceChildren(List.of(replacement)), scopeCopy.resolve(true));

    }

    private static void assertPreservedProperties(UnaryPlan original, UnaryPlan copy) {
        // Node.equals compares children only; check opaque compiler state as well as the replaced child.
        assertEquals(
            original.nodeProperties().subList(1, original.nodeProperties().size()),
            copy.nodeProperties().subList(1, copy.nodeProperties().size())
        );
    }

    private static LogicalPlan parsed() {
        return PARSER.createStatement("""
            SEQUENCE BY key WITHIN 1m PRIMARY 2 (FROM a | KEEP key), (FROM b | KEEP key)
            | EVAL supporting = MEMBER(1, key)
            """).plan();
    }
}
