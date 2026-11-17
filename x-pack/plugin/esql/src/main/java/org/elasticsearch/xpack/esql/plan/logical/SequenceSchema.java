/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.TypeConflictedField;
import org.elasticsearch.xpack.esql.expression.function.fulltext.SingleFieldFullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToText;

import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/** Carries column restrictions and declared analysis across materialization without claiming an indexed source field. */
final class SequenceSchema {
    private SequenceSchema() {}

    /** The same typed boundary applies to wide outputs and to protected inputs of an earlier-member condition. */
    static Attribute reference(Attribute source, Source location, String name, @Nullable NameId id, boolean synthetic) {
        return reference(source, location, name, id, synthetic, null);
    }

    /** Stage provenance recovers an agreed mapped analyzer that ordinary EVAL or union references do not carry. */
    static Attribute reference(
        Attribute source,
        Source location,
        String name,
        @Nullable NameId id,
        boolean synthetic,
        @Nullable LogicalPlan stage
    ) {
        Attribute typed = source;
        if (source instanceof FieldAttribute field
            && field.field() instanceof TypeConflictedField conflict
            && conflict.isSingleTypePotentiallyUnmapped() == false) {
            typed = field.flagTypeConflicts();
        }
        if (typed instanceof UnsupportedAttribute unsupported) {
            return new UnsupportedAttribute(location, null, name, unsupported.field(), unsupported.unresolvedMessage(), id);
        }
        String valuesAnalyzer = AnalyzedTextExpression.valuesAnalyzerOf(source);
        if (stage != null && source.dataType() == DataType.TEXT) {
            // Inspect every producer before trusting metadata that may describe only the first union branch.
            valuesAnalyzer = analyzers(stage, source).sharedAnalyzer();
        } else if (valuesAnalyzer == null && source instanceof FieldAttribute field && field.field() instanceof TextEsField text) {
            // Field caps reports a name only when it can be resolved outside the index's own analysis settings.
            valuesAnalyzer = text.analyzerName();
        }
        return new ReferenceAttribute(location, null, name, source.dataType(), source.nullable(), id, synthetic, valuesAnalyzer);
    }

    /** Reject full-text use of an unexportable mapped analyzer without restricting ordinary values or string operations. */
    static void verifyFullText(Sequence sequence, LogicalPlan plan, Failures failures) {
        if (plan == sequence || plan.anyMatch(node -> node == sequence) == false) {
            return;
        }
        plan.forEachExpression(SingleFieldFullTextFunction.class, function -> {
            Expression searched = resolveTextInput(plan, function.field());
            if (searched instanceof Attribute attribute) {
                for (int column = 0; column < sequence.output().size(); column++) {
                    if (sequence.output().get(column).semanticEquals(attribute)) {
                        LogicalPlan stage = sequence.children().get(sequence.sourceStages().get(column));
                        Attribute input = sequence.sourceAttributes().get(column);
                        AnalyzerSummary analysis = analyzers(stage, input);
                        if (analysis.unknownColumn != null) {
                            failures.add(
                                fail(
                                    function,
                                    "Full-text function [{}] after SEQUENCE cannot preserve the mapped analyzer for column [{}]; "
                                        + "apply the full-text filter inside its source stage",
                                    function.functionName(),
                                    analysis.unknownColumn
                                )
                            );
                        } else if (analysis.names.size() > 1) {
                            failures.add(
                                fail(
                                    function,
                                    "Full-text function [{}] after SEQUENCE cannot preserve conflicting analyzers [{}] for column [{}]; "
                                        + "apply the full-text filter inside its source stage",
                                    function.functionName(),
                                    String.join(", ", analysis.names),
                                    analysis.mappedColumn == null ? function.field().sourceText() : analysis.mappedColumn
                                )
                            );
                        }
                    }
                }
            }
        });
    }

    /** An undeclared text conversion preserves provenance; an explicit analyzer chooses new runtime semantics. */
    private static Expression resolveTextInput(LogicalPlan plan, Expression input) {
        var aliases = AliasBindings.of(plan);
        Expression resolved = aliases.resolve(input, input);
        while (resolved instanceof ToText convert && AnalyzedTextExpression.valuesAnalyzerOf(resolved) == null) {
            resolved = aliases.resolve(convert.field(), convert.field());
        }
        return resolved;
    }

    private static AnalyzerSummary analyzers(LogicalPlan plan, Expression input) {
        AnalyzerSummary summary = new AnalyzerSummary();
        if (input.dataType() == DataType.TEXT) {
            collectAnalyzers(plan, input, summary);
        }
        return summary;
    }

    private static void collectAnalyzers(LogicalPlan plan, Expression input, AnalyzerSummary summary) {
        Expression original = resolveTextInput(plan, input);
        if (Expressions.isGuaranteedNull(original)) {
            return;
        }
        if (original instanceof FieldAttribute field && field.dataType() == DataType.TEXT) {
            if (summary.mappedColumn == null) {
                summary.mappedColumn = field.name();
            }
            String analyzer = field.field() instanceof TextEsField text ? text.analyzerName() : null;
            if (analyzer == null) {
                summary.unknownColumn = field.name();
            } else {
                summary.names.add(analyzer);
            }
            return;
        }
        if (original instanceof Attribute attribute && attribute.dataType() == DataType.TEXT) {
            for (UnionAll union : plan.collect(UnionAll.class)) {
                if (union.output().stream().anyMatch(column -> column.semanticEquals(attribute))) {
                    // Resolved union branches align by name but own distinct column identities.
                    for (LogicalPlan branch : union.children()) {
                        var onlyNull = Fork.producesOnlyNull(branch);
                        for (Attribute column : branch.output()) {
                            if (column.name().equals(attribute.name()) && onlyNull.test(column) == false) {
                                collectAnalyzers(branch, column, summary);
                            }
                        }
                    }
                    return;
                }
            }
        }
        // Genuine computations use their explicit values analyzer, or the ordinary runtime default.
        String declared = AnalyzedTextExpression.valuesAnalyzerOf(original);
        summary.names.add(declared == null ? AnalyzedTextExpression.STANDARD_ANALYZER : declared);
    }

    /** One materialized declaration must represent all potentially non-null producers of the column. */
    private static final class AnalyzerSummary {
        private final Set<String> names = new TreeSet<>();
        @Nullable
        private String unknownColumn;
        @Nullable
        private String mappedColumn;

        @Nullable
        String sharedAnalyzer() {
            return unknownColumn == null && names.size() == 1 ? names.iterator().next() : null;
        }
    }

    /** Compiler labels are pass-through projections; ordinary user aliases still receive the ordinary unsupported-field error. */
    static NamedExpression projection(Attribute value, String name, @Nullable NameId id, boolean synthetic) {
        return value instanceof UnsupportedAttribute
            ? reference(value, value.source(), name, id == null ? value.id() : id, synthetic)
            : new Alias(value.source(), name, value, id, synthetic);
    }
}
