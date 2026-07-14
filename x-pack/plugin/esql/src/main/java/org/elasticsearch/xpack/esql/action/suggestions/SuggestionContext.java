/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.action.suggestions.CursorLocation.OffsetRange;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

/**
 * Describes what the caret is completing, derived from an analyzed+optimized {@link LogicalPlan}, the
 * original query string, and an absolute cursor offset.
 *
 * <p>The {@link #command} is the logical-plan node whose {@link Source} range contains the cursor (or
 * {@code null} when the cursor sits past the last command, i.e. a fresh pipe position). The
 * {@link #kind} refines the sub-position; {@link #targetField} names the single field being compared
 * when the cursor sits in a literal.
 *
 * <p>{@code command == null} (paired with {@code Kind.PIPE_POSITION} and {@code targetField == null})
 * is not a special case a reader needs to track down: it is absorbed in exactly one place
 * ({@link #schemaSource}, computed once at construction time) and read defensively in exactly one other
 * ({@code TransportEsqlSuggestionsAction}'s {@code context.kind() != STRING_LITERAL_EQUALITY ||
 * context.targetField() == null} guard). There is no wider proliferation of null-checks for this case.
 */
public record SuggestionContext(Kind kind, @Nullable LogicalPlan command, @Nullable String targetField, LogicalPlan schemaSource) {

    public enum Kind {
        /** Caret inside a string literal on the right of an equality comparison — value completion for one field. */
        STRING_LITERAL_EQUALITY,
        /** Caret on a numeric literal in a range comparison — range completion for one field (detection deferred). */
        NUMERIC_LITERAL_RANGE,
        /** Caret where a field name is expected (KEEP, WHERE before an operator, etc.) — all fields, no statistics. */
        FIELD_NAME,
        /** Caret at a fresh pipe position after the last command — all fields; statistics only if requested. */
        PIPE_POSITION
    }

    /**
     * Detect the completion context. Walks the plan top-down to find the deepest command node whose
     * source range contains the cursor. If the cursor falls after every command, it is treated as a
     * pipe position. Inside a {@link Filter}, a literal child whose range contains the cursor narrows
     * the context to a single-field value/range completion.
     */
    public static SuggestionContext detect(LogicalPlan plan, CursorLocation locations, int cursor) {
        LogicalPlan containing = findContainingCommand(plan, locations, cursor);
        if (containing == null) {
            return new SuggestionContext(Kind.PIPE_POSITION, null, null, plan);
        }
        if (containing instanceof Filter filter) {
            SuggestionContext literalContext = detectLiteral(filter, locations, cursor);
            if (literalContext != null) {
                return literalContext;
            }
        }
        return new SuggestionContext(Kind.FIELD_NAME, containing, null, schemaSourceOf(containing));
    }

    /**
     * The deepest command node whose source range contains the cursor, or {@code null} if none does.
     * "Deepest" — nearest the leaves — wins so that, e.g., a {@code WHERE} nested inside a larger plan
     * is preferred over an ancestor whose range also spans the cursor.
     *
     * <p>{@link Limit} nodes are skipped entirely, regardless of their own source range. Two
     * independent sources put a stale or duplicated {@link Source} on a {@code Limit}: the analyzer's
     * default-limit insertion, and the logical optimizer's limit push-down rules, which relocate a
     * {@code Limit} deeper into the tree while leaving it
     * carrying the very same source text as the command it was combined with. A plain "last (i.e.
     * deepest) match wins" walk would then let that relocated, unrelated {@code Limit} steal the match
     * away from the real user-authored command (e.g. {@code KEEP}) sitting above it, corrupting
     * {@link #schemaSource}. A {@code Limit} never carries a field-name or literal completion
     * opportunity of its own (only its numeric argument, which this detector does not target), so
     * excluding it entirely is safe.
     */
    @Nullable
    private static LogicalPlan findContainingCommand(LogicalPlan plan, CursorLocation locations, int cursor) {
        LogicalPlan[] found = new LogicalPlan[1];
        plan.forEachDown(node -> {
            if (node instanceof Limit) {
                return;
            }
            Source source = node.source();
            if (source == Source.EMPTY || source.text().isEmpty()) {
                return;
            }
            OffsetRange range = locations.range(source);
            if (range.containsInclusive(cursor)) {
                found[0] = node;
            }
        });
        return found[0];
    }

    /**
     * If the cursor sits inside a literal child of the filter condition, return a single-field
     * completion context. A string literal implies equality/value completion; a numeric literal
     * implies range completion (population deferred). Returns {@code null} when the cursor is not on a
     * literal (i.e. it is at a field-name slot).
     */
    @Nullable
    private static SuggestionContext detectLiteral(Filter filter, CursorLocation locations, int cursor) {
        Expression condition = filter.condition();
        Literal[] hit = new Literal[1];
        condition.forEachDown(Literal.class, literal -> {
            Source source = literal.source();
            if (source == Source.EMPTY || source.text().isEmpty()) {
                return;
            }
            if (locations.range(source).containsInclusive(cursor)) {
                hit[0] = literal;
            }
        });
        if (hit[0] == null) {
            return null;
        }
        String field = comparedFieldName(condition, hit[0]);
        DataType type = hit[0].dataType();
        if (DataType.isString(type)) {
            return new SuggestionContext(Kind.STRING_LITERAL_EQUALITY, filter, field, schemaSourceOf(filter));
        }
        if (type.isNumeric()) {
            return new SuggestionContext(Kind.NUMERIC_LITERAL_RANGE, filter, field, schemaSourceOf(filter));
        }
        return new SuggestionContext(Kind.STRING_LITERAL_EQUALITY, filter, field, schemaSourceOf(filter));
    }

    /**
     * Best-effort extraction of the field name compared against {@code literal} inside {@code condition}.
     * Looks for a binary node that has both the literal and a single named field as descendants.
     */
    @Nullable
    private static String comparedFieldName(Expression condition, Literal literal) {
        String[] field = new String[1];
        condition.forEachDown(node -> {
            if (node.children().size() == 2 && node.children().contains(literal)) {
                for (Expression child : node.children()) {
                    if (child != literal) {
                        child.forEachDown(org.elasticsearch.xpack.esql.core.expression.NamedExpression.class, named -> {
                            if (field[0] == null) {
                                field[0] = named.name();
                            }
                        });
                    }
                }
            }
        });
        return field[0];
    }

    /**
     * The logical-plan node whose output schema should populate the suggestions — i.e. the command
     * immediately preceding {@code command}. For a command in the chain it is that command's child.
     */
    private static LogicalPlan schemaSourceOf(LogicalPlan command) {
        if (command instanceof UnaryPlan unary) {
            return unary.child();
        }
        return command;
    }
}
