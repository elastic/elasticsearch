/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Coordinator bindings for processed PRIMARY fields and explicit MEMBER references. */
public final class SequenceMatchOutput {
    private SequenceMatchOutput() {}

    /** A member field demand resolved against the stage's processed output, never an original-source lookup. */
    public record Member(int stage, String field) {}

    private record Context(int primary, Map<Member, NameId> members, Set<String> reservedNames) {}

    /** Parse-time declarations disappear before analysis and never acquire a wire representation. */
    public static final class Declaration extends UnaryPlan {
        private final int primary;
        private final Map<String, Integer> stageNames;

        /** Named references lower to existing positions before analysis, so aliases never enter a runtime plan. */
        public Declaration(Source source, LogicalPlan child, int primary, Map<String, Integer> stageNames) {
            super(source, child);
            this.primary = primary;
            this.stageNames = Map.copyOf(stageNames);
        }

        public Map<String, Integer> stageNames() {
            return stageNames;
        }

        @Override
        protected NodeInfo<Declaration> info() {
            return NodeInfo.create(this, Declaration::new, child(), primary, stageNames);
        }

        @Override
        public Declaration replaceChild(LogicalPlan child) {
            return new Declaration(source(), child, primary, stageNames);
        }

        @Override
        public boolean expressionsResolved() {
            return false;
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException("parse-only sequence declaration");
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException("parse-only sequence declaration");
        }
    }

    /** Compile stage selections and MEMBER demands before ordinary field discovery. */
    public static EsqlStatement prepare(EsqlStatement statement) {
        LogicalPlan plan = statement.plan();
        if (plan instanceof Explain explain) {
            EsqlStatement prepared = prepare(new EsqlStatement(explain.query(), statement.settings(), statement.letBindings()));
            if (prepared.plan() == explain.query()) return statement;
            return new EsqlStatement(new Explain(explain.source(), prepared.plan()), statement.settings(), statement.letBindings());
        }
        for (var binding : statement.letBindings()) {
            rejectSequenceOutputInLet(binding.plan());
        }
        rejectSequenceOutputInMembershipSubqueries(plan);
        List<Declaration> declarations = plan.collect(Declaration.class);
        if (declarations.isEmpty()) {
            plan.forEachExpressionDown(UnresolvedFunction.class, f -> {
                if (f.name().equalsIgnoreCase("member")) {
                    throw new ParsingException(f.source(), "MEMBER requires SEQUENCE with an explicit PRIMARY");
                }
            });
            return plan == statement.plan() ? statement : new EsqlStatement(plan, statement.settings(), statement.letBindings());
        }
        Declaration header = declarations.getFirst();
        Sequence sequence = prepareStageConditions((Sequence) header.child(), header.stageNames);
        if (statement.letBindings().isEmpty() == false) {
            throw new ParsingException(plan.source(), "LET is outside the sequence output prototype");
        }
        if (header.primary > sequence.children().size()) {
            throw new ParsingException(header.source(), "PRIMARY must name an existing one-based member");
        }
        LogicalPlan cursor = plan;
        while (cursor != header) {
            if (cursor instanceof LookupJoin join) {
                join.right().forEachExpressionDown(UnresolvedFunction.class, function -> {
                    if (function.name().equalsIgnoreCase("member")) {
                        throw new ParsingException(function.source(), "MEMBER is not available inside the joined side of LOOKUP JOIN");
                    }
                });
                cursor = join.left();
                continue;
            }
            if (cursor instanceof UnaryPlan == false) {
                throw new ParsingException(cursor.source(), "Command [{}] is outside the sequence output prototype", cursor.nodeName());
            }
            cursor = ((UnaryPlan) cursor).child();
        }
        Map<Member, NameId> members = new LinkedHashMap<>();
        plan = plan.transformUp(Declaration.class, declaration -> sequence);
        plan = plan.transformExpressionsUp(UnresolvedFunction.class, f -> {
            if (f.name().equalsIgnoreCase("member") == false) return f;
            if (f.children().size() != 2 || (f.children().get(1) instanceof UnresolvedAttribute) == false) {
                throw new ParsingException(
                    f.source(),
                    "MEMBER requires a literal member number or an unquoted stage name and a field identifier"
                );
            }
            Expression selected = f.children().get(0);
            final int stage;
            if (selected instanceof Literal number) {
                if ((number.value() instanceof Integer) == false || number.source().text().matches("[0-9]+") == false) {
                    throw new ParsingException(f.source(), "MEMBER requires a literal member number");
                }
                stage = (Integer) number.value();
                if (stage < 1 || stage > sequence.children().size()) {
                    throw new ParsingException(f.source(), "MEMBER must name an existing one-based member");
                }
            } else if (selected instanceof UnresolvedAttribute name
                && name.qualifier() == null
                && name.source().text().equals(name.name())
                && name.name().contains(".") == false) {
                    Integer position = header.stageNames.get(name.name());
                    if (position == null) {
                        throw new ParsingException(f.source(), "Unknown SEQUENCE stage name [{}] in MEMBER", name.name());
                    }
                    stage = position;
                } else {
                    throw new ParsingException(
                        f.source(),
                        "MEMBER requires a literal member number or an unquoted stage name and a field identifier"
                    );
                }
            if (stage == header.primary) {
                throw new ParsingException(f.source(), "Use ordinary field names for the PRIMARY member");
            }
            String field = ((UnresolvedAttribute) f.children().get(1)).name();
            Member member = new Member(stage - 1, field);
            NameId binding = members.computeIfAbsent(member, ignored -> new NameId());
            return new MemberReference(f.source(), member, binding);
        });
        return prepare(new EsqlStatement(plan, statement.settings(), statement.letBindings()), header.primary - 1, members);
    }

    /** Consecutive trailing WHEREs share one own-row binding point before the permitted trailing projections. */
    private static Sequence prepareStageConditions(Sequence sequence, Map<String, Integer> stageNames) {
        List<LogicalPlan> stages = new ArrayList<>();
        for (int ownStage = 0; ownStage < sequence.children().size(); ownStage++) {
            SequenceStage stage = (SequenceStage) sequence.children().get(ownStage);
            LogicalPlan stagePlan = stage.child();
            LogicalPlan cursor = stagePlan;
            List<UnaryPlan> projections = new ArrayList<>();
            while (isStageProjection(cursor)) {
                projections.add((UnaryPlan) cursor);
                cursor = ((UnaryPlan) cursor).child();
            }
            cursor = mergeTrailingFilters(cursor);
            if (cursor instanceof Filter filter && containsMember(filter.condition())) {
                List<Expression> own = new ArrayList<>();
                List<Expression> deferred = new ArrayList<>();
                List<List<Integer>> referenced = new ArrayList<>();
                final int currentStage = ownStage;
                for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
                    if (containsMember(conjunct) == false) {
                        own.add(conjunct);
                        continue;
                    }
                    conjunct.forEachDown(expression -> {
                        if (expression instanceof InSubquery || expression instanceof MultiColumnInSubquery) {
                            throw new ParsingException(
                                expression.source(),
                                "IN subqueries are not supported in a SEQUENCE MEMBER condition"
                            );
                        }
                    });
                    Set<Integer> earlier = new HashSet<>();
                    deferred.add(conjunct.transformUp(UnresolvedFunction.class, function -> {
                        if (function.name().equalsIgnoreCase("member") == false) return function;
                        MemberReference reference = stageMember(function, stageNames, sequence.children().size(), currentStage);
                        earlier.add(reference.stage());
                        return reference;
                    }));
                    referenced.add(earlier.stream().sorted().toList());
                }
                cursor = own.isEmpty() ? filter.child() : new Filter(filter.source(), filter.child(), Predicates.combineAnd(own));
                for (int conjunct = deferred.size() - 1; conjunct >= 0; conjunct--) {
                    cursor = new SequenceCondition(
                        filter.source(),
                        cursor,
                        ownStage,
                        deferred.get(conjunct),
                        referenced.get(conjunct),
                        List.of()
                    );
                }
            }
            for (int projection = projections.size() - 1; projection >= 0; projection--) {
                cursor = projections.get(projection).replaceChild(cursor);
            }
            stage = stage.replaceChild(cursor);
            stage.forEachExpressionDown(UnresolvedFunction.class, function -> {
                if (function.name().equalsIgnoreCase("member")) {
                    throw misplacedStageMember(stagePlan, function);
                }
            });
            stages.add(stage);
        }
        return sequence.replaceChildren(stages);
    }

    private static boolean isStageProjection(LogicalPlan plan) {
        return plan instanceof Keep || plan instanceof Drop || plan instanceof Rename;
    }

    /** Attribute a misplaced filter to the first command that prevents it from acting as a final stage filter. */
    private static ParsingException misplacedStageMember(LogicalPlan stagePlan, UnresolvedFunction member) {
        var following = new ArrayDeque<UnaryPlan>();
        for (LogicalPlan cursor = stagePlan; cursor instanceof UnaryPlan command; cursor = command.child()) {
            if (command instanceof Filter filter && filter.condition().anyMatch(expression -> expression == member)) {
                while (following.peekFirst() instanceof Filter) {
                    following.removeFirst();
                }
                while (isStageProjection(following.peekFirst())) {
                    following.removeFirst();
                }
                if (following.peekFirst() instanceof Eval eval) {
                    return new ParsingException(
                        eval.source(),
                        "EVAL cannot follow a SEQUENCE stage WHERE that uses MEMBER. "
                            + "Move EVAL before that WHERE if the move preserves the inputs to both commands"
                    );
                }
                if (following.isEmpty() == false) {
                    return new ParsingException(
                        following.getFirst().source(),
                        "This command cannot follow the final WHERE of a SEQUENCE stage that uses MEMBER. "
                            + "After that WHERE, only KEEP, DROP or RENAME may follow"
                    );
                }
                break;
            }
            following.addFirst(command);
        }
        return new ParsingException(
            member.source(),
            "MEMBER is only supported in the final WHERE of a SEQUENCE stage; only KEEP, DROP or RENAME may follow that WHERE"
        );
    }

    /** No schema-changing command separates these filters, so their fields keep the same binding and source locations. */
    private static LogicalPlan mergeTrailingFilters(LogicalPlan plan) {
        while (plan instanceof Filter filter && filter.child() instanceof Filter preceding) {
            plan = new Filter(
                filter.source(),
                preceding.child(),
                Predicates.combineAnd(List.of(preceding.condition(), filter.condition()))
            );
        }
        return plan;
    }

    private static boolean containsMember(Expression expression) {
        return expression.anyMatch(e -> e instanceof UnresolvedFunction function && function.name().equalsIgnoreCase("member"));
    }

    private static MemberReference stageMember(UnresolvedFunction function, Map<String, Integer> names, int count, int ownStage) {
        if (function.children().size() != 2 || function.children().get(1) instanceof UnresolvedAttribute == false) {
            throw new ParsingException(
                function.source(),
                "MEMBER requires a literal member number or an unquoted stage name and a field identifier"
            );
        }
        Expression selected = function.children().getFirst();
        final int stage;
        if (selected instanceof Literal number) {
            if (number.value() instanceof Integer == false || number.source().text().matches("[0-9]+") == false) {
                throw new ParsingException(function.source(), "MEMBER requires a literal member number");
            }
            stage = (Integer) number.value();
            if (stage < 1 || stage > count) {
                throw new ParsingException(function.source(), "MEMBER must name an existing one-based member");
            }
        } else if (selected instanceof UnresolvedAttribute name
            && name.qualifier() == null
            && name.source().text().equals(name.name())
            && name.name().contains(".") == false) {
                Integer position = names.get(name.name());
                if (position == null) {
                    throw new ParsingException(function.source(), "Unknown SEQUENCE stage name [{}] in MEMBER", name.name());
                }
                stage = position;
            } else {
                throw new ParsingException(
                    function.source(),
                    "MEMBER requires a literal member number or an unquoted stage name and a field identifier"
                );
            }
        if (stage - 1 >= ownStage) {
            throw new ParsingException(
                function.source(),
                "MEMBER in SEQUENCE stage [{}] must refer to an earlier stage; found [{}]",
                ownStage + 1,
                stage
            );
        }
        return new MemberReference(
            function.source(),
            new Member(stage - 1, ((UnresolvedAttribute) function.children().get(1)).name()),
            new NameId()
        );
    }

    /** LET plans are expanded after preparation, so inspect their expression-held subqueries before declarations can escape. */
    private static void rejectSequenceOutputInLet(LogicalPlan plan) {
        var pending = new ArrayDeque<LogicalPlan>();
        pending.add(plan);
        while (pending.isEmpty() == false) {
            LogicalPlan current = pending.removeFirst();
            if (current instanceof Explain explain) {
                pending.add(explain.query());
            }
            current.forEachExpression(expression -> {
                if (expression instanceof UnresolvedFunction function && function.name().equalsIgnoreCase("member")) {
                    throw new ParsingException(function.source(), "LET is outside the sequence output prototype");
                }
                if (expression instanceof InSubquery in) {
                    pending.add(in.subquery());
                } else if (expression instanceof MultiColumnInSubquery in) {
                    pending.add(in.subquery());
                }
            });
            pending.addAll(current.children());
        }
    }

    /** Output preparation owns one pipeline; expression-held sequence declarations must not escape unprepared. */
    private static void rejectSequenceOutputInMembershipSubqueries(LogicalPlan plan) {
        var pending = new ArrayDeque<LogicalPlan>();
        pending.add(plan);
        while (pending.isEmpty() == false) {
            LogicalPlan current = pending.removeFirst();
            current.forEachExpression(expression -> {
                LogicalPlan subquery;
                if (expression instanceof InSubquery in) {
                    subquery = in.subquery();
                } else if (expression instanceof MultiColumnInSubquery in) {
                    subquery = in.subquery();
                } else {
                    return;
                }
                subquery.forEachExpressionDown(UnresolvedFunction.class, function -> {
                    if (function.name().equalsIgnoreCase("member")) {
                        throw new ParsingException(function.source(), "MEMBER is not available inside an IN subquery");
                    }
                });
                pending.add(subquery);
            });
            pending.addAll(current.children());
        }
    }

    /** An analysis-only reference to a compiler binding, independent of its collision-free temporary name. */
    public static class BindingReference extends UnresolvedAttribute {
        private BindingReference(Source source, String name, NameId binding) {
            this(source, name, binding, null);
        }

        private BindingReference(Source source, String name, NameId binding, @Nullable String message) {
            super(source, name, binding, message);
        }

        @Override
        protected NodeInfo<? extends BindingReference> info() {
            return NodeInfo.create(
                this,
                (source, name, binding, message, custom) -> new BindingReference(source, name, binding, custom ? message : null),
                name(),
                id(),
                unresolvedMessage(),
                customMessage()
            );
        }

        @Override
        public BindingReference withId(NameId id) {
            return new BindingReference(source(), name(), id, customMessage() ? unresolvedMessage() : null);
        }

        @Override
        public BindingReference withLocation(Source source) {
            return new BindingReference(source, name(), id(), customMessage() ? unresolvedMessage() : null);
        }

        /** A diagnostic must not turn a protected compiler binding into an ordinary NULLIFY/name lookup. */
        @Override
        public BindingReference withUnresolvedMessage(@Nullable String message) {
            return new BindingReference(source(), name(), id(), message);
        }

        /** Resolve only a binding still present in the current pipeline, never a similarly named ordinary field. */
        public Attribute resolve(List<Attribute> output) {
            for (Attribute attribute : output) {
                if (attribute.id().equals(id())) return attribute.withLocation(source());
            }
            // A late NULLIFY field may make this binding available on the next analysis pass.
            // Keep its protected identity and subtype rather than falling back to name resolution.
            return this;
        }
    }

    /** Build unresolved field demands and protected source bindings for the linear prototype surface. */
    public static final class MemberReference extends BindingReference {
        private final Member member;

        private MemberReference(Source source, Member member, NameId binding) {
            this(source, member, binding, null);
        }

        private MemberReference(Source source, Member member, NameId binding, @Nullable String message) {
            super(source, "MEMBER(" + (member.stage() + 1) + ", " + member.field() + ")", binding, message);
            this.member = member;
        }

        @Override
        protected NodeInfo<MemberReference> info() {
            return NodeInfo.create(
                this,
                (source, member, binding, message, custom) -> new MemberReference(source, member, binding, custom ? message : null),
                member,
                id(),
                unresolvedMessage(),
                customMessage()
            );
        }

        @Override
        public MemberReference withId(NameId id) {
            return new MemberReference(source(), member, id, customMessage() ? unresolvedMessage() : null);
        }

        @Override
        public MemberReference withLocation(Source source) {
            return new MemberReference(source, member, id(), customMessage() ? unresolvedMessage() : null);
        }

        /** Missing-member diagnostics retain the earlier stage and field for scope-aware NULLIFY handling. */
        @Override
        public MemberReference withUnresolvedMessage(@Nullable String message) {
            return new MemberReference(source(), member, id(), message);
        }

        public String field() {
            return member.field();
        }

        public int stage() {
            return member.stage();
        }
    }

    /** Publish processed PRIMARY fields and explicitly demanded MEMBER bindings through ordinary projections. */
    private static EsqlStatement prepare(EsqlStatement statement, int primary, Map<Member, NameId> members) {
        Set<String> names = new HashSet<>();
        statement.plan().forEachExpressionDown(Alias.class, alias -> names.add(alias.name()));
        statement.plan().forEachExpressionDown(Attribute.class, attribute -> names.add(attribute.name()));
        members.keySet().forEach(member -> names.add(member.field()));
        Context context = new Context(primary, Map.copyOf(members), Set.copyOf(names));
        LogicalPlan plan = statement.plan().transformUp(Sequence.class, sequence -> new Scope(sequence.source(), sequence, context));
        return new EsqlStatement(plan, statement.settings(), statement.letBindings());
    }

    /** Adds actual member field demands without removing ordinary fields that happen to match a diagnostic label. */
    public static Set<String> discoverFields(LogicalPlan parsed, Set<String> original) {
        Set<String> fields = new HashSet<>(original);
        parsed.forEachExpressionDown(MemberReference.class, member -> fields.add(member.field()));
        return fields;
    }

    /** Reserve an actual free name: a quoted user identifier can otherwise spell the synthetic-name convention. */
    private static String temporaryName(Set<String> usedNames, String prefix, NameId id) {
        String base = Attribute.rawTemporaryName(prefix, id.toString());
        String name = base;
        int suffix = 1;
        while (usedNames.add(name) == false) {
            name = base + "_" + suffix++;
        }
        return name;
    }

    /** Analysis-only context lowered to ordinary typed projections after its sequence resolves. */
    public static final class Scope extends UnaryPlan {
        private final Context context;
        private final boolean keepUnmapped;

        private Scope(Source source, LogicalPlan child, Context context) {
            this(source, child, context, false);
        }

        private Scope(Source source, LogicalPlan child, Context context, boolean keepUnmapped) {
            super(source, child);
            this.context = context;
            this.keepUnmapped = keepUnmapped;
        }

        @Override
        protected NodeInfo<Scope> info() {
            return NodeInfo.create(this, Scope::new, child(), context, keepUnmapped);
        }

        @Override
        public Scope replaceChild(LogicalPlan child) {
            return new Scope(source(), child, context, keepUnmapped);
        }

        @Override
        public boolean equals(Object other) {
            return super.equals(other) && keepUnmapped == ((Scope) other).keepUnmapped;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), keepUnmapped);
        }

        @Override
        public boolean expressionsResolved() {
            return keepUnmapped;
        }

        @Override
        public List<Attribute> output() {
            return keepUnmapped ? Expressions.asAttributes(projections()) : child().output();
        }

        public int primary() {
            return context.primary();
        }

        /** Retain stage provenance while NULLIFY can still widen the processed stage outputs. */
        public LogicalPlan resolve(boolean keepUnmapped) {
            return keepUnmapped ? new Scope(source(), child(), context, true) : lower();
        }

        @Override
        public boolean skipTelemetry() {
            return true;
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException("analysis-only sequence output node");
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException("analysis-only sequence output node");
        }

        /** Binds requested ordinary fields to their stage output; a removed field is not reconstructed from source. */
        public LogicalPlan lower() {
            return new Project(source(), child(), projections());
        }

        private List<NamedExpression> projections() {
            Sequence sequence = (Sequence) child();
            List<NamedExpression> output = new ArrayList<>();
            Set<String> usedNames = new HashSet<>(context.reservedNames());
            sequence.sourceAttributes().forEach(attribute -> usedNames.add(attribute.name()));
            sequence.output().forEach(attribute -> usedNames.add(attribute.name()));
            int keyCount = ((SequenceStage) sequence.children().getFirst()).keys().size();
            for (int i = 0; i < sequence.output().size(); i++) {
                Attribute input = sequence.sourceAttributes().get(i);
                Attribute value = sequence.output().get(i);
                int stage = sequence.sourceStages().get(i);
                if (Sequence.isProvenanceAttribute(input)) {
                    Set<String> names = new HashSet<>();
                    ((SequenceStage) sequence.children().get(context.primary())).output()
                        .stream()
                        .filter(attribute -> attribute.synthetic() == false)
                        .forEach(attribute -> names.add(attribute.name()));
                    String name = "sequence_provenance";
                    int suffix = 1;
                    while (names.add(name) == false)
                        name = "sequence_provenance_" + suffix++;
                    output.add(SequenceSchema.projection(value, name, keepUnmapped ? value.id() : null, false));
                    continue;
                }
                if (input.synthetic() == false && (i < keyCount || stage == context.primary())) {
                    String name = i < keyCount
                        ? ((SequenceStage) sequence.children().get(context.primary())).keys().get(i).name()
                        : input.name();
                    // Recomputing a compiler projection must not change an already published binding.
                    output.add(SequenceSchema.projection(value, name, keepUnmapped ? value.id() : null, false));
                }
                for (var entry : context.members().entrySet()) {
                    Member member = entry.getKey();
                    // A key slot is shared by value, but each member names it through its own stage's key attribute.
                    // An opposite-named non-key payload must still bind to its actual member instead of that slot.
                    String name = i < keyCount
                        ? ((SequenceStage) sequence.children().get(member.stage())).keys().get(i).name()
                        : input.name();
                    if ((i < keyCount || stage == member.stage()) && input.synthetic() == false && name.equals(member.field())) {
                        NameId binding = entry.getValue();
                        // Reserve against parsed and resolved user names; MEMBER resolution uses only the binding ID.
                        output.add(SequenceSchema.projection(value, temporaryName(usedNames, "sequence_member", binding), binding, true));
                    }
                }
            }
            return output;
        }
    }

}
