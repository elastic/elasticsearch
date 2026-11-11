/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Prototype coordinator bindings for processed primary fields and row-associated authentic originals. */
public final class SequenceMatchOutput {
    private SequenceMatchOutput() {}

    /** A member field demand resolved against the stage's processed output, never an original-source lookup. */
    public record Member(int stage, String field) {}

    /** Original input and exported slots have separate identities; names never establish provenance. */
    private record Role(int stage, String metadata, NameId input, NameId output) {
        String diagnosticName() {
            return "member_" + (stage + 1) + metadata;
        }
    }

    private record Context(
        int primary,
        Map<Member, NameId> members,
        List<Role> roles,
        List<NameId> keys,
        Map<NameId, NameId> published,
        List<String> keyNames,
        Set<String> reservedNames
    ) {}

    /** Parse-time declarations disappear before analysis and never acquire a wire representation. */
    public static final class Declaration extends UnaryPlan {
        private final int primary;

        public Declaration(Source source, LogicalPlan child, int primary) {
            super(source, child);
            this.primary = primary;
        }

        @Override
        protected NodeInfo<Declaration> info() {
            return NodeInfo.create(this, Declaration::new, child(), primary);
        }

        @Override
        public Declaration replaceChild(LogicalPlan child) {
            return new Declaration(source(), child, primary);
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

    /** Compile the narrow authoring surface before field discovery, retaining genuine metadata by identity. */
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
            return statement;
        }
        List<Declaration> headers = declarations.stream().filter(d -> d.primary > 0).toList();
        if (headers.size() != 1 || (headers.getFirst().child() instanceof Sequence) == false) {
            throw new ParsingException(plan.source(), "RETURN MATCHES requires one SEQUENCE with an explicit PRIMARY");
        }
        Declaration header = headers.getFirst();
        Sequence sequence = (Sequence) header.child();
        boolean capture = declarations.size() == 2;
        if (declarations.size() > 2
            || (capture && (plan instanceof Declaration) == false)
            || (capture && ((Declaration) plan).primary != 0)) {
            throw new ParsingException(plan.source(), "RETURN MATCHES must be the final command");
        }
        if (statement.letBindings().isEmpty() == false) {
            throw new ParsingException(plan.source(), "LET is outside the sequence output prototype");
        }
        if (header.primary > sequence.children().size()) {
            throw new ParsingException(header.source(), "PRIMARY must name an existing one-based member");
        }
        // PRIMARY is ordinary presentation. Only retained evidence needs the row-preserving outer boundary.
        LogicalPlan cursor = capture ? ((Declaration) plan).child() : plan;
        while (cursor != header) {
            if ((cursor instanceof UnaryPlan) == false
                || (capture
                    && (cursor instanceof Eval
                        || cursor instanceof Keep
                        || cursor instanceof Drop
                        || cursor instanceof Rename
                        || cursor instanceof Filter
                        || cursor instanceof OrderBy
                        || cursor instanceof Limit) == false)) {
                throw new ParsingException(cursor.source(), "Command [{}] is outside the sequence output prototype", cursor.nodeName());
            }
            if (capture) {
                // Membership inside a stage filters events before matching; only the outer retained pipeline excludes joins.
                cursor.forEachExpression(expression -> {
                    if (expression instanceof InSubquery || expression instanceof MultiColumnInSubquery) {
                        throw new ParsingException(
                            expression.source(),
                            "IN subqueries are outside the sequence output prototype with RETURN MATCHES"
                        );
                    }
                });
            }
            cursor = ((UnaryPlan) cursor).child();
        }
        sequence.forEachExpressionDown(UnresolvedFunction.class, f -> {
            if (f.name().equalsIgnoreCase("member")) {
                throw new ParsingException(f.source(), "MEMBER is only available after SEQUENCE");
            }
        });
        Map<Member, NameId> members = new LinkedHashMap<>();
        plan = plan.transformUp(Declaration.class, Declaration::child);
        plan = plan.transformExpressionsUp(UnresolvedFunction.class, f -> {
            if (f.name().equalsIgnoreCase("member") == false) return f;
            if (f.children().size() != 2
                || (f.children().get(0) instanceof Literal) == false
                || (f.children().get(1) instanceof UnresolvedAttribute) == false) {
                throw new ParsingException(f.source(), "MEMBER requires a literal member number and a field identifier");
            }
            Literal number = (Literal) f.children().get(0);
            if ((number.value() instanceof Integer) == false || number.source().text().matches("[0-9]+") == false) {
                throw new ParsingException(f.source(), "MEMBER requires a literal member number");
            }
            int stage = (Integer) number.value();
            if (stage < 1 || stage > sequence.children().size()) {
                throw new ParsingException(f.source(), "MEMBER must name an existing one-based member");
            }
            if (stage == header.primary) {
                throw new ParsingException(f.source(), "Use ordinary field names for the PRIMARY member");
            }
            String field = ((UnresolvedAttribute) f.children().get(1)).name();
            Member member = new Member(stage - 1, field);
            NameId binding = members.computeIfAbsent(member, ignored -> new NameId());
            return new MemberReference(f.source(), member, binding);
        });
        return prepare(new EsqlStatement(plan, statement.settings(), statement.letBindings()), header.primary - 1, capture, members);
    }

    /** LET plans are expanded after preparation, so inspect their expression-held subqueries before declarations can escape. */
    private static void rejectSequenceOutputInLet(LogicalPlan plan) {
        var pending = new ArrayDeque<LogicalPlan>();
        pending.add(plan);
        while (pending.isEmpty() == false) {
            LogicalPlan current = pending.removeFirst();
            if (current instanceof Declaration) {
                throw new ParsingException(current.source(), "LET is outside the sequence output prototype");
            }
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
                List<Declaration> declarations = subquery.collect(Declaration.class);
                if (declarations.isEmpty() == false) {
                    throw new ParsingException(declarations.getFirst().source(), "SEQUENCE is not supported as an IN subquery source");
                }
                pending.add(subquery);
            });
            pending.addAll(current.children());
        }
    }

    /** An analysis-only reference to a compiler binding, independent of its collision-free temporary name. */
    public static class BindingReference extends UnresolvedAttribute {
        private BindingReference(Source source, String name, NameId binding) {
            super(source, name, binding, null);
        }

        @Override
        protected NodeInfo<? extends BindingReference> info() {
            return NodeInfo.create(this, BindingReference::new, name(), id());
        }

        @Override
        public BindingReference withId(NameId id) {
            return new BindingReference(source(), name(), id);
        }

        @Override
        public BindingReference withLocation(Source source) {
            return new BindingReference(source, name(), id());
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
            super(source, "MEMBER(" + (member.stage() + 1) + ", " + member.field() + ")", binding);
            this.member = member;
        }

        @Override
        protected NodeInfo<MemberReference> info() {
            return NodeInfo.create(this, MemberReference::new, member, id());
        }

        @Override
        public MemberReference withId(NameId id) {
            return new MemberReference(source(), member, id);
        }

        @Override
        public MemberReference withLocation(Source source) {
            return new MemberReference(source, member, id());
        }

        public String field() {
            return member.field();
        }

        public int stage() {
            return member.stage();
        }
    }

    /** Projects one sequence's primary fields, adding source bindings only for the narrow retained-evidence surface. */
    private static EsqlStatement prepare(EsqlStatement statement, int primary, boolean capture, Map<Member, NameId> members) {
        List<Sequence> sequences = statement.plan().collect(Sequence.class);
        if (sequences.size() != 1) throw new IllegalArgumentException("sequence output requires one sequence");
        Sequence sequence = sequences.getFirst();
        if (primary < 0 || primary >= sequence.children().size()) throw new IllegalArgumentException("invalid sequence primary");
        Set<String> names = new HashSet<>();
        statement.plan().forEachExpressionDown(Alias.class, alias -> names.add(alias.name()));
        statement.plan().forEachExpressionDown(Attribute.class, attribute -> names.add(attribute.name()));
        members.keySet().forEach(member -> names.add(member.field()));
        Set<String> reservedNames = Set.copyOf(names);
        Set<String> inputNames = new HashSet<>(reservedNames);
        List<Role> roles = new ArrayList<>();
        List<LogicalPlan> stages = new ArrayList<>();
        for (int i = 0; i < sequence.children().size(); i++) {
            SequenceStage stage = (SequenceStage) sequence.children().get(i);
            if (capture == false) {
                // The ordinary SequenceStage verifier owns the event-stage contract without retained originals.
                stages.add(stage);
                continue;
            }
            requireSimpleStage(stage.child());
            List<UnresolvedRelation> relations = stage.child().collect(UnresolvedRelation.class);
            if (relations.size() != 1) throw new IllegalArgumentException("sequence output requires one event relation per stage");
            UnresolvedRelation relation = relations.getFirst();
            List<Alias> originals = new ArrayList<>();
            for (String name : List.of("_index", "_id", "_source")) {
                MetadataAttribute metadata = null;
                for (NamedExpression field : relation.metadataFields()) {
                    if (field instanceof MetadataAttribute m && m.name().equals(name)) metadata = m;
                }
                if (metadata == null) {
                    MetadataAttribute original = (MetadataAttribute) MetadataAttribute.create(Source.EMPTY, name);
                    metadata = new MetadataAttribute(
                        Source.EMPTY,
                        name,
                        original.dataType(),
                        original.nullable(),
                        original.id(),
                        true,
                        original.searchable()
                    );
                    relation = relation.addMetadataField(metadata);
                }
                Role role = new Role(i, name, new NameId(), new NameId());
                roles.add(role);
                originals.add(
                    new Alias(Source.EMPTY, temporaryName(inputNames, "sequence_original", role.input()), metadata, role.input(), true)
                );
            }
            UnresolvedRelation replacement = relation;
            LogicalPlan retained = stage.child().transformUp(p -> {
                if (p instanceof UnresolvedRelation) return new Originals(p.source(), replacement, originals, reservedNames);
                if (p instanceof Keep keep) {
                    List<NamedExpression> projections = new ArrayList<>(keep.projections());
                    // The source may rename this binding after mapped fields resolve; KEEP follows its ID, not that old name.
                    originals.forEach(a -> projections.add(new BindingReference(a.source(), a.name(), a.id())));
                    return new Keep(keep.source(), keep.child(), projections);
                }
                return p;
            });
            stages.add(stage.replaceChild(retained));
        }
        List<NameId> keys = new ArrayList<>();
        if (capture) for (Attribute ignored : ((SequenceStage) sequence.children().getFirst()).keys())
            keys.add(new NameId());
        Map<NameId, NameId> published = new LinkedHashMap<>();
        roles.forEach(role -> published.put(role.output(), new NameId()));
        keys.forEach(key -> published.put(key, new NameId()));
        List<String> keyNames = ((SequenceStage) sequence.children().getFirst()).keys().stream().map(Attribute::name).toList();
        Context context = new Context(
            primary,
            Map.copyOf(members),
            List.copyOf(roles),
            List.copyOf(keys),
            Map.copyOf(published),
            keyNames,
            reservedNames
        );
        LogicalPlan plan = statement.plan().transformUp(Sequence.class, s -> new Scope(s.source(), s.replaceChildren(stages), context));
        if (capture) plan = new Capture(Source.EMPTY, plan, context);
        return new EsqlStatement(plan, statement.settings(), statement.letBindings());
    }

    private static void requireSimpleStage(LogicalPlan plan) {
        if (plan instanceof UnresolvedRelation) return;
        if (plan instanceof Eval
            || plan instanceof Keep
            || plan instanceof Drop
            || plan instanceof Rename
            || plan instanceof Filter
            || plan instanceof UnresolvedMetadata) {
            requireSimpleStage(((UnaryPlan) plan).child());
            return;
        }
        throw new ParsingException(plan.source(), "outside sequence output stage boundary: {}", plan.nodeName());
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

    /** Capture genuine source slots by projection; SOURCE is not an EVAL result type. */
    public static final class Originals extends UnaryPlan {
        private final List<Alias> originals;
        private final Set<String> reservedNames;
        private final boolean keepUnmapped;

        private Originals(Source source, LogicalPlan child, List<Alias> originals, Set<String> reservedNames) {
            this(source, child, originals, reservedNames, false);
        }

        private Originals(Source source, LogicalPlan child, List<Alias> originals, Set<String> reservedNames, boolean keepUnmapped) {
            super(source, child);
            this.originals = List.copyOf(originals);
            this.reservedNames = reservedNames;
            this.keepUnmapped = keepUnmapped;
        }

        @Override
        protected NodeInfo<Originals> info() {
            return NodeInfo.create(this, Originals::new, child(), originals, reservedNames, keepUnmapped);
        }

        @Override
        public Originals replaceChild(LogicalPlan child) {
            return new Originals(source(), child, originals, reservedNames, keepUnmapped);
        }

        @Override
        public boolean equals(Object other) {
            return super.equals(other) && keepUnmapped == ((Originals) other).keepUnmapped;
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

        /** Keep compiler projections responsive to NULLIFY until the analysis fixpoint finishes. */
        public LogicalPlan resolve(boolean keepUnmapped) {
            return keepUnmapped ? new Originals(source(), child(), originals, reservedNames, true) : lower();
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

        public LogicalPlan lower() {
            return new Project(source(), child(), projections());
        }

        private List<NamedExpression> projections() {
            List<NamedExpression> output = new ArrayList<>(child().output());
            Set<String> usedNames = new HashSet<>(reservedNames);
            output.forEach(field -> usedNames.add(field.name()));
            for (Alias original : originals) {
                output.add(
                    new Alias(
                        original.source(),
                        temporaryName(usedNames, "sequence_original", original.id()),
                        original.child(),
                        original.id(),
                        true
                    )
                );
            }
            return output;
        }
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
                if (input.synthetic() == false && (i < keyCount || stage == context.primary())) {
                    // Recomputing a compiler projection must not change an already published binding.
                    output.add(
                        keepUnmapped
                            ? new Alias(value.source(), input.name(), value, value.id(), false)
                            : new Alias(value.source(), input.name(), value)
                    );
                }
                for (var entry : context.members().entrySet()) {
                    Member member = entry.getKey();
                    // Matching keys are shared by every member, although Sequence emits them only once.
                    if ((i < keyCount || stage == member.stage()) && input.synthetic() == false && input.name().equals(member.field())) {
                        NameId binding = entry.getValue();
                        // Reserve against parsed and resolved user names; MEMBER resolution uses only the binding ID.
                        output.add(new Alias(value.source(), temporaryName(usedNames, "sequence_member", binding), value, binding, true));
                    }
                }
                for (Role role : context.roles()) {
                    if (stage == role.stage() && input.id().equals(role.input())) {
                        output.add(
                            new Alias(
                                value.source(),
                                temporaryName(usedNames, "sequence_original", role.output()),
                                value,
                                role.output(),
                                true
                            )
                        );
                    }
                }
                if (i < context.keys().size()) {
                    NameId key = context.keys().get(i);
                    output.add(new Alias(value.source(), temporaryName(usedNames, "sequence_key", key), value, key, true));
                }
            }
            return output;
        }
    }

    /** Explicit complete-match dependencies published as ordinary typed output columns. */
    public static final class Capture extends UnaryPlan {
        private final Context context;

        private Capture(Source source, LogicalPlan child, Context context) {
            super(source, child);
            this.context = context;
        }

        @Override
        protected NodeInfo<Capture> info() {
            return NodeInfo.create(this, Capture::new, child(), context);
        }

        @Override
        public Capture replaceChild(LogicalPlan child) {
            return new Capture(source(), child, context);
        }

        @Override
        public boolean expressionsResolved() {
            return false;
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

        /** Adds real passthrough dependencies only after ordinary projection references have resolved. */
        public LogicalPlan lower() {
            Set<NameId> ids = new HashSet<>(context.keys());
            context.roles().forEach(role -> ids.add(role.output()));
            LogicalPlan retained = retain(child(), ids);
            List<NamedExpression> output = new ArrayList<>();
            Set<NameId> found = new HashSet<>();
            Map<NameId, Attribute> protectedFields = new LinkedHashMap<>();
            for (Attribute attribute : retained.output()) {
                if (ids.contains(attribute.id())) {
                    found.add(attribute.id());
                    protectedFields.put(attribute.id(), attribute);
                } else if (attribute.synthetic() == false) {
                    output.add(attribute);
                }
            }
            Map<NameId, String> names = new LinkedHashMap<>();
            for (int i = 0; i < context.keys().size(); i++) {
                names.put(context.keys().get(i), "match_key_" + (i + 1));
            }
            context.roles().forEach(role -> names.put(role.output(), role.diagnosticName()));
            Set<String> usedNames = new HashSet<>();
            output.forEach(field -> usedNames.add(field.name()));
            for (var entry : names.entrySet()) {
                NameId id = entry.getKey();
                Attribute attribute = protectedFields.get(id);
                if (attribute == null) throw new IllegalStateException("missing authentic sequence role");
                // Assign display names only after user commands finish, preserving ordinary names on collision.
                String name = entry.getValue();
                int suffix = 1;
                while (usedNames.add(name) == false) {
                    name = entry.getValue() + "_" + suffix++;
                }
                output.add(new Alias(attribute.source(), name, attribute, context.published().get(id), false));
            }
            if (found.equals(ids) == false) throw new IllegalStateException("missing authentic sequence role");
            return new Project(source(), retained, output);
        }

        private static LogicalPlan retain(LogicalPlan plan, Set<NameId> ids) {
            if (plan instanceof Sequence || plan instanceof Scope) return plan;
            if ((plan instanceof Project
                || plan instanceof Eval
                || plan instanceof Filter
                || plan instanceof OrderBy
                || plan instanceof Limit) == false) {
                throw new IllegalArgumentException("outside sequence output boundary: " + plan.nodeName());
            }
            UnaryPlan unary = (UnaryPlan) plan;
            LogicalPlan child = retain(unary.child(), ids);
            UnaryPlan result = unary.replaceChild(child);
            if (result instanceof Project project) {
                List<NamedExpression> projections = new ArrayList<>(project.projections());
                for (Attribute attribute : child.output()) {
                    if (ids.contains(attribute.id()) && project.outputSet().contains(attribute) == false) projections.add(attribute);
                }
                result = project.withProjections(projections);
            }
            return result;
        }
    }

    /**
     * Verify the actual final schema against compiler-created original bindings, then attach a response-local description.
     * No role is inferred from a display name or from a response position supplied by the consumer.
     */
    public static Map<NameId, Map<String, Object>> deliveryMetadata(
        EsqlStatement statement,
        List<Attribute> schema,
        Map<NameId, Map<String, Object>> existing
    ) {
        if ((statement.plan() instanceof Capture) == false) return existing;
        Context context = ((Capture) statement.plan()).context;
        Map<NameId, Attribute> fields = new LinkedHashMap<>();
        Map<NameId, String> responseIds = new LinkedHashMap<>();
        for (int i = 0; i < schema.size(); i++) {
            Attribute field = schema.get(i);
            if (fields.put(field.id(), field) != null) throw new IllegalStateException("duplicate final attribute binding");
            responseIds.put(field.id(), "c" + i);
        }
        if (fields.keySet().containsAll(context.published().values()) == false) {
            throw new IllegalStateException("missing final original binding");
        }
        List<Map<String, Object>> keys = new ArrayList<>();
        for (int i = 0; i < context.keys().size(); i++) {
            NameId id = context.published().get(context.keys().get(i));
            keys.add(
                Map.of("name", context.keyNames().get(i), "type", fields.get(id).dataType().outputType(), "column", responseIds.get(id))
            );
        }
        int memberCount = context.roles().size() / 3;
        List<Map<String, Object>> members = new ArrayList<>();
        for (int stage = 0; stage < memberCount; stage++) {
            Map<String, Object> member = new LinkedHashMap<>();
            member.put("stage", stage + 1);
            for (Role role : context.roles()) {
                if (role.stage() != stage) continue;
                NameId id = context.published().get(role.output());
                DataType expected = role.metadata().equals("_source") ? DataType.SOURCE : DataType.KEYWORD;
                if (fields.get(id).dataType() != expected) throw new IllegalStateException("wrong final original type");
                member.put(role.metadata().substring(1), responseIds.get(id));
            }
            if (member.size() != 4) throw new IllegalStateException("incomplete member binding");
            members.add(Map.copyOf(member));
        }
        Map<String, Object> contract = Map.of(
            "version",
            1,
            "layout",
            "columns",
            "primary_stage",
            context.primary() + 1,
            "member_count",
            memberCount,
            "keys",
            keys,
            "members",
            members
        );
        NameId anchor = context.published().get(context.roles().getFirst().output());
        Map<NameId, Map<String, Object>> result = new LinkedHashMap<>(existing);
        for (Attribute field : schema) {
            Map<String, Object> role = new LinkedHashMap<>();
            role.put("column_id", responseIds.get(field.id()));
            if (field.id().equals(anchor)) role.put("contract", contract);
            Map<String, Object> metadata = new LinkedHashMap<>(result.getOrDefault(field.id(), Map.of()));
            if (metadata.put("sequence", Map.copyOf(role)) != null) throw new IllegalStateException("duplicate delivery metadata");
            result.put(field.id(), Map.copyOf(metadata));
        }
        return Map.copyOf(result);
    }
}
