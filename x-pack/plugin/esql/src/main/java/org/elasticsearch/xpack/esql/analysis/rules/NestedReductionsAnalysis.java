/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.Nested;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedReduction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.elasticsearch.xpack.esql.plan.logical.NestedReduce;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Binds reductions before EVAL's aggregate verification; public outputs contain only completed parent values. */
public final class NestedReductionsAnalysis {
    private static final List<String> METADATA = List.of("_source", "_index");
    /** Types the source loader can convert. */
    private static final Set<DataType> SUPPORTED_CHILD_TYPES = Set.of(
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.KEYWORD,
        DataType.TEXT
    );

    private NestedReductionsAnalysis() {}

    private static String privateMetadataName(String name) {
        return Attribute.rawTemporaryName(NestedScopeField.NAMESPACE, "metadata", name);
    }

    private static boolean isPrivateMetadataName(String name) {
        return METADATA.stream().anyMatch(role -> privateMetadataName(role).equals(name));
    }

    /** A lookup attaches values to the FROM row; it does not replace that row's nested source. */
    private static List<EsRelation> primarySources(LogicalPlan input) {
        if (input instanceof EsRelation relation) {
            return List.of(relation);
        }
        if (input instanceof Join join) {
            return primarySources(join.left());
        }
        List<EsRelation> sources = new ArrayList<>();
        for (LogicalPlan child : input.children()) {
            sources.addAll(primarySources(child));
        }
        return sources;
    }

    private static boolean usesNested(LogicalPlan plan) {
        return plan.anyMatch(
            p -> p instanceof NestedExpand || p.expressions().stream().anyMatch(e -> e.anyMatch(Nested.class::isInstance))
        );
    }

    /** A NESTED_EXPAND turns its collection into ordinary columns, so later wrappers cannot treat it as a scope. */
    private static Set<String> expandedPaths(LogicalPlan input) {
        Set<String> paths = new HashSet<>();
        input.forEachDown(NestedExpand.class, expand -> paths.add(expand.fieldName()));
        return paths;
    }

    private record ScopeSchema(Map<String, FieldAttribute> fields, Map<String, NestedScopeField> roots) {}

    /** Resolve each wrapper against its own input, so later joins and sibling inputs cannot overwrite its schema. */
    private static ScopeSchema scopeSchema(LogicalPlan input, Nested nested) {
        Map<String, FieldAttribute> fields = new HashMap<>();
        Map<String, NestedScopeField> roots = new HashMap<>();
        Set<String> expanded = expandedPaths(input);
        for (EsRelation relation : primarySources(input)) {
            for (Attribute attribute : relation.output()) {
                if (attribute instanceof FieldAttribute field
                    && field.field() instanceof NestedScopeField scope
                    && expanded.stream().noneMatch(path -> inPath(scope.path(), path))) {
                    FieldAttribute merged = fields.merge(field.name(), field, (left, right) -> mergeBinding(left, right, nested));
                    if (scope.collection()) {
                        roots.put(scope.path(), (NestedScopeField) merged.field());
                    }
                }
            }
        }
        return new ScopeSchema(fields, roots);
    }

    private static FieldAttribute mergeBinding(FieldAttribute left, FieldAttribute right, Nested nested) {
        NestedScopeField first = (NestedScopeField) left.field();
        NestedScopeField second = (NestedScopeField) right.field();
        if (first.path().equals(second.path()) == false || first.collection() != second.collection()) {
            throw error(nested, "NESTED cannot merge different nested paths for [" + left.name() + "] across branches");
        }
        Set<String> indices = new HashSet<>(first.indices());
        indices.addAll(second.indices());
        Map<String, String> sourceFailures = new HashMap<>(first.sourceFailures());
        sourceFailures.putAll(second.sourceFailures());
        DataType type = first.getDataType() == second.getDataType() ? first.getDataType() : DataType.UNSUPPORTED;
        // These bindings are already flattened: each child has its own entry and per-index applicability.
        // Retaining one branch's property tree would misrepresent children supplied only by another branch.
        return left.withField(
            new NestedScopeField(first.getName(), type, Map.of(), first.path(), indices, first.collection(), sourceFailures)
        );
    }

    /** Reserve names once, before union resolution can rebuild internal attributes without their synthetic flag. */
    public static final class ValidateNames extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            if (usesNested(plan)) {
                plan.forEachExpressionDown(NamedExpression.class, expression -> {
                    if (expression.synthetic() == false
                        && (isPrivateMetadataName(expression.name())
                            || expression.name().startsWith(NestedScopeField.PREFIX)
                            || expression.name().equals(NestedScopeField.NAMESPACE))) {
                        throw error(expression, "NESTED uses reserved internal metadata and schema names");
                    }
                });
            }
            return plan;
        }
    }

    public static final class Prepare extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            Set<EsRelation> requiredSources = new HashSet<>();
            plan.forEachDown(p -> {
                if (p.children().isEmpty() == false
                    && (p instanceof NestedExpand || p.expressions().stream().anyMatch(e -> e.anyMatch(Nested.class::isInstance)))) {
                    requiredSources.addAll(primarySources(p.children().getFirst()));
                }
            });
            if (requiredSources.isEmpty()) {
                return plan;
            }
            Set<NameId> captured = new HashSet<>();
            plan.forEachExpressionDown(NamedExpression.class, expression -> {
                if (isPrivateMetadataName(expression.name())) {
                    if (expression.synthetic() == false) {
                        throw error(expression, "NESTED uses reserved internal metadata names");
                    }
                    if (expression instanceof Alias alias && alias.child() instanceof MetadataAttribute metadata) {
                        captured.add(metadata.id());
                    }
                }
            });
            LogicalPlan prepared = plan.transformUp(EsRelation.class, relation -> {
                if (requiredSources.contains(relation) == false) {
                    return relation;
                }
                List<Attribute> output = new ArrayList<>(relation.output());
                for (Attribute attribute : output) {
                    if (attribute.synthetic() == false && isPrivateMetadataName(attribute.name())) {
                        throw error(attribute, "NESTED uses reserved internal metadata names");
                    }
                }
                List<Alias> aliases = new ArrayList<>();
                for (String name : METADATA) {
                    Attribute metadata = output.stream()
                        .filter(a -> a instanceof MetadataAttribute && a.name().equals(name))
                        .findFirst()
                        .orElse(null);
                    if (metadata == null) {
                        metadata = new MetadataAttribute(
                            relation.source(),
                            name,
                            name.equals("_source") ? DataType.SOURCE : DataType.KEYWORD,
                            Nullability.TRUE,
                            null,
                            true,
                            false
                        );
                        output.add(metadata);
                    }
                    if (captured.contains(metadata.id()) == false) {
                        aliases.add(new Alias(relation.source(), privateMetadataName(name), metadata, null, true));
                    }
                }
                EsRelation scan = output.equals(relation.output()) ? relation : relation.withAttributes(output);
                if (aliases.isEmpty()) {
                    return scan;
                }
                // Capture identity at the scan, before public EVAL/RENAME/DROP can replace the same metadata names.
                // A projection aliases existing blocks without computing a public SOURCE-valued expression.
                List<NamedExpression> projections = new ArrayList<>(output);
                projections.addAll(aliases);
                return new Project(relation.source(), scan, projections);
            });
            return prepared.transformUp(p -> {
                if (p.children().isEmpty()) {
                    return p;
                }
                return p.transformExpressionsOnlyUp(Nested.class, nested -> {
                    LogicalPlan input = p.children().getFirst();
                    ScopeSchema schema = scopeSchema(input, nested);
                    Set<String> allPaths = new HashSet<>();
                    Set<String> expanded = expandedPaths(input);
                    input.forEachDown(EsRelation.class, relation -> relation.output().forEach(a -> {
                        if (a instanceof FieldAttribute field
                            && field.field() instanceof NestedScopeField scope
                            && scope.collection()
                            && expanded.stream().noneMatch(path -> inPath(scope.path(), path))) {
                            allPaths.add(scope.path());
                        }
                    }));
                    Expression child = nested.field().transformUp(UnresolvedAttribute.class, attribute -> {
                        String name = attribute.name();
                        String rawName = name.startsWith(NestedScopeField.PREFIX) ? name.substring(NestedScopeField.PREFIX.length()) : name;
                        boolean primary = schema.roots().keySet().stream().anyMatch(path -> inPath(rawName, path));
                        if (primary == false && allPaths.stream().anyMatch(path -> inPath(rawName, path))) {
                            throw error(attribute, "NESTED lookup-side nested scopes are not supported by this PoC");
                        }
                        if (name.startsWith(NestedScopeField.PREFIX) == false && primary) {
                            name = NestedScopeField.PREFIX + name;
                        }
                        FieldAttribute field = schema.fields().get(name);
                        if (field != null) {
                            return field.withLocation(attribute.source());
                        }
                        return name.equals(attribute.name())
                            ? attribute
                            : new UnresolvedAttribute(attribute.source(), name, attribute.id(), null);
                    });
                    return child == nested.field() ? nested : new Nested(nested.source(), child);
                });
            });
        }
    }

    public static final class Resolve extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            // A query that never calls NESTED has nothing to bind. Carrying every synthetic attribute through its
            // projections also pulls the empty-output marker into KEEP and DROP, so a later project still names it
            // after the scan has stopped producing it.
            if (usesNested(plan) == false) {
                return plan;
            }
            return plan.transformUp(p -> {
                if (p instanceof NestedExpand expand && expand.bound() == false) {
                    return expand.child().resolved() ? bindExpand(expand, context) : expand;
                }
                if (p.children().isEmpty() || p.childrenResolved() == false || p.expressionsResolved() == false) {
                    return p;
                }
                if (p instanceof Aggregate && p.expressions().stream().anyMatch(e -> e.anyMatch(Nested.class::isInstance))) {
                    throw error(p.expressions().getFirst(), "NESTED reductions must be completed in EVAL before outer STATS");
                }
                p.forEachExpression(Nested.class, nested -> {
                    if (nested.field().anyMatch(Nested.class::isInstance)) {
                        throw error(nested, "NESTED wrappers cannot be nested");
                    }
                });
                List<Alias> reductions = new ArrayList<>();
                LogicalPlan lowered = p.transformExpressionsOnlyUp(Nested.class, nested -> {
                    if (p instanceof Eval == false && p instanceof Filter == false) {
                        throw error(nested, "NESTED is supported in EVAL and WHERE");
                    }
                    if (context.minimumVersion().supports(NestedReduction.VERSION) == false) {
                        throw error(nested, "NESTED requires all nodes to support nested reductions");
                    }
                    var reduction = bind(nested, p.children().getFirst(), context);
                    Alias result = new Alias(
                        nested.source(),
                        Attribute.rawTemporaryName("nested_result", nested.hashCode() + ""),
                        reduction,
                        null,
                        true
                    );
                    reductions.add(result);
                    return result.toAttribute();
                });
                if (reductions.isEmpty() == false) {
                    lowered = lowered.replaceChildren(List.of(new NestedReduce(p.source(), lowered.children().getFirst(), reductions)));
                }
                if (lowered instanceof Project project) {
                    List<NamedExpression> projected = new ArrayList<>(project.projections());
                    for (Attribute a : project.child().output()) {
                        if (carriedThroughProject(a) && project.outputSet().contains(a) == false) {
                            projected.add(a);
                        }
                    }
                    if (projected.size() != project.projections().size()) {
                        lowered = project.withProjections(projected);
                    }
                }
                return lowered;
            });
        }
    }

    private static boolean inPath(String name, String path) {
        return name.equals(path) || name.startsWith(path + ".");
    }

    private static NestedReduction bind(Nested nested, LogicalPlan input, AnalyzerContext context) {
        Map<String, NestedScopeField> roots = scopeSchema(input, nested).roots();
        Expression body = nested.field();
        if (body.anyMatch(e -> e instanceof Nested || e instanceof FullTextFunction)) {
            throw error(nested, "NESTED does not allow inner scopes or full-text functions");
        }
        if (body instanceof Sum == false && body instanceof Last == false) {
            if (body.anyMatch(AggregateFunction.class::isInstance)) {
                throw error(nested, "NESTED supports SUM and LAST only, not [" + body.sourceText() + "]");
            }
            if (body.dataType() == DataType.SOURCE || body.dataType() == DataType.OBJECT) {
                throw error(nested, "NESTED requires a scalar reduction, not an object collection");
            }
            if (body.dataType() != DataType.BOOLEAN) {
                throw error(nested, "NESTED requires an explicit reduction for per-item values");
            }
        }
        if (body instanceof AggregateFunction aggregate) {
            if (aggregate.filter().equals(Literal.TRUE) == false || aggregate.window().equals(AggregateFunction.NO_WINDOW) == false) {
                throw error(nested, "NESTED does not support inner aggregate filters or windows");
            }
            for (Expression field : aggregate.fields()) {
                if (field.anyMatch(AggregateFunction.class::isInstance)) {
                    throw error(nested, "NESTED does not support nested aggregates");
                }
            }
        }
        Map<NameId, Attribute> fields = new LinkedHashMap<>();
        body.forEachDown(Attribute.class, a -> fields.putIfAbsent(a.id(), a));
        String path = null;
        Set<String> indices = new HashSet<>();
        Map<String, String> sourceFailures = new HashMap<>();
        for (Attribute attr : fields.values()) {
            if (attr instanceof FieldAttribute f && f.field() instanceof NestedScopeField n) {
                if (path != null && path.equals(n.path()) == false) {
                    throw error(nested, "NESTED requires one nested path");
                }
                path = n.path();
                indices.addAll(n.indices());
                sourceFailures.putAll(n.sourceFailures());
            }
        }
        if (path == null) {
            throw error(nested, "NESTED requires a mapped nested field");
        }
        String scope = path;
        if (roots.containsKey(path)) {
            indices = new HashSet<>(roots.get(path).indices());
            sourceFailures.putAll(roots.get(path).sourceFailures());
        }
        if (roots.keySet().stream().anyMatch(parent -> scope.startsWith(parent + "."))) {
            throw error(nested, "NESTED supports one nesting level");
        }
        if (indices.stream().anyMatch(index -> index.contains(":"))) {
            throw error(nested, "NESTED cross-cluster loading is not supported by this PoC");
        }
        List<Attribute> channels = new ArrayList<>();
        List<String> paths = new ArrayList<>();
        List<Set<String>> mapped = new ArrayList<>();
        Map<NameId, Attribute> replacement = new HashMap<>();
        for (Attribute attr : fields.values()) {
            if (attr instanceof FieldAttribute == false || ((FieldAttribute) attr).field() instanceof NestedScopeField == false) {
                throw error(nested, "Field [" + attr.name() + "] is outside the nested scope");
            }
            if (SUPPORTED_CHILD_TYPES.contains(attr.dataType()) == false) {
                throw error(nested, "NESTED does not support child type [" + attr.dataType() + "] for [" + attr.name() + "]");
            }
            ReferenceAttribute ref = new ReferenceAttribute(attr.source(), attr.name(), attr.dataType());
            channels.add(ref);
            replacement.put(attr.id(), ref);
            paths.add(attr.name().substring(NestedScopeField.PREFIX.length() + path.length() + 1));
            mapped.add(((NestedScopeField) ((FieldAttribute) attr).field()).indices());
        }
        Expression bound = body.transformUp(Attribute.class, a -> replacement.get(a.id()));
        if (bound instanceof Sum sum) {
            bound = sum.forTransportVersion(context.minimumVersion());
        }
        List<Expression> metadata = new ArrayList<>();
        for (String name : METADATA) {
            Attribute found = input.output()
                .stream()
                .filter(a -> a.synthetic() && a.name().equals(privateMetadataName(name)))
                .findFirst()
                .orElse(null);
            if (found == null) {
                throw error(nested, "NESTED input is missing required source metadata [" + name + "]");
            }
            metadata.add(found);
        }
        return new NestedReduction(nested.source(), metadata, path, indices, bound, channels, paths, mapped, sourceFailures);
    }

    /**
     * Binds NESTED_EXPAND to a mapped nested collection in its input. An unknown name stays unbound, so the verifier
     * reports it like any other unknown column.
     */
    private static LogicalPlan bindExpand(NestedExpand expand, AnalyzerContext context) {
        NamedExpression target = expand.target();
        String name = target.name();
        List<Attribute> input = expand.child().output();
        FieldAttribute root = null;
        Set<String> collections = new HashSet<>();
        for (Attribute attribute : input) {
            if (attribute instanceof FieldAttribute field && field.field() instanceof NestedScopeField scope && scope.collection()) {
                collections.add(scope.path());
                if (field.name().equals(NestedScopeField.PREFIX + name)) {
                    root = field;
                }
            }
        }
        if (root == null) {
            if (input.stream().anyMatch(a -> inPath(a.name(), name) || inPath(a.name(), NestedScopeField.PREFIX + name))) {
                throw error(target, "NESTED_EXPAND requires a mapped nested field, not [" + name + "]");
            }
            return expand;
        }
        if (context.minimumVersion().supports(NestedExpand.VERSION) == false) {
            throw error(target, "NESTED_EXPAND requires all nodes to support nested expansion");
        }
        NestedScopeField scope = (NestedScopeField) root.field();
        String path = scope.path();
        if (collections.stream().anyMatch(parent -> path.startsWith(parent + "."))) {
            throw error(target, "NESTED_EXPAND supports one nesting level");
        }
        if (scope.indices().stream().anyMatch(index -> index.contains(":"))) {
            throw error(target, "NESTED_EXPAND cross-cluster loading is not supported by this PoC");
        }
        List<FieldAttribute> children = new ArrayList<>();
        for (Attribute attribute : input) {
            if (attribute instanceof FieldAttribute field
                && field.field() instanceof NestedScopeField child
                && child.collection() == false
                && child.path().equals(path)
                && child.getDataType() != DataType.OBJECT) {
                if (SUPPORTED_CHILD_TYPES.contains(child.getDataType()) == false) {
                    throw error(
                        target,
                        "NESTED_EXPAND does not support child type [" + child.getDataType() + "] for [" + publicName(field) + "]"
                    );
                }
                children.add(field);
            }
        }
        children.sort(Comparator.comparing(NestedReductionsAnalysis::publicName));
        List<Attribute> expanded = new ArrayList<>();
        List<String> paths = new ArrayList<>();
        List<Set<String>> mapped = new ArrayList<>();
        for (FieldAttribute child : children) {
            String column = publicName(child);
            expanded.add(new ReferenceAttribute(expand.source(), column, child.dataType()));
            paths.add(column.substring(path.length() + 1));
            mapped.add(((NestedScopeField) child.field()).indices());
        }
        List<Attribute> metadata = new ArrayList<>();
        for (String role : METADATA) {
            Attribute found = input.stream()
                .filter(a -> a.synthetic() && a.name().equals(privateMetadataName(role)))
                .findFirst()
                .orElse(null);
            if (found == null) {
                throw error(target, "NESTED_EXPAND input is missing required source metadata [" + role + "]");
            }
            metadata.add(found);
        }
        return new NestedExpand(
            expand.source(),
            expand.child(),
            null,
            metadata,
            path,
            scope.indices(),
            expanded,
            paths,
            mapped,
            scope.sourceFailures()
        );
    }

    private static String publicName(FieldAttribute field) {
        return field.name().substring(NestedScopeField.PREFIX.length());
    }

    public static final class Cleanup extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            if (plan.anyMatch(
                p -> p instanceof NestedExpand || p.expressions().stream().anyMatch(e -> e.anyMatch(NestedReduction.class::isInstance))
            ) == false) {
                return plan;
            }
            return plan.transformUp(p -> {
                if (p instanceof EsRelation relation) {
                    return relation.withAttributes(relation.output().stream().filter(a -> isSchema(a) == false).toList());
                }
                if (p instanceof MergePlan merge) {
                    return merge.replaceSubPlansAndOutput(
                        merge.children(),
                        merge.output().stream().filter(a -> isSchema(a) == false).toList()
                    );
                }
                if (p instanceof Project project) {
                    var projections = project.projections().stream().filter(a -> isSchema(a) == false).toList();
                    return projections.size() == project.projections().size() ? project : project.withProjections(projections);
                }
                if (p instanceof Eval eval) {
                    var fields = eval.fields().stream().filter(a -> isSchema(a) == false).toList();
                    if (fields.size() != eval.fields().size()) {
                        if (fields.isEmpty()) {
                            return eval.child();
                        }
                        return eval instanceof NestedReduce
                            ? new NestedReduce(eval.source(), eval.child(), fields)
                            : new Eval(eval.source(), eval.child(), fields);
                    }
                }
                return p;
            });
        }
    }

    /** Nested schema and the captured source aliases. The empty-output marker is synthetic and must stay where it is. */
    private static boolean carriedThroughProject(Attribute attribute) {
        return isSchema(attribute) || (attribute.synthetic() && isPrivateMetadataName(attribute.name()));
    }

    private static boolean isSchema(Expression a) {
        return a instanceof FieldAttribute f && f.field() instanceof NestedScopeField
            || a instanceof NamedExpression n && n.name().startsWith(NestedScopeField.PREFIX);
    }

    private static VerificationException error(Expression expression, String message) {
        return new VerificationException(List.of(Failure.fail(expression, message)));
    }
}
