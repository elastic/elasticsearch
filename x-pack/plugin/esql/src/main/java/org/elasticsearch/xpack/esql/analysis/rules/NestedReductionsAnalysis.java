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
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Last;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.Nested;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedAll;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedJson;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedReduction;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedStats;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.index.NestedField;
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

/**
 * Binds the nested functions and {@code NESTED_EXPAND} before EVAL's aggregate verification; public outputs contain only
 * completed parent values. A field under a nested object is an ordinary column, read flattened; these are the only places
 * where it means one value per object. The mapping tells which columns those are.
 */
public final class NestedReductionsAnalysis {
    private static final List<String> METADATA = List.of("_source", "_index");
    private static final String METADATA_PREFIX = "$$nested_reductions";
    /** Types NESTED_JSON can write and the source loader can convert. */
    private static final Set<DataType> JSON_CHILD_TYPES = Set.of(
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.KEYWORD,
        DataType.TEXT
    );
    /** Types the reductions and NESTED_EXPAND read; the source loader converts only {@link #JSON_CHILD_TYPES}. */
    private static final Set<DataType> SUPPORTED_CHILD_TYPES = Set.of(
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.DOUBLE,
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.DATETIME,
        DataType.IP
    );

    private NestedReductionsAnalysis() {}

    private static String privateMetadataName(String name) {
        return Attribute.rawTemporaryName(METADATA_PREFIX, "metadata", name);
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

    /** How the indices of the primary sources of {@code input} map the nested objects and the fields under them. */
    private static Map<String, NestedField> nestedFields(LogicalPlan input, AnalyzerContext context) {
        Map<String, EsIndex> indices = new HashMap<>();
        for (IndexResolution resolution : context.indexResolution().values()) {
            if (resolution != null && resolution.isValid()) {
                indices.put(resolution.get().name(), resolution.get());
            }
        }
        Set<String> expanded = expandedPaths(input);
        Map<String, NestedField> fields = new HashMap<>();
        // The nested functions read each branch's values by the type of the merged column, which the union rules only
        // settle after these functions are bound.
        Map<String, DataType> types = new HashMap<>();
        for (EsRelation relation : primarySources(input)) {
            EsIndex index = indices.get(relation.indexPattern());
            if (index == null) {
                continue;
            }
            Map<String, DataType> relationTypes = new HashMap<>();
            for (Attribute attribute : relation.output()) {
                if (attribute instanceof FieldAttribute field) {
                    relationTypes.put(field.fieldName().string(), field.dataType());
                }
            }
            index.nestedFields().forEach((name, field) -> {
                if (expanded.stream().noneMatch(path -> inPath(field.path(), path))) {
                    NestedField merged = fields.merge(name, field, NestedReductionsAnalysis::merge);
                    DataType type = relationTypes.get(name);
                    DataType previous = type == null ? null : types.putIfAbsent(name, type);
                    if (previous != null && previous != type && merged.conflict() == null) {
                        String conflict = "its type is ["
                            + previous.typeName()
                            + "] in one branch but ["
                            + type.typeName()
                            + "] in another";
                        fields.put(
                            name,
                            new NestedField(merged.path(), merged.indices(), merged.collection(), merged.sourceFailures(), conflict)
                        );
                    }
                }
            });
        }
        return fields;
    }

    /** The same field read by several branches, such as the subqueries of a FROM. */
    private static NestedField merge(NestedField first, NestedField second) {
        Set<String> indices = new HashSet<>(first.indices());
        indices.addAll(second.indices());
        Map<String, String> sourceFailures = new HashMap<>(first.sourceFailures());
        sourceFailures.putAll(second.sourceFailures());
        String conflict = first.conflict() != null ? first.conflict() : second.conflict();
        if (conflict == null && (first.path().equals(second.path()) == false || first.collection() != second.collection())) {
            conflict = "its nested path is [" + first.path() + "] in one branch but [" + second.path() + "] in another";
        }
        return new NestedField(first.path(), indices, first.collection(), sourceFailures, conflict);
    }

    /**
     * The field each column of {@code input} reads, by column id, for the columns that read a field of a primary source:
     * those of the relations themselves, and those a union or FORK merges from its branches.
     */
    private static Map<NameId, String> fieldNames(LogicalPlan input) {
        Map<NameId, String> names = new HashMap<>();
        for (EsRelation relation : primarySources(input)) {
            for (Attribute attribute : relation.output()) {
                if (attribute instanceof FieldAttribute field) {
                    names.put(field.id(), field.fieldName().string());
                }
            }
        }
        input.forEachDown(MergePlan.class, merge -> merge.output().forEach(a -> names.putIfAbsent(a.id(), a.name())));
        return names;
    }

    /** What the nested functions see of {@code input}: the mapping of its primary sources, and which columns read it. */
    private record Scope(Map<String, NestedField> fields, Map<NameId, String> names) {
        static Scope of(LogicalPlan input, AnalyzerContext context) {
            return new Scope(nestedFields(input, context), fieldNames(input));
        }

        /** The field under a nested object that {@code expression} reads, or {@code null} if it is not such a column. */
        String child(Expression expression) {
            if (expression instanceof Attribute attribute) {
                String name = names.get(attribute.id());
                NestedField field = name == null ? null : fields.get(name);
                if (field != null && field.collection() == false) {
                    return name;
                }
            }
            return null;
        }

        /** The nested object {@code expression} names, as {@link Prepare} resolves it, or {@code null}. */
        NestedField collection(Expression expression) {
            if (expression instanceof FieldAttribute attribute && attribute.synthetic()) {
                NestedField field = fields.get(attribute.name());
                if (field != null && field.collection()) {
                    return field;
                }
            }
            return null;
        }

        /** The nested objects, by path. */
        Map<String, NestedField> roots() {
            Map<String, NestedField> roots = new HashMap<>();
            fields.forEach((name, field) -> {
                if (field.collection()) {
                    roots.put(field.path(), field);
                }
            });
            return roots;
        }
    }

    /**
     * Adds the private source metadata the nested functions read to their sources, and resolves the nested objects they
     * name, which are not columns.
     */
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
                    if (nested.children().getFirst() instanceof UnresolvedAttribute == false) {
                        return nested;
                    }
                    UnresolvedAttribute named = (UnresolvedAttribute) nested.children().getFirst();
                    NestedField field = nestedFields(p.children().getFirst(), context).get(named.name());
                    if (field == null || field.collection() == false) {
                        return nested;
                    }
                    EsField object = new EsField(named.name(), DataType.OBJECT, Map.of(), false, EsField.TimeSeriesFieldType.NONE);
                    List<Expression> children = new ArrayList<>(nested.children());
                    children.set(0, new FieldAttribute(named.source(), null, null, named.name(), object, true));
                    return nested.replaceChildren(children);
                });
            });
        }
    }

    public static final class Resolve extends ParameterizedRule<LogicalPlan, LogicalPlan, AnalyzerContext> {
        @Override
        public LogicalPlan apply(LogicalPlan plan, AnalyzerContext context) {
            // A query that never calls a nested function has nothing to bind. Carrying every synthetic attribute through its
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
                if (p instanceof Aggregate) {
                    p.forEachExpression(Nested.class, nested -> {
                        throw error(nested, nested.functionName() + " must be completed in EVAL before outer STATS");
                    });
                }
                p.forEachExpression(Nested.class, nested -> {
                    if (nested.children().stream().anyMatch(c -> c.anyMatch(Nested.class::isInstance))) {
                        throw error(nested, nested.functionName() + " cannot contain another nested function");
                    }
                });
                List<Alias> reductions = new ArrayList<>();
                LogicalPlan lowered = p.transformExpressionsOnlyUp(Nested.class, nested -> {
                    if (nested instanceof NestedJson && p instanceof Eval == false) {
                        throw error(nested, "NESTED_JSON is supported in EVAL");
                    }
                    if (p instanceof Eval == false && p instanceof Filter == false) {
                        throw error(nested, nested.functionName() + " is supported in EVAL and WHERE");
                    }
                    if (context.minimumVersion().supports(NestedReduction.VERSION) == false) {
                        throw error(nested, nested.functionName() + " requires all nodes to support nested reductions");
                    }
                    var reduction = nested instanceof NestedJson json
                        ? bindJson(json, p.children().getFirst(), context)
                        : bind(nested, p.children().getFirst(), context);
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
                        if (a.synthetic() && isPrivateMetadataName(a.name()) && project.outputSet().contains(a) == false) {
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
        String name = nested.functionName();
        Scope scope = Scope.of(input, context);
        Map<String, NestedField> roots = scope.roots();
        Expression body = nested.body();
        if (body.anyMatch(e -> e instanceof Nested || e instanceof FullTextFunction)) {
            throw error(nested, name + " does not allow inner scopes or full-text functions");
        }
        List<Expression> keys = nested.keys();
        if (nested.extraArguments().isEmpty() == false) {
            throw error(nested, name + " takes a collection and one expression");
        }
        String collection = null;
        if (nested.collection() != null) {
            NestedField n = scope.collection(nested.collection());
            if (n == null) {
                throw error(nested, name + " requires a nested collection first, not [" + nested.collection().sourceText() + "]");
            }
            if (n.conflict() != null) {
                throw error(nested, name + " cannot read [" + n.path() + "]: " + n.conflict());
            }
            collection = n.path();
            checkNotReplaced(input, collection, nested, name);
        }
        if (nested instanceof NestedStats == false) {
            if (body.anyMatch(AggregateFunction.class::isInstance)) {
                throw error(nested, name + " takes a predicate; use NESTED for a reduction such as SUM");
            }
            if (body.dataType() != DataType.BOOLEAN) {
                throw error(nested, name + " requires a predicate, not [" + body.sourceText() + "]");
            }
        } else if (keys.isEmpty() == false) {
            if (body instanceof Count == false
                && body instanceof Sum == false
                && body instanceof Min == false
                && body instanceof Max == false) {
                throw error(nested, "NESTED BY supports COUNT, SUM, MIN and MAX, not [" + body.sourceText() + "]");
            }
            for (Expression key : keys) {
                if (key instanceof Attribute == false) {
                    throw error(key, "NESTED BY requires child fields, not [" + key.sourceText() + "]");
                }
                if (scope.child(key) == null) {
                    throw error(key, "Field [" + key.sourceText() + "] is outside the nested scope");
                }
            }
        } else if (body instanceof Count count) {
            if (collection == null && count.field().foldable()) {
                throw error(nested, "NESTED(COUNT(*)) needs the collection to count, such as NESTED(items, COUNT(*))");
            }
        } else if (body instanceof Sum == false && body instanceof Last == false) {
            if (body.anyMatch(AggregateFunction.class::isInstance)) {
                throw error(nested, "NESTED supports COUNT, SUM and LAST only, not [" + body.sourceText() + "]");
            }
            if (body.dataType() == DataType.SOURCE || body.dataType() == DataType.OBJECT) {
                throw error(nested, "NESTED requires a scalar reduction, not an object collection");
            }
            if (body.dataType() == DataType.BOOLEAN) {
                throw error(nested, "NESTED takes a reduction; use NESTED_ANY or NESTED_ALL for a predicate");
            }
            throw error(nested, "NESTED requires a reduction such as SUM, COUNT or LAST, not [" + body.sourceText() + "]");
        }
        if (body instanceof AggregateFunction aggregate) {
            if (aggregate.filter().equals(Literal.TRUE) == false || aggregate.window().equals(AggregateFunction.NO_WINDOW) == false) {
                throw error(nested, name + " does not support inner aggregate filters or windows");
            }
            for (Expression field : aggregate.fields()) {
                if (field.anyMatch(AggregateFunction.class::isInstance)) {
                    throw error(nested, name + " does not support nested aggregates");
                }
            }
        }
        Map<NameId, Attribute> fields = new LinkedHashMap<>();
        body.forEachDown(Attribute.class, a -> fields.putIfAbsent(a.id(), a));
        keys.forEach(key -> fields.putIfAbsent(((Attribute) key).id(), (Attribute) key));
        String path = collection;
        Set<String> indices = new HashSet<>();
        Map<String, String> sourceFailures = new HashMap<>();
        for (Attribute attr : fields.values()) {
            String child = scope.child(attr);
            if (child == null) {
                continue;
            }
            NestedField n = scope.fields().get(child);
            if (n.conflict() != null) {
                throw error(nested, name + " cannot read [" + child + "]: " + n.conflict());
            }
            if (collection != null && collection.equals(n.path()) == false) {
                throw error(nested, "Field [" + child + "] is outside the nested scope [" + collection + "]");
            }
            if (path != null && path.equals(n.path()) == false) {
                throw error(nested, name + " requires one nested path");
            }
            path = n.path();
            indices.addAll(n.indices());
            sourceFailures.putAll(n.sourceFailures());
        }
        if (path == null) {
            throw error(nested, name + " requires a mapped nested field");
        }
        String nestedPath = path;
        if (roots.containsKey(path)) {
            indices = new HashSet<>(roots.get(path).indices());
            sourceFailures.putAll(roots.get(path).sourceFailures());
        }
        if (roots.keySet().stream().anyMatch(parent -> nestedPath.startsWith(parent + "."))) {
            throw error(nested, name + " supports one nesting level");
        }
        if (indices.stream().anyMatch(index -> index.contains(":"))) {
            throw error(nested, name + " cross-cluster loading is not supported by this PoC");
        }
        List<Attribute> channels = new ArrayList<>();
        List<String> paths = new ArrayList<>();
        List<Set<String>> mapped = new ArrayList<>();
        Map<NameId, Attribute> replacement = new HashMap<>();
        for (Attribute attr : fields.values()) {
            String child = scope.child(attr);
            if (child == null) {
                throw error(nested, "Field [" + attr.name() + "] is outside the nested scope");
            }
            if (SUPPORTED_CHILD_TYPES.contains(attr.dataType()) == false) {
                throw error(nested, name + " does not support child type [" + attr.dataType() + "] for [" + child + "]");
            }
            ReferenceAttribute ref = new ReferenceAttribute(attr.source(), child, attr.dataType());
            channels.add(ref);
            replacement.put(attr.id(), ref);
            paths.add(child.substring(path.length() + 1));
            mapped.add(scope.fields().get(child).indices());
        }
        Expression bound = body.transformUp(Attribute.class, a -> replacement.get(a.id()));
        List<Attribute> groupings = keys.stream().map(key -> replacement.get(((Attribute) key).id())).toList();
        if (bound instanceof Sum sum) {
            bound = sum.forTransportVersion(context.minimumVersion());
        }
        return new NestedReduction(
            nested.source(),
            metadata(nested, input),
            path,
            indices,
            bound,
            groupings,
            false,
            nested instanceof NestedAll,
            channels,
            paths,
            mapped,
            sourceFailures
        );
    }

    private static List<Expression> metadata(Nested nested, LogicalPlan input) {
        List<Expression> metadata = new ArrayList<>();
        for (String name : METADATA) {
            Attribute found = input.output()
                .stream()
                .filter(a -> a.synthetic() && a.name().equals(privateMetadataName(name)))
                .findFirst()
                .orElse(null);
            if (found == null) {
                throw error(nested, nested.functionName() + " input is missing required source metadata [" + name + "]");
            }
            metadata.add(found);
        }
        return metadata;
    }

    /**
     * A column named like the collection, such as {@code EVAL items = NESTED_JSON(items)}, hides it from the user; reading
     * the collection behind it would give a different {@code items} than the one the query shows.
     */
    private static void checkNotReplaced(LogicalPlan input, String path, Expression at, String function) {
        if (input.output().stream().anyMatch(a -> a.name().equals(path))) {
            throw error(at, function + " cannot use [" + path + "]: a column of that name replaces it here");
        }
    }

    /** The columns of {@code input} that read a field directly under the nested object {@code path}, in name order. */
    private static List<Attribute> children(LogicalPlan input, Scope scope, String path) {
        List<Attribute> children = new ArrayList<>();
        for (Attribute attribute : input.output()) {
            String child = scope.child(attribute);
            if (child != null && scope.fields().get(child).path().equals(path) && attribute.dataType() != DataType.OBJECT) {
                children.add(attribute);
            }
        }
        children.sort(Comparator.comparing(scope::child));
        return children;
    }

    /** Binds NESTED_JSON to every child field of its collection, in name order, like NESTED_EXPAND. */
    private static NestedReduction bindJson(NestedJson json, LogicalPlan input, AnalyzerContext context) {
        Scope scope = Scope.of(input, context);
        NestedField collection = scope.collection(json.body());
        if (collection == null) {
            throw error(json, "NESTED_JSON requires a nested collection, not [" + json.body().sourceText() + "]");
        }
        String path = collection.path();
        if (collection.conflict() != null) {
            throw error(json, "NESTED_JSON cannot read [" + path + "]: " + collection.conflict());
        }
        checkNotReplaced(input, path, json, "NESTED_JSON");
        if (scope.roots().keySet().stream().anyMatch(parent -> path.startsWith(parent + "."))) {
            throw error(json, "NESTED_JSON supports one nesting level");
        }
        if (collection.indices().stream().anyMatch(index -> index.contains(":"))) {
            throw error(json, "NESTED_JSON cross-cluster loading is not supported by this PoC");
        }
        List<Attribute> channels = new ArrayList<>();
        List<String> paths = new ArrayList<>();
        List<Set<String>> mapped = new ArrayList<>();
        for (Attribute child : children(input, scope, path)) {
            String name = scope.child(child);
            if (JSON_CHILD_TYPES.contains(child.dataType()) == false) {
                throw error(json, "NESTED_JSON does not support child type [" + child.dataType() + "] for [" + name + "]");
            }
            channels.add(new ReferenceAttribute(child.source(), name, child.dataType()));
            paths.add(name.substring(path.length() + 1));
            mapped.add(scope.fields().get(name).indices());
        }
        return new NestedReduction(
            json.source(),
            metadata(json, input),
            path,
            collection.indices(),
            Literal.NULL,
            List.of(),
            true,
            false,
            channels,
            paths,
            mapped,
            collection.sourceFailures()
        );
    }

    /**
     * Binds NESTED_EXPAND to a mapped nested collection in its input. An unknown name stays unbound, so the verifier
     * reports it like any other unknown column. So does a collection whose parent rows the input no longer has, such as
     * after STATS.
     */
    private static LogicalPlan bindExpand(NestedExpand expand, AnalyzerContext context) {
        NamedExpression target = expand.target();
        String name = target.name();
        LogicalPlan input = expand.child();
        List<Attribute> metadata = new ArrayList<>();
        for (String role : METADATA) {
            input.output()
                .stream()
                .filter(a -> a.synthetic() && a.name().equals(privateMetadataName(role)))
                .findFirst()
                .ifPresent(metadata::add);
        }
        if (metadata.size() < METADATA.size()) {
            return expand;
        }
        Scope scope = Scope.of(input, context);
        NestedField collection = scope.fields().get(name);
        if (collection == null || collection.collection() == false) {
            if (input.output().stream().anyMatch(a -> inPath(a.name(), name))) {
                throw error(target, "NESTED_EXPAND requires a mapped nested field, not [" + name + "]");
            }
            return expand;
        }
        if (context.minimumVersion().supports(NestedExpand.VERSION) == false) {
            throw error(target, "NESTED_EXPAND requires all nodes to support nested expansion");
        }
        if (collection.conflict() != null) {
            throw error(target, "NESTED_EXPAND cannot read [" + name + "]: " + collection.conflict());
        }
        String path = collection.path();
        checkNotReplaced(input, path, target, "NESTED_EXPAND");
        if (scope.roots().keySet().stream().anyMatch(parent -> path.startsWith(parent + "."))) {
            throw error(target, "NESTED_EXPAND supports one nesting level");
        }
        if (collection.indices().stream().anyMatch(index -> index.contains(":"))) {
            throw error(target, "NESTED_EXPAND cross-cluster loading is not supported by this PoC");
        }
        List<Attribute> expanded = new ArrayList<>();
        List<String> paths = new ArrayList<>();
        List<Set<String>> mapped = new ArrayList<>();
        for (Attribute child : children(input, scope, path)) {
            String column = scope.child(child);
            if (SUPPORTED_CHILD_TYPES.contains(child.dataType()) == false) {
                throw error(target, "NESTED_EXPAND does not support child type [" + child.dataType() + "] for [" + column + "]");
            }
            expanded.add(new ReferenceAttribute(expand.source(), column, child.dataType()));
            paths.add(column.substring(path.length() + 1));
            mapped.add(scope.fields().get(column).indices());
        }
        return new NestedExpand(
            expand.source(),
            input,
            null,
            metadata,
            path,
            collection.indices(),
            expanded,
            paths,
            mapped,
            collection.sourceFailures()
        );
    }

    private static VerificationException error(Expression expression, String message) {
        return new VerificationException(List.of(Failure.fail(expression, message)));
    }
}
