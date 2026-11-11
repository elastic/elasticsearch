/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.PlanId;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Renders a {@link QueryPlan} as the stable JSON document consumed by EXPLAIN, PROFILE, and the
 * {@code org.elasticsearch.xpack.esql.plan.json} logger.
 * <p>
 * Walks query-plan nodes only. Expressions are nested under {@code properties}, not given plan ids.
 * The nodes of the tree are all written, but what a property holds is bounded like the ASCII plan: a string is cut at
 * {@link #MAX_STRING_CHARS} characters, and a list or a map at {@link #MAX_ELEMENTS} elements, with a {@code truncated} marker that says
 * how many were left out. The JSON is copied into the profile of every compute of a query, so a Lucene query or an index list that is
 * huge must not make it huge. {@link EsQueryExec} Lucene DSL is parked under {@code properties.detail.query} so consumers can
 * ignore it. A node reports its
 * properties through {@link QueryPlan#jsonPropertyValue}, so the JSON shows no more than the ASCII plan does.
 * {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec} exposes its inner logical plan as the sole JSON child.
 */
public final class PlanToXContent {
    private static final Map<Class<?>, List<Method>> ACCESSORS = new ConcurrentHashMap<>();

    private static final Set<String> SKIP_ACCESSORS = Set.of(
        "output",
        "outputSet",
        "inputSet",
        "references",
        "expressions",
        "children",
        "jsonChildren",
        "source",
        "sourceText",
        "sourceLocation",
        "nodeName",
        "nodeProperties",
        "planId",
        "origin",
        "exchange",
        "planRole",
        "toString",
        "hashCode",
        "equals",
        "getClass",
        "getWriteableName",
        "resolved",
        "expressionsResolved",
        "childrenResolved",
        "analyzed",
        "preAnalyzed",
        "optimized",
        "preOptimized",
        "skipTelemetry",
        "telemetryLabel",
        "leftReferences",
        "rightReferences"
    );

    /**
     * The longest string a property is written with, in characters. The ASCII plan is bounded the same way, and the JSON is copied
     * into the profile of every compute of a query, so one raw Lucene query or a long literal must not make it huge.
     */
    static final int MAX_STRING_CHARS = 4096;
    /** The most elements of a list or entries of a map a property is written with. */
    static final int MAX_ELEMENTS = 100;

    private PlanToXContent() {}

    public static void writePlan(XContentBuilder builder, QueryPlan<?> plan) throws IOException {
        builder.startObject();
        builder.field("id", plan.planId().toString());
        builder.field("type", plan.nodeName());
        builder.field("role", plan instanceof LogicalPlan ? "logical" : "physical");
        writeSource(builder, "source", plan.source());
        builder.startArray("origin");
        for (PlanId id : plan.planOrigin()) {
            builder.value(id.toString());
        }
        builder.endArray();
        if (plan.exchange() != null) {
            builder.field("exchange_id", plan.exchange().id());
            if (plan.exchange().childIndex() != null) {
                builder.field("child_index", plan.exchange().childIndex());
            }
        }
        writeProperties(builder, plan);
        builder.startArray("children");
        for (QueryPlan<?> child : plan.jsonChildren()) {
            writePlan(builder, child);
        }
        builder.endArray();
        builder.endObject();
    }

    private static void writeSource(XContentBuilder builder, String field, Source source) throws IOException {
        builder.startObject(field);
        if (source == null || source == Source.EMPTY) {
            builder.nullField("line").nullField("column").nullField("text").nullField("view");
        } else {
            builder.field("line", source.lineNumber());
            builder.field("column", source.columnNumber());
            builder.field("text", source.text());
            builder.field("view", source.viewName());
        }
        builder.endObject();
    }

    private static void writeProperties(XContentBuilder builder, QueryPlan<?> plan) throws IOException {
        builder.startObject("properties");
        List<Object> props = plan.nodeProperties();
        List<Method> accessors = accessors(plan.getClass());
        boolean[] used = new boolean[accessors.size()];
        Set<String> names = new HashSet<>();
        String detailQuery = null;
        int unnamed = 0;
        for (Object prop : props) {
            if (prop == null || isChildProperty(plan, prop)) {
                continue;
            }
            String name = matchAccessor(plan, prop, accessors, used);
            if (name == null) {
                name = "p" + unnamed++;
            }
            name = uniqueName(name, names);
            if (plan instanceof EsQueryExec && ("queryBuilderAndTags".equals(name) || prop instanceof QueryBuilder)) {
                detailQuery = queryText((EsQueryExec) plan, prop);
                continue;
            }
            builder.field(name);
            writeValue(builder, plan.jsonPropertyValue(prop));
        }
        if (detailQuery != null) {
            builder.startObject("detail").field("query", truncateIf(detailQuery)).endObject();
        }
        builder.endObject();
    }

    private static String queryText(EsQueryExec exec, Object prop) {
        try {
            QueryBuilder query = exec.query();
            if (query != null) {
                return query.toString();
            }
        } catch (UnsupportedOperationException ignored) {
            // Tagged or split queries no longer have a single original builder.
        }
        return prop instanceof QueryBuilder ? prop.toString() : String.valueOf(exec.queryBuilderAndTags());
    }

    private static boolean isChildProperty(QueryPlan<?> plan, Object prop) {
        List<?> structural = plan.children();
        if (structural == prop || (structural.isEmpty() == false && structural.equals(prop))) {
            return true;
        }
        for (Object child : structural) {
            if (child == prop) {
                return true;
            }
        }
        for (QueryPlan<?> child : plan.jsonChildren()) {
            if (child == prop) {
                return true;
            }
        }
        return false;
    }

    private static void writeValue(XContentBuilder builder, Object value) throws IOException {
        if (value == null) {
            builder.nullValue();
        } else if (value instanceof Expression expression) {
            writeExpression(builder, expression);
        } else if (value instanceof QueryPlan<?> nested) {
            writePlan(builder, nested);
        } else if (value instanceof Map<?, ?> map) {
            writeMap(builder, map);
        } else if (value instanceof Iterable<?> iterable) {
            builder.startArray();
            int count = 0;
            for (Object element : iterable) {
                if (count++ < MAX_ELEMENTS) {
                    writeValue(builder, element);
                }
            }
            writeTruncated(builder, count - MAX_ELEMENTS);
            builder.endArray();
        } else if (value instanceof Enum<?> enumeration) {
            builder.value(enumeration.name());
        } else if (value instanceof Boolean || value instanceof Number) {
            builder.value(value);
        } else {
            // Strings, QueryBuilders and anything else the writer has no structure for
            builder.value(truncateIf(String.valueOf(value)));
        }
    }

    /** {@code string} itself, or when it is longer than {@link #MAX_STRING_CHARS}, its start and how many characters were left out. */
    private static String truncateIf(String string) {
        if (string.length() <= MAX_STRING_CHARS) {
            return string;
        }
        return string.substring(0, MAX_STRING_CHARS) + "... [" + (string.length() - MAX_STRING_CHARS) + " more characters]";
    }

    /**
     * Ends a list that was cut with an object that says how many elements were left out, which no element of a plan looks like.
     * Nothing is written when none were.
     */
    private static void writeTruncated(XContentBuilder builder, int omitted) throws IOException {
        if (omitted > 0) {
            builder.startObject().field("truncated", omitted).endObject();
        }
    }

    private static void writeMap(XContentBuilder builder, Map<?, ?> map) throws IOException {
        if (map.keySet().stream().anyMatch(key -> key != null && key instanceof String == false)) {
            builder.value(truncateIf(String.valueOf(map)));
            return;
        }
        builder.startObject();
        int count = 0;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (count++ == MAX_ELEMENTS) {
                builder.field("truncated", map.size() - MAX_ELEMENTS);
                break;
            }
            builder.field(String.valueOf(entry.getKey()));
            writeValue(builder, entry.getValue());
        }
        builder.endObject();
    }

    private static void writeExpression(XContentBuilder builder, Expression expression) throws IOException {
        builder.startObject();
        builder.field("type", expression.nodeName());
        writeSource(builder, "source", expression.source());
        builder.field("repr", truncateIf(fullRepr(expression)));
        List<Expression> args = expression.children();
        if (args.isEmpty() == false) {
            builder.startArray("args");
            for (int i = 0; i < Math.min(args.size(), MAX_ELEMENTS); i++) {
                writeExpression(builder, args.get(i));
            }
            writeTruncated(builder, args.size() - MAX_ELEMENTS);
            builder.endArray();
        }
        builder.endObject();
    }

    private static String fullRepr(Node<?> node) {
        StringBuilder sb = new StringBuilder();
        node.nodeString(sb, Node.NodeStringFormat.FULL, NodeStringMapper.IDENTITY);
        return sb.toString();
    }

    /**
     * Names a non-child property from a public no-arg accessor whose return value is that property.
     * Plan nodes do not store property names on {@link NodeInfo}; accessor names ({@code condition},
     * {@code fields}, {@code indexPattern}) are the stable labels consumers see.
     */
    private static String matchAccessor(QueryPlan<?> plan, Object prop, List<Method> accessors, boolean[] used) {
        String byIdentity = match(plan, prop, accessors, used, true);
        if (byIdentity != null) {
            return byIdentity;
        }
        return match(plan, prop, accessors, used, false);
    }

    private static String match(QueryPlan<?> plan, Object prop, List<Method> accessors, boolean[] used, boolean identity) {
        for (int i = 0; i < accessors.size(); i++) {
            if (used[i]) {
                continue;
            }
            Object value;
            try {
                value = accessors.get(i).invoke(plan);
            } catch (IllegalAccessException | InvocationTargetException | RuntimeException e) {
                continue;
            }
            boolean hit = identity ? value == prop : (value != null && value.equals(prop));
            if (hit) {
                used[i] = true;
                return propertyName(accessors.get(i));
            }
        }
        return null;
    }

    private static String propertyName(Method method) {
        String name = method.getName();
        if (name.startsWith("get") && name.length() > 3 && Character.isUpperCase(name.charAt(3))) {
            return Character.toLowerCase(name.charAt(3)) + name.substring(4);
        }
        if (name.startsWith("is") && name.length() > 2 && Character.isUpperCase(name.charAt(2))) {
            return Character.toLowerCase(name.charAt(2)) + name.substring(3);
        }
        return name;
    }

    private static String uniqueName(String name, Set<String> used) {
        if (used.add(name)) {
            return name;
        }
        for (int n = 2;; n++) {
            String candidate = name + "_" + n;
            if (used.add(candidate)) {
                return candidate;
            }
        }
    }

    private static List<Method> accessors(Class<?> type) {
        return ACCESSORS.computeIfAbsent(type, PlanToXContent::findAccessors);
    }

    private static List<Method> findAccessors(Class<?> type) {
        // First public method of each name wins, so an override and the method it replaces are not both candidates.
        List<Method> methods = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Method method : type.getMethods()) {
            if (method.getParameterCount() != 0 || method.getReturnType() == void.class) {
                continue;
            }
            if (Modifier.isStatic(method.getModifiers()) || method.isSynthetic() || method.isBridge()) {
                continue;
            }
            String name = method.getName();
            if (SKIP_ACCESSORS.contains(name) || skipPrefix(name)) {
                continue;
            }
            Class<?> declared = method.getDeclaringClass();
            if (declared == Object.class || declared == Node.class || declared == QueryPlan.class) {
                continue;
            }
            if (seen.add(name)) {
                methods.add(method);
            }
        }
        // Class#getMethods has no defined order. Properties with equal values, such as two booleans, are named by the first unused
        // accessor that returns them, so the order decides which name each gets, and it must not change from one JVM to the next.
        methods.sort(Comparator.comparing(Method::getName));
        return List.copyOf(methods);
    }

    private static boolean skipPrefix(String name) {
        return name.startsWith("set")
            || name.startsWith("write")
            || name.startsWith("read")
            || name.startsWith("transform")
            || name.startsWith("replace")
            || name.startsWith("forEach")
            || name.startsWith("collect");
    }
}
