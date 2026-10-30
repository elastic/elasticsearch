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
 * The tree skeleton ({@code id}, {@code type}, {@code role}, {@code source}, {@code origin}, {@code children})
 * is never truncated. {@link EsQueryExec} Lucene DSL is parked under {@code properties.detail.query}
 * so consumers can ignore it. {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec} exposes its
 * inner logical plan as the sole JSON child.
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
            builder.nullField("line");
            builder.nullField("column");
            builder.nullField("text");
            builder.nullField("view");
        } else {
            builder.field("line", source.lineNumber());
            builder.field("column", source.columnNumber());
            builder.field("text", source.text());
            if (source.viewName() == null) {
                builder.nullField("view");
            } else {
                builder.field("view", source.viewName());
            }
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
            writeValue(builder, prop);
        }
        if (detailQuery != null) {
            builder.startObject("detail");
            builder.field("query", detailQuery);
            builder.endObject();
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
        for (int i = 0; i < structural.size(); i++) {
            if (structural.get(i) == prop) {
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
            for (Object element : iterable) {
                writeValue(builder, element);
            }
            builder.endArray();
        } else if (value instanceof QueryBuilder query) {
            builder.value(query.toString());
        } else if (value instanceof Enum<?> enumeration) {
            builder.value(enumeration.name());
        } else if (value instanceof Boolean || value instanceof Number || value instanceof String) {
            builder.value(value);
        } else {
            builder.value(String.valueOf(value));
        }
    }

    private static void writeMap(XContentBuilder builder, Map<?, ?> map) throws IOException {
        boolean stringKeys = true;
        for (Object key : map.keySet()) {
            if (key != null && key instanceof String == false) {
                stringKeys = false;
                break;
            }
        }
        if (stringKeys == false) {
            builder.value(String.valueOf(map));
            return;
        }
        builder.startObject();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey() == null ? "null" : (String) entry.getKey();
            builder.field(key);
            writeValue(builder, entry.getValue());
        }
        builder.endObject();
    }

    private static void writeExpression(XContentBuilder builder, Expression expression) throws IOException {
        builder.startObject();
        builder.field("type", expression.nodeName());
        writeSource(builder, "source", expression.source());
        builder.field("repr", fullRepr(expression));
        List<Expression> args = expression.children();
        if (args.isEmpty() == false) {
            builder.startArray("args");
            for (Expression arg : args) {
                writeExpression(builder, arg);
            }
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
        int n = 2;
        String candidate = name + "_" + n;
        while (used.add(candidate) == false) {
            n++;
            candidate = name + "_" + n;
        }
        return candidate;
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
