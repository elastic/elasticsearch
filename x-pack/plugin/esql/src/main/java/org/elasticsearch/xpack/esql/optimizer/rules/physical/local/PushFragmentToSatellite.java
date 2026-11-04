/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.SatelliteFragment;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Contains;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.ProjectAwayColumns;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.splitAnd;

/**
 * Extracts the longest pushable {@code [Project] [Limit|TopN] [Eval] [Filter]} chain sitting on an {@link ExternalSourceExec}
 * whose metadata carries the {@code _satellite} capability stamp, translates it to the satellite protocol IR (see
 * {@code satellite-spec.md}) and replaces it with the same exec carrying a {@link SatelliteFragment}. The leaf's output is the
 * output of the extracted subtree, so the satellite returns exactly those columns.
 * <ul>
 *   <li>Top-down rule: it fires on the highest node of the chain, not on its fragments.</li>
 *   <li>Filter conjuncts that cannot be translated, or that the satellite did not declare, stay behind in a remainder
 *   {@link FilterExec}; the walk stops there.</li>
 *   <li>A pushed Limit/TopN is the root of the fragment (placement rule of the protocol) and is also kept in ES above the leaf, to
 *   merge the partial results of several splits. Nothing above it is pushed.</li>
 * </ul>
 */
public class PushFragmentToSatellite extends PhysicalOptimizerRules.ParameterizedOptimizerRule<UnaryExec, LocalPhysicalOptimizerContext> {

    private static final int MAX_PLAN_DEPTH = 64;
    /** The cheapest to parse and send first: a datetime is a text to parse on the satellite, and a keyword is the heaviest to send. */
    private static final List<DataType> CHEAPEST_COLUMN_TYPES = List.of(
        DataType.INTEGER,
        DataType.LONG,
        DataType.BOOLEAN,
        DataType.DOUBLE,
        DataType.DATETIME
    );

    /** What the satellite declared (section 6 of the protocol) plus the columns it sends as lists. */
    record Caps(
        Set<String> plan,
        Set<String> functions,
        Set<String> types,
        Set<String> listColumns,
        Set<String> strictColumns,
        Set<String> options,
        int maxFragmentBytes,
        int maxExpressionDepth,
        int maxInListSize,
        int maxPatternBytes
    ) {
        static Caps of(Map<?, ?> m) {
            if (numbers(m.get("specVersions")).contains(0) == false || strings(m.get("fragmentFormats")).contains("json") == false) {
                return null;
            }
            Map<?, ?> limits = m.get("limits") instanceof Map<?, ?> l ? l : Map.of();
            return new Caps(
                strings(m.get("plan")),
                strings(m.get("functions")),
                strings(m.get("types")),
                strings(m.get("listColumns")),
                strings(m.get("strictColumns")),
                strings(m.get("options")),
                limit(limits, "maxFragmentBytes", 1024 * 1024),
                limit(limits, "maxExpressionDepth", 64),
                limit(limits, "maxInListSize", 1024),
                limit(limits, "maxPatternBytes", 1024)
            );
        }

        private static int limit(Map<?, ?> limits, String name, int dflt) {
            return limits.get(name) instanceof Number n ? n.intValue() : dflt;
        }

        private static Set<Integer> numbers(Object o) {
            Set<Integer> s = new HashSet<>();
            if (o instanceof List<?> l) {
                l.forEach(e -> s.add(e instanceof Number n ? n.intValue() : -1));
            }
            return s;
        }

        private static Set<String> strings(Object o) {
            Set<String> s = new HashSet<>();
            if (o instanceof List<?> l) {
                l.forEach(e -> s.add(String.valueOf(e)));
            }
            return s;
        }
    }

    private static Caps caps(ExternalSourceExec ext) {
        return ext.sourceMetadata().get(SatelliteFragment.CAPABILITY_KEY) instanceof Map<?, ?> m ? Caps.of(m) : null;
    }

    @Override
    protected PhysicalPlan rule(UnaryExec top, LocalPhysicalOptimizerContext ctx) {
        List<UnaryExec> chain = new ArrayList<>(); // top-first
        PhysicalPlan current = top;
        while (current instanceof ProjectExec
            || current instanceof LimitExec
            || current instanceof TopNExec
            || current instanceof EvalExec
            || current instanceof FilterExec) {
            chain.add((UnaryExec) current);
            current = ((UnaryExec) current).child();
        }
        // a source with no operator above it is only visited as the child of its parent
        boolean bare = chain.isEmpty();
        PhysicalPlan leaf = bare ? top.child() : current;
        if (leaf instanceof ExternalSourceExec == false) {
            return top;
        }
        ExternalSourceExec ext = (ExternalSourceExec) leaf;
        Caps caps = caps(ext);
        if (caps == null || ext.pushedFilter() != null) {
            return top;
        }
        // Source options (protocol section 11) reach a satellite only inside a fragment. A dataset that sets options is therefore
        // always read through one, even with nothing to push or with the pushdown switched off (then the fragment is the scan
        // alone), and a read that cannot send a fragment fails instead of silently ignoring the options.
        Map<String, Object> options = declaredOptions(ext.config(), caps);
        boolean mustPush = options.isEmpty() == false;
        boolean pushdownOff = ctx.configuration().pragmas().satellitePushdown() == false;
        if (pushdownOff && mustPush == false) {
            return top;
        }
        if (defaultSettings(ctx) == false) {
            return decline(top, mustPush, "the query's locale or time zone is not the default (en-US, UTC)");
        }
        if (caps.plan.contains("scan") == false) {
            return decline(top, mustPush, "the satellite does not declare the scan operator");
        }
        Translator tr = new Translator(caps);
        if (ext.output().isEmpty() || ext.output().stream().anyMatch(a -> ProjectAwayColumns.ALL_FIELDS_PROJECTED.equals(a.name()))) {
            // COUNT(*) and the like read no column, and a fragment cannot scan zero columns: scan the cheapest one and let the
            // operators above ignore it, so that the satellite does not parse and send the whole row
            Attribute narrow = tr.narrowestColumn(ext);
            if (narrow == null) {
                return decline(
                    top,
                    mustPush,
                    "the query reads no column of the source (for example COUNT(*)) and the table has no column to scan instead"
                );
            }
            ext = ext.withAttributes(List.of(narrow));
        }
        Map<String, Object> plan = tr.scan(ext);
        if (plan == null) {
            return decline(top, mustPush, "a scanned column has a type the protocol cannot carry");
        }

        List<UnaryExec> retained = new ArrayList<>(); // pushed Limit/TopN kept in ES too, bottom-up
        List<Attribute> leafOutput = ext.output();
        boolean afterBreaker = false;
        int depth = 1;
        Expression remainder = null;
        int stop = -1; // chain index (top-first) of the first node not (fully) pushed; -1 when everything was
        for (int i = chain.size() - 1; i >= 0 && stop < 0; i--) {
            UnaryExec op = chain.get(i);
            Map<String, Object> next = null;
            if (pushdownOff || afterBreaker || ++depth > MAX_PLAN_DEPTH) {
                stop = i;
            } else if (op instanceof ProjectExec p) {
                next = caps.plan.contains("project") ? tr.project(p, plan) : null;
                if (next != null) {
                    leafOutput = p.output();
                }
            } else if (op instanceof FilterExec f) {
                List<Expression> pushed = new ArrayList<>();
                List<Expression> rest = new ArrayList<>();
                for (Expression conjunct : splitAnd(f.condition())) {
                    (caps.plan.contains("filter") && tr.expr(conjunct) != null ? pushed : rest).add(conjunct);
                }
                if (pushed.isEmpty() == false) {
                    Translator.E cond = tr.expr(Predicates.combineAnd(pushed));
                    if (cond != null) {
                        next = Translator.node("filter", "condition", cond.node, "input", plan);
                    } else {
                        rest.addAll(pushed);
                    }
                }
                if (rest.isEmpty() == false) {
                    remainder = Predicates.combineAnd(rest);
                    stop = i;
                }
            } else if (op instanceof EvalExec e) {
                next = caps.plan.contains("eval") ? tr.eval(e, plan, leafOutput) : null;
                if (next != null) {
                    leafOutput = e.output();
                }
            } else if (op instanceof LimitExec l && caps.plan.contains("limit") && l.limit() instanceof Literal lit) {
                next = tr.limit(lit, plan);
                if (next != null) {
                    retained.add(l);
                    afterBreaker = true;
                }
            } else if (op instanceof TopNExec t && caps.plan.contains("topn") && t.limit() instanceof Literal lit) {
                next = tr.topn(t, lit, plan);
                if (next != null) {
                    retained.add(t);
                    afterBreaker = true;
                }
            }
            if (next != null) {
                plan = next;
            } else if (stop < 0) {
                stop = i;
            }
        }
        // A scan of only the columns the query reads is pushed even with nothing else to push: a read without a fragment
        // takes every column of the table off the satellite.
        List<Map<String, Object>> output = tr.columns(leafOutput);
        if (output == null) {
            return decline(top, mustPush, "an output column has a type the protocol cannot carry");
        }
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("specVersion", 0);
        request.put("format", "json");
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("locale", "en-US");
        settings.put("timeZone", "UTC");
        request.put("settings", settings);
        if (mustPush) {
            request.put("options", options);
        }
        request.put("output", output);
        request.put("plan", plan);

        if (fits(request, caps.maxFragmentBytes) == false) {
            return decline(top, mustPush, "the fragment is larger than the satellite accepts");
        }

        PhysicalPlan rebuilt = ext.withAttributes(leafOutput).withPushedFilter(new SatelliteFragment(request));
        if (bare) {
            return top.replaceChild(rebuilt);
        }
        for (UnaryExec r : retained) {
            rebuilt = r.replaceChild(rebuilt);
        }
        for (int i = stop < 0 ? -1 : stop; i >= 0; i--) {
            UnaryExec op = chain.get(i);
            if (i == stop && op instanceof FilterExec f && remainder != null) {
                rebuilt = new FilterExec(f.source(), rebuilt, remainder);
            } else {
                rebuilt = op.replaceChild(rebuilt);
            }
        }
        return rebuilt;
    }

    /** The dataset's source options the satellite declared: scalars, in a stable order. */
    private static Map<String, Object> declaredOptions(Map<String, Object> config, Caps caps) {
        Map<String, Object> options = new TreeMap<>();
        config.forEach((k, v) -> {
            if (SatelliteFragment.isOptionKey(k)
                && caps.options.contains(k)
                && (v instanceof String || v instanceof Number || v instanceof Boolean)) {
                options.put(k, v);
            }
        });
        return options;
    }

    /** Not pushing is always allowed, unless source options need the fragment: then the query fails and says why. */
    private static PhysicalPlan decline(PhysicalPlan top, boolean mustPush, String reason) {
        if (mustPush) {
            throw new IllegalArgumentException(
                "the dataset sets source options, which reach the satellite only inside a fragment, but no fragment can be sent: " + reason
            );
        }
        return top;
    }

    /** Room kept for the {@code partitions} entry the connector adds: the base64 of a ticket of up to 16 KiB. */
    private static final int PARTITIONS_HEADROOM_BYTES = 24 * 1024;

    /** ES MUST NOT send a fragment above the declared {@code maxFragmentBytes}, and cannot fall back once it is sent. */
    private static boolean fits(Map<String, Object> request, int maxFragmentBytes) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            // a limit as small as the schema allows (1,024) leaves no room for 24 KiB: keep a quarter of the limit free instead
            int headroom = Math.min(PARTITIONS_HEADROOM_BYTES, maxFragmentBytes / 4);
            return Strings.toString(builder.map(request)).getBytes(StandardCharsets.UTF_8).length + headroom <= maxFragmentBytes;
        } catch (IOException e) {
            return false;
        }
    }

    /** v0 pushes only with the default settings, which a satellite supports (protocol section 7). */
    private static boolean defaultSettings(LocalPhysicalOptimizerContext ctx) {
        return Locale.US.equals(ctx.configuration().locale())
            && ZoneOffset.UTC.equals(QuerySettings.TIME_ZONE.get(ctx.configuration().resolvedSettings()).normalized());
    }

    /** Builds the IR of the protocol; every method returns null when something is not translatable or not declared. */
    static final class Translator {
        record E(Map<String, Object> node, int depth, DataType type) {}

        private static final Map<DataType, String> IR_TYPES = Map.of(
            DataType.INTEGER,
            "integer",
            DataType.LONG,
            "long",
            DataType.DOUBLE,
            "double",
            DataType.BOOLEAN,
            "boolean",
            DataType.KEYWORD,
            "keyword",
            DataType.DATETIME,
            "datetime",
            DataType.DATE_NANOS,
            "date_nanos"
        );

        private final Caps caps;

        Translator(Caps caps) {
            this.caps = caps;
        }

        static Map<String, Object> node(String op, Object... kv) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("op", op);
            for (int i = 0; i < kv.length; i += 2) {
                m.put((String) kv[i], kv[i + 1]);
            }
            return m;
        }

        static String irType(DataType t) {
            return IR_TYPES.get(t);
        }

        private String declaredType(DataType t) {
            String ir = irType(t);
            return ir != null && caps.types.contains(ir) ? ir : null;
        }

        List<Map<String, Object>> columns(List<Attribute> attrs) {
            List<Map<String, Object>> cols = new ArrayList<>();
            Set<String> names = new HashSet<>();
            for (Attribute a : attrs) {
                String type = declaredType(a.dataType());
                if (type == null || names.add(a.name()) == false || a.name().isEmpty()) {
                    return null;
                }
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("name", a.name());
                c.put("type", type);
                cols.add(c);
            }
            return cols.isEmpty() ? null : cols;
        }

        /**
         * The column of the table to scan when a query reads none: one the satellite can scan with the strict Arrow types, and the
         * cheapest to parse and send among them (see {@link #CHEAPEST_COLUMN_TYPES}).
         */
        Attribute narrowestColumn(ExternalSourceExec ext) {
            if (ext.unifiedSchema() == null) {
                return null;
            }
            Attribute best = null;
            int bestRank = Integer.MAX_VALUE;
            for (Attribute a : ext.unifiedSchema()) {
                if (declaredType(a.dataType()) == null
                    || caps.listColumns.contains(a.name())
                    || caps.strictColumns.contains(a.name()) == false) {
                    continue;
                }
                int rank = CHEAPEST_COLUMN_TYPES.indexOf(a.dataType());
                rank = rank < 0 ? CHEAPEST_COLUMN_TYPES.size() : rank;
                if (rank < bestRank) {
                    best = a;
                    bestRank = rank;
                }
            }
            return best;
        }

        Map<String, Object> scan(ExternalSourceExec ext) {
            List<Map<String, Object>> cols = columns(ext.output());
            Object target = ext.config().get("target");
            if (cols == null || target instanceof String == false || ((String) target).isEmpty()) {
                return null;
            }
            // a satellite refuses a column whose Arrow type is outside the strict set of the protocol, so it cannot be pushed
            for (Attribute a : ext.output()) {
                if (caps.strictColumns.contains(a.name()) == false) {
                    return null;
                }
            }
            return node("scan", "target", target, "columns", cols);
        }

        Map<String, Object> project(ProjectExec p, Map<String, Object> input) {
            List<Map<String, Object>> cols = new ArrayList<>();
            for (Expression e : p.projections()) {
                if (e instanceof Attribute a == false) {
                    return null;
                }
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("name", ((Attribute) e).name());
                c.put("from", ((Attribute) e).name());
                cols.add(c);
            }
            return columns(p.output()) == null ? null : node("project", "columns", cols, "input", input);
        }

        Map<String, Object> eval(EvalExec e, Map<String, Object> input, List<Attribute> inputOutput) {
            Set<String> taken = new HashSet<>();
            inputOutput.forEach(a -> taken.add(a.name()));
            List<Map<String, Object>> fields = new ArrayList<>();
            for (Alias a : e.fields()) {
                E expr = expr(a.child());
                if (expr == null || declaredType(a.dataType()) == null || taken.add(a.name()) == false) {
                    return null;
                }
                Map<String, Object> f = new LinkedHashMap<>();
                f.put("name", a.name());
                f.put("expr", expr.node);
                fields.add(f);
            }
            return fields.isEmpty() ? null : node("eval", "fields", fields, "input", input);
        }

        Map<String, Object> limit(Literal count, Map<String, Object> input) {
            return count.value() instanceof Integer n && n >= 0 ? node("limit", "count", n, "input", input) : null;
        }

        Map<String, Object> topn(TopNExec t, Literal limit, Map<String, Object> input) {
            if (limit.value() instanceof Integer n == false || (Integer) limit.value() < 0) {
                return null;
            }
            List<Map<String, Object>> order = new ArrayList<>();
            for (Order o : t.order()) {
                if (o.child() instanceof Attribute == false) {
                    return null;
                }
                E key = expr(o.child());
                if (key == null || TOPN_KEY_TYPES.contains(key.type) == false) {
                    return null;
                }
                Map<String, Object> k = new LinkedHashMap<>();
                k.put("expr", key.node);
                k.put("direction", o.direction() == Order.OrderDirection.ASC ? "asc" : "desc");
                k.put("nulls", o.nullsPosition() == Order.NullsPosition.FIRST ? "first" : "last");
                order.add(k);
            }
            return order.isEmpty() ? null : node("topn", "order", order, "limit", limit.value(), "input", input);
        }

        /** Boolean and double keys have no defined order in v0 (the order of booleans, and of the signs of zero). */
        private static final Set<DataType> TOPN_KEY_TYPES = Set.of(
            DataType.INTEGER,
            DataType.LONG,
            DataType.KEYWORD,
            DataType.DATETIME,
            DataType.DATE_NANOS
        );

        private static boolean numeric(DataType t) {
            return t == DataType.INTEGER || t == DataType.LONG || t == DataType.DOUBLE;
        }

        private static boolean sameFamily(DataType a, DataType b, boolean allowBoolean) {
            if (numeric(a) && numeric(b)) {
                return true;
            }
            return a == b
                && (a == DataType.KEYWORD || a == DataType.DATETIME || a == DataType.DATE_NANOS || (allowBoolean && a == DataType.BOOLEAN));
        }

        private static DataType widest(List<E> args) {
            DataType w = DataType.INTEGER;
            for (E a : args) {
                if (a.type == DataType.DOUBLE || (a.type == DataType.LONG && w == DataType.INTEGER)) {
                    w = a.type;
                }
            }
            return w;
        }

        E expr(Expression e) {
            E r = translate(e);
            return r == null || r.depth > caps.maxExpressionDepth ? null : r;
        }

        private List<E> args(List<Expression> children) {
            List<E> out = new ArrayList<>();
            for (Expression c : children) {
                E t = translate(c);
                if (t == null) {
                    return null;
                }
                out.add(t);
            }
            return out;
        }

        private E fn(String name, Expression e, List<E> args) {
            String type = declaredType(e.dataType());
            if (args == null || type == null || caps.functions.contains(name) == false) {
                return null;
            }
            int depth = 1;
            List<Object> list = new ArrayList<>();
            for (E a : args) {
                depth = Math.max(depth, a.depth + 1);
                list.add(a.node);
            }
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("fn", name);
            n.put("type", type);
            n.put("args", list);
            return new E(n, depth, e.dataType());
        }

        private E translate(Expression e) {
            if (e instanceof Attribute a) {
                String type = declaredType(a.dataType());
                if (type == null || caps.listColumns.contains(a.name())) {
                    return null;
                }
                return new E(new LinkedHashMap<>(Map.of("col", a.name())), 1, a.dataType());
            }
            if (e instanceof Literal lit) {
                return literal(lit);
            }
            if (e instanceof And || e instanceof Or) {
                List<E> args = args(e.children());
                return args != null && args.stream().allMatch(a -> a.type == DataType.BOOLEAN)
                    ? fn(e instanceof And ? "and" : "or", e, args)
                    : null;
            }
            if (e instanceof Not) {
                List<E> args = args(e.children());
                return args != null && args.get(0).type == DataType.BOOLEAN ? fn("not", e, args) : null;
            }
            String cmp = comparison(e);
            if (cmp != null) {
                List<E> args = args(e.children());
                return args != null && sameFamily(args.get(0).type, args.get(1).type, cmp.equals("equals") || cmp.equals("not_equals"))
                    ? fn(cmp, e, args)
                    : null;
            }
            if (e instanceof In in) {
                List<Expression> all = new ArrayList<>();
                all.add(in.value());
                all.addAll(in.list());
                List<E> args = args(all);
                if (args == null || in.list().isEmpty() || in.list().size() > caps.maxInListSize) {
                    return null;
                }
                boolean ok = args.stream().allMatch(a -> sameFamily(args.get(0).type, a.type, true));
                return ok ? fn("in", e, args) : null;
            }
            if (e instanceof IsNull || e instanceof IsNotNull) {
                List<E> args = args(e.children());
                return fn(e instanceof IsNull ? "is_null" : "is_not_null", e, args);
            }
            if (e instanceof Add || e instanceof Sub || e instanceof Mul) {
                List<E> args = args(e.children());
                if (args == null || args.stream().allMatch(a -> numeric(a.type)) == false || e.dataType() != widest(args)) {
                    return null;
                }
                return fn(e instanceof Add ? "add" : e instanceof Sub ? "sub" : "mul", e, args);
            }
            if (e instanceof Neg || e instanceof Abs) {
                List<E> args = args(e.children());
                if (args == null || numeric(args.get(0).type) == false || e.dataType() != args.get(0).type) {
                    return null;
                }
                return fn(e instanceof Neg ? "neg" : "abs", e, args);
            }
            if (e instanceof StartsWith || e instanceof EndsWith || e instanceof Contains) {
                List<E> args = args(e.children());
                if (args == null || args.stream().allMatch(a -> a.type == DataType.KEYWORD) == false) {
                    return null;
                }
                return fn(e instanceof StartsWith ? "starts_with" : e instanceof EndsWith ? "ends_with" : "contains", e, args);
            }
            if (e instanceof Coalesce) {
                List<E> args = args(e.children());
                if (args == null || args.size() < 2 || args.stream().allMatch(a -> a.type == args.get(0).type) == false) {
                    return null;
                }
                return fn("coalesce", e, args);
            }
            if (e instanceof WildcardLike like && like.caseInsensitive() == false) {
                String pattern = like.pattern().pattern();
                E value = translate(e.children().get(0));
                if (value == null || value.type != DataType.KEYWORD || validPattern(pattern) == false) {
                    return null;
                }
                Map<String, Object> lit = new LinkedHashMap<>();
                lit.put("lit", pattern);
                lit.put("type", "keyword");
                return fn("like", e, List.of(value, new E(lit, 1, DataType.KEYWORD)));
            }
            return null;
        }

        /** Every backslash must escape {@code *}, {@code ?} or a backslash, and the pattern must fit the declared limit. */
        private boolean validPattern(String pattern) {
            if (pattern.getBytes(StandardCharsets.UTF_8).length > caps.maxPatternBytes) {
                return false;
            }
            for (int i = 0; i < pattern.length(); i++) {
                if (pattern.charAt(i) == '\\') {
                    if (i + 1 >= pattern.length() || "*?\\".indexOf(pattern.charAt(i + 1)) < 0) {
                        return false;
                    }
                    i++;
                }
            }
            return true;
        }

        private static String comparison(Expression e) {
            if (e instanceof Equals) return "equals";
            if (e instanceof NotEquals) return "not_equals";
            if (e instanceof GreaterThan) return "greater_than";
            if (e instanceof GreaterThanOrEqual) return "greater_than_or_equal";
            if (e instanceof LessThan) return "less_than";
            if (e instanceof LessThanOrEqual) return "less_than_or_equal";
            return null;
        }

        private E literal(Literal lit) {
            String type = declaredType(lit.dataType());
            Object v = lit.value();
            if (type == null || v == null) {
                return null; // null literals are never pushed in v0
            }
            DataType dt = lit.dataType();
            Object encoded = null;
            if (dt == DataType.INTEGER) {
                encoded = v instanceof Integer ? v : null;
            } else if (dt == DataType.LONG || dt == DataType.DATETIME || dt == DataType.DATE_NANOS) {
                encoded = v instanceof Long ? String.valueOf(v) : null;
            } else if (dt == DataType.DOUBLE) {
                encoded = v instanceof Double d && Double.isFinite(d) && (d != 0.0 || 1 / d > 0) ? v : null;
            } else if (dt == DataType.BOOLEAN) {
                encoded = v instanceof Boolean ? v : null;
            } else if (dt == DataType.KEYWORD) {
                encoded = v instanceof BytesRef b ? b.utf8ToString() : null;
            }
            if (encoded == null) {
                return null;
            }
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("lit", encoded);
            n.put("type", type);
            return new E(n, 1, lit.dataType());
        }
    }
}
