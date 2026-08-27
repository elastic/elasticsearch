/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * The dimensions of the external-datasource read path, and which pairs of them interact.
 *
 * <p>Testing every combination of every dimension is millions of configurations. What makes that
 * tractable is that most pairs do not interact: varying both together exercises no code path that
 * varying each alone does not. So the declaration records, for every pair, whether they INTERACT --
 * and the test set is derived from that rather than written by hand.
 *
 * <p>A group is a MAXIMAL CLIQUE of the interaction graph: a set of dimensions in which every pair
 * interacts. Each group is tested as a full cross product with every dimension outside it pinned at
 * its declared default, so any generated vector differs from the baseline in only that group's
 * dimensions. Cliques are the sites where a three-way or higher interaction can live at all, since
 * such an effect needs its constituent pairs to interact.
 *
 * <p>The asymmetry that matters: an {@code interacting} verdict costs test executions if it is wrong.
 * An {@code independent} verdict REMOVES cells, and nothing ever reveals that they are missing -- so
 * it carries the mechanism that makes it safe, and an untraceable pair is recorded {@code unverified}
 * and treated as interacting. Uncertainty costs tests, never coverage.
 */
public final class FixtureDimensions {

    private static final String RESOURCE = "fixture-dimensions.properties";

    /** How a pair of dimensions relates. */
    public enum Verdict {
        /** Varying both together reaches code that varying each alone does not. Cross them. */
        INTERACTING,
        /** The cross product exists but carries no new signal. Skipping it is a licensed choice. */
        INDEPENDENT,
        /** No single configuration can hold both values, so there is nothing to test or to justify. */
        DISJOINT,
        /** Not traced. Treated as INTERACTING, so the cells are generated until someone settles it. */
        UNVERIFIED
    }

    private static final FixtureDimensions INSTANCE = load();

    private final List<String> names;
    private final Map<String, List<String>> valuesByName;
    private final Map<String, String> defaultByName;
    private final Map<String, Set<String>> appliesToByName;
    private final Map<String, String> bindsByName;
    private final Map<String, Verdict> verdicts;

    private FixtureDimensions(
        List<String> names,
        Map<String, List<String>> valuesByName,
        Map<String, String> defaultByName,
        Map<String, Set<String>> appliesToByName,
        Map<String, String> bindsByName,
        Map<String, Verdict> verdicts
    ) {
        this.names = List.copyOf(names);
        this.valuesByName = Map.copyOf(valuesByName);
        this.defaultByName = Map.copyOf(defaultByName);
        this.appliesToByName = Map.copyOf(appliesToByName);
        this.bindsByName = Map.copyOf(bindsByName);
        this.verdicts = Map.copyOf(verdicts);
    }

    public static FixtureDimensions get() {
        return INSTANCE;
    }

    private static FixtureDimensions load() {
        Properties props = new Properties();
        try (InputStream in = FixtureDimensions.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("[" + RESOURCE + "] is not on the classpath");
            }
            props.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read [" + RESOURCE + "]", e);
        }

        Map<String, List<String>> values = new LinkedHashMap<>();
        Map<String, String> defaults = new LinkedHashMap<>();
        Map<String, Set<String>> appliesTo = new LinkedHashMap<>();
        Map<String, String> binds = new LinkedHashMap<>();
        Map<String, Verdict> verdicts = new LinkedHashMap<>();

        for (String key : new TreeSet<>(props.stringPropertyNames())) {
            String value = props.getProperty(key).trim();
            if (key.startsWith("dimension.")) {
                String rest = key.substring("dimension.".length());
                int dot = rest.lastIndexOf('.');
                if (dot < 0) {
                    throw new IllegalStateException("malformed dimension key [" + key + "]");
                }
                String name = rest.substring(0, dot);
                switch (rest.substring(dot + 1)) {
                    case "values" -> values.put(name, splitList(value));
                    case "default" -> defaults.put(name, value);
                    case "applies_to" -> appliesTo.put(name, new LinkedHashSet<>(splitList(value)));
                    case "binds" -> binds.put(name, value);
                    default -> throw new IllegalStateException("unknown dimension attribute in [" + key + "]");
                }
            } else if (key.startsWith("pair.")) {
                String rest = key.substring("pair.".length());
                // pair.<a>.<b> is the verdict; pair.<a>.<b>.why carries the mechanism for a reader.
                if (rest.endsWith(".why") || rest.endsWith(".needs")) {
                    continue;
                }
                verdicts.put(rest, parseVerdict(key, value));
            } else if (key.endsWith(".why") || key.endsWith(".needs")) {
                // prose attached to a verdict; carried for the reader, not consumed here
            } else {
                throw new IllegalStateException(
                    "unknown key [" + key + "] in [" + RESOURCE + "]; expected 'dimension.<n>.*' or 'pair.<a>.<b>'"
                );
            }
        }

        List<String> names = new ArrayList<>(values.keySet());
        Collections.sort(names);
        for (String name : names) {
            if (defaults.containsKey(name) == false) {
                throw new IllegalStateException("dimension [" + name + "] declares no default");
            }
            if (values.get(name).contains(defaults.get(name)) == false) {
                throw new IllegalStateException(
                    "dimension [" + name + "] defaults to [" + defaults.get(name) + "], which is not one of its values"
                );
            }
        }
        // The pair table must be TOTAL: adding a dimension without saying how it relates to the
        // existing ones is exactly the unexamined combination this declaration exists to prevent.
        for (int i = 0; i < names.size(); i++) {
            for (int j = i + 1; j < names.size(); j++) {
                String pair = names.get(i) + "." + names.get(j);
                if (verdicts.containsKey(pair) == false) {
                    throw new IllegalStateException("no verdict declared for pair [" + pair + "]");
                }
            }
        }
        for (String name : names) {
            if (binds.containsKey(name) == false) {
                throw new IllegalStateException("dimension [" + name + "] declares no binds -- nothing knows how to make its value real");
            }
        }
        return new FixtureDimensions(names, values, defaults, appliesTo, binds, verdicts);
    }

    private static Verdict parseVerdict(String key, String value) {
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "interacting" -> Verdict.INTERACTING;
            case "independent" -> Verdict.INDEPENDENT;
            case "disjoint" -> Verdict.DISJOINT;
            case "unverified" -> Verdict.UNVERIFIED;
            default -> throw new IllegalStateException("pair [" + key + "] has unknown verdict [" + value + "]");
        };
    }

    private static List<String> splitList(String value) {
        List<String> out = new ArrayList<>();
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty() == false) {
                out.add(trimmed);
            }
        }
        return out;
    }

    /** Every declared dimension, sorted, so derived output is stable across runs. */
    public List<String> names() {
        return names;
    }

    public List<String> values(String dimension) {
        List<String> v = valuesByName.get(dimension);
        if (v == null) {
            throw new IllegalArgumentException("unknown dimension [" + dimension + "]; declared are " + names);
        }
        return v;
    }

    public String defaultValue(String dimension) {
        values(dimension);
        return defaultByName.get(dimension);
    }

    /** The formats this dimension exists for, or empty if it applies to all of them. */
    public Set<String> appliesTo(String dimension) {
        values(dimension);
        return appliesToByName.getOrDefault(dimension, Set.of());
    }

    /**
     * How a value of this dimension becomes real. {@code fixture} changes the bytes on disk, so a
     * vector naming it requires a generated fixture; {@code directive} is a key in the dataset WITH
     * clause and costs nothing; {@code backend} rides the cross product the suites already do;
     * {@code pragma} is a query pragma; {@code cluster} is a node setting and needs its own cluster.
     */
    public String binds(String dimension) {
        values(dimension);
        return bindsByName.get(dimension);
    }

    /** The dimensions in this group whose values require a fixture to exist. */
    public Set<String> fixtureBound(Set<String> group) {
        Set<String> out = new TreeSet<>();
        for (String d : group) {
            if ("fixture".equals(binds(d))) {
                out.add(d);
            }
        }
        return out;
    }

    public Verdict verdict(String a, String b) {
        String pair = a.compareTo(b) < 0 ? a + "." + b : b + "." + a;
        Verdict v = verdicts.get(pair);
        if (v == null) {
            throw new IllegalArgumentException("no verdict for pair [" + pair + "]");
        }
        return v;
    }

    /** Whether the two must be crossed. UNVERIFIED counts as interacting: uncertainty costs tests. */
    public boolean crosses(String a, String b) {
        Verdict v = verdict(a, b);
        return v == Verdict.INTERACTING || v == Verdict.UNVERIFIED;
    }

    /**
     * The groups to cross, as maximal cliques of the interaction graph -- derived, never declared.
     * Flipping one verdict recomputes them, which is what keeps this a mechanism rather than a list.
     */
    public List<Set<String>> groups() {
        List<Set<String>> out = new ArrayList<>();
        bronKerbosch(new LinkedHashSet<>(), new LinkedHashSet<>(names), new LinkedHashSet<>(), out);
        out.removeIf(clique -> clique.size() < 2);
        out.sort((x, y) -> y.size() != x.size() ? y.size() - x.size() : x.toString().compareTo(y.toString()));
        return out;
    }

    private void bronKerbosch(Set<String> r, Set<String> p, Set<String> x, List<Set<String>> out) {
        if (p.isEmpty() && x.isEmpty()) {
            out.add(new TreeSet<>(r));
            return;
        }
        for (String v : new ArrayList<>(p)) {
            Set<String> nv = neighbours(v);
            Set<String> rr = new LinkedHashSet<>(r);
            rr.add(v);
            Set<String> pp = new LinkedHashSet<>(p);
            pp.retainAll(nv);
            Set<String> xx = new LinkedHashSet<>(x);
            xx.retainAll(nv);
            bronKerbosch(rr, pp, xx, out);
            p.remove(v);
            x.add(v);
        }
    }

    private Set<String> neighbours(String dimension) {
        Set<String> out = new LinkedHashSet<>();
        for (String other : names) {
            if (other.equals(dimension) == false && crosses(dimension, other)) {
                out.add(other);
            }
        }
        return out;
    }

    /** The formats a group can be exercised on: the intersection of its members' applicability. */
    public Set<String> formatsFor(Set<String> group) {
        Set<String> formats = new LinkedHashSet<>(values("format"));
        for (String d : group) {
            Set<String> scope = appliesTo(d);
            if (scope.isEmpty() == false) {
                formats.retainAll(scope);
            }
        }
        return formats;
    }

    /**
     * Every vector to run: for each group, the full cross product of its members with all other
     * dimensions at their defaults. Deduplicated, so the all-defaults baseline appears once.
     */
    public List<Map<String, String>> vectors() {
        Map<String, Map<String, String>> unique = new LinkedHashMap<>();
        for (Set<String> group : groups()) {
            Set<String> formats = formatsFor(group);
            if (formats.isEmpty()) {
                continue;
            }
            List<String> axes = new ArrayList<>(group);
            for (Map<String, String> assignment : crossProduct(axes, formats)) {
                Map<String, String> vector = new LinkedHashMap<>();
                for (String d : names) {
                    vector.put(d, defaultValue(d));
                }
                vector.putAll(assignment);
                unique.putIfAbsent(vector.toString(), vector);
            }
        }
        return List.copyOf(unique.values());
    }

    private List<Map<String, String>> crossProduct(List<String> axes, Set<String> formats) {
        List<Map<String, String>> acc = new ArrayList<>();
        acc.add(new LinkedHashMap<>());
        for (String axis : axes) {
            List<String> choices = axis.equals("format") ? new ArrayList<>(formats) : values(axis);
            List<Map<String, String>> next = new ArrayList<>();
            for (Map<String, String> partial : acc) {
                for (String choice : choices) {
                    Map<String, String> copy = new LinkedHashMap<>(partial);
                    copy.put(axis, choice);
                    next.add(copy);
                }
            }
            acc = next;
        }
        // A group not containing `format` still has to pin one the group is legal on.
        if (axes.contains("format") == false) {
            String pinned = formats.contains(defaultValue("format")) ? defaultValue("format") : formats.iterator().next();
            for (Map<String, String> vector : acc) {
                vector.put("format", pinned);
            }
        }
        return acc;
    }
}
