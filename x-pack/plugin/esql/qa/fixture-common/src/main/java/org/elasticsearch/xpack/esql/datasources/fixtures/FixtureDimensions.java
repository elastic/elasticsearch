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
import java.util.function.Consumer;

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

    /**
     * Lazy, deliberately. An eager static field runs {@link #load()} the moment anything on the
     * classpath touches this class -- including code that never asks for a dimension -- so a missing or
     * malformed resource becomes an ExceptionInInitializerError in an unrelated suite. Deferring it means
     * the declaration is read when it is wanted, and the failure names the caller that wanted it.
     */
    /**
     * How a dimension's value is made real. Public because a test that repeats the list drifts from the
     * parser that enforces it, and then agrees with itself while both are wrong.
     *
     * <p>{@code fixture} writes bytes, {@code resolver} changes how those bytes are asked for,
     * {@code directive} and {@code pragma} travel with the query, {@code backend} selects where the data
     * lives, and {@code cluster} needs a differently-configured node.
     */
    public static final Set<String> BINDS = Set.of("fixture", "resolver", "directive", "pragma", "backend", "cluster");

    private static final class Holder {
        private static final FixtureDimensions INSTANCE = load();
    }

    private final List<String> names;
    private final Map<String, List<String>> valuesByName;
    private final Map<String, String> defaultByName;
    private final Map<String, Set<String>> appliesToByName;
    private final Map<String, String> bindsByName;
    private final Map<String, String> directiveKeyByName;
    private final Map<String, Map<String, String>> directiveValuesByName;
    private final Map<String, String> derivedByName;
    private final Map<String, Map<String, String>> derivedValuesByName;
    private final Map<String, String> readKeyByName;
    private final Map<String, String> pragmaKeyByName;
    private final Map<String, Map<String, String>> formatDefaultsByName;
    private final Map<String, Map<String, String>> backendByName;
    private final Map<String, Map<String, String>> absenceByName;
    private final Map<String, Verdict> verdicts;

    private FixtureDimensions(
        List<String> names,
        Map<String, List<String>> valuesByName,
        Map<String, String> defaultByName,
        Map<String, Set<String>> appliesToByName,
        Map<String, String> bindsByName,
        Map<String, String> directiveKeyByName,
        Map<String, Map<String, String>> directiveValuesByName,
        Map<String, String> derivedByName,
        Map<String, Map<String, String>> derivedValuesByName,
        Map<String, String> readKeyByName,
        Map<String, String> pragmaKeyByName,
        Map<String, Map<String, String>> formatDefaultsByName,
        Map<String, Map<String, String>> backendByName,
        Map<String, Map<String, String>> absenceByName,
        Map<String, Verdict> verdicts
    ) {
        this.names = List.copyOf(names);
        this.valuesByName = Map.copyOf(valuesByName);
        this.defaultByName = Map.copyOf(defaultByName);
        this.appliesToByName = Map.copyOf(appliesToByName);
        this.bindsByName = Map.copyOf(bindsByName);
        this.directiveKeyByName = Map.copyOf(directiveKeyByName);
        this.directiveValuesByName = Map.copyOf(directiveValuesByName);
        this.derivedByName = Map.copyOf(derivedByName);
        this.derivedValuesByName = Map.copyOf(derivedValuesByName);
        this.readKeyByName = Map.copyOf(readKeyByName);
        this.pragmaKeyByName = Map.copyOf(pragmaKeyByName);
        this.formatDefaultsByName = Map.copyOf(formatDefaultsByName);
        this.backendByName = Map.copyOf(backendByName);
        this.absenceByName = Map.copyOf(absenceByName);
        this.verdicts = Map.copyOf(verdicts);
    }

    /** The {@code WITH} key a directive-bound dimension travels under, or null when its value is derived. */
    public String directiveKey(String dimension) {
        return directiveKeyByName.get(dimension);
    }

    /**
     * What a dimension's value becomes in the {@code WITH} clause -- the value itself unless the
     * declaration maps it to a different spelling.
     */
    public String directiveValue(String dimension, String value) {
        return directiveValuesByName.getOrDefault(dimension, Map.of()).getOrDefault(value, value);
    }

    /** The reader setting a fixture-bound value announces itself under, or null when it needs none. */
    public String readKey(String dimension) {
        return readKeyByName.get(dimension);
    }

    /**
     * The query pragma a pragma-bound dimension travels under, or null.
     *
     * <p>Deliberately NOT the same map as {@link #directiveKey}: a pragma is not a dataset setting, and
     * putting it in the directive map would make the dimension look directive-expressible, change the
     * generated vector counts, and inject an unknown key into every dataset's WITH clause.
     */
    public String pragmaKey(String dimension) {
        return pragmaKeyByName.get(dimension);
    }

    /**
     * The value a dimension falls back to for one format.
     *
     * <p>Per-format because a reader default can be per-extension: the text mode a {@code .tsv} file is
     * read with is plain, while {@code .csv} is quoted. One global default would make every tsv vector
     * claim to vary a slot that was already at its baseline.
     */
    public String defaultValue(String dimension, String format) {
        return formatDefaultsByName.getOrDefault(dimension, Map.of()).getOrDefault(format, defaultValue(dimension));
    }

    /** The storage backend a value corresponds to, or null when the dimension names none. */
    public String backendFor(String dimension, String value) {
        return backendByName.getOrDefault(dimension, Map.of()).get(value);
    }

    /**
     * The declared reason a value is not exercised, or null when nothing licenses its absence.
     *
     * <p>A per-format reason wins over a bare one, so a value can be a rule on one format and a gap on
     * another without either shadowing the other silently.
     */
    public String absenceReason(String dimension, String value, String format) {
        Map<String, String> declared = absenceByName.getOrDefault(dimension, Map.of());
        String perFormat = declared.get(value + "." + format);
        return perFormat != null ? perFormat : declared.get(value);
    }

    /** What a dimension's value is derived from when no constant can express it, or null when one can. */
    public String derivedFrom(String dimension) {
        return derivedByName.get(dimension);
    }

    /**
     * What one VALUE of a dimension needs beyond itself, or null when it stands alone.
     *
     * <p>Separate from {@link #derivedFrom} because the two are different claims. A dimension is derived
     * when none of its values is a constant; a VALUE is derived when the rest of the dimension is fine and
     * that one option needs a companion. {@code partition_detection} is the second kind -- auto, hive and
     * none are constants, and template alone is rejected because it needs the path template with it.
     */
    public String derivedFromForValue(String dimension, String value) {
        return derivedValuesByName.getOrDefault(dimension, Map.of()).get(value);
    }

    /**
     * The constant {@code WITH} settings a vector pins: every directive-bound slot sitting off its
     * declared default and expressible as a constant.
     *
     * <p>Slots at their default are omitted -- omission IS the default, so an all-defaults vector
     * produces no settings and reads byte-identically to a suite that never heard of vectors. Derived
     * slots are absent too; a caller that wants those has to supply them from the dataset, and
     * {@link #derivedFrom} names what it needs.
     */
    public Map<String, String> directiveSettings(Map<String, String> vector) {
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> slot : vector.entrySet()) {
            String dimension = slot.getKey();
            String key = directiveKeyByName.get(dimension);
            if (key == null || slot.getValue().equals(defaultValue(dimension))) {
                continue;
            }
            out.put(key, directiveValue(dimension, slot.getValue()));
        }
        return out;
    }

    public static FixtureDimensions get() {
        return Holder.INSTANCE;
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
        return parse(props);
    }

    /**
     * Parses a declaration. Separate from {@link #load()} so the validation below -- unknown keys, a
     * default outside its own values, a missing binds, an incomplete pair table -- can be tested against
     * a constructed Properties rather than only against the real resource, which no test can make wrong.
     */
    static FixtureDimensions parse(Properties props) {

        Map<String, List<String>> values = new LinkedHashMap<>();
        Map<String, String> defaults = new LinkedHashMap<>();
        Map<String, Set<String>> appliesTo = new LinkedHashMap<>();
        Map<String, String> binds = new LinkedHashMap<>();
        Map<String, String> directiveKeys = new LinkedHashMap<>();
        Map<String, Map<String, String>> directiveValues = new LinkedHashMap<>();
        Map<String, String> derived = new LinkedHashMap<>();
        Map<String, Map<String, String>> derivedValues = new LinkedHashMap<>();
        Map<String, String> readKeys = new LinkedHashMap<>();
        Map<String, String> declaredKeys = new LinkedHashMap<>();
        Map<String, String> pragmaKeys = new LinkedHashMap<>();
        Map<String, Map<String, String>> formatDefaults = new LinkedHashMap<>();
        Map<String, Map<String, String>> backends = new LinkedHashMap<>();
        Map<String, Map<String, String>> absences = new LinkedHashMap<>();
        Map<String, Verdict> verdicts = new LinkedHashMap<>();

        for (String key : new TreeSet<>(props.stringPropertyNames())) {
            String value = props.getProperty(key).trim();
            if (key.startsWith("dimension.")) {
                // Split on the FIRST dot: a dimension name never contains one, and half the attributes
                // are two-part (`gap.<v>.<format>` is three). Splitting on the last dot instead reads a
                // value name as the attribute, which is why this used to need a special case per
                // two-part attribute -- one per attribute, each easy to forget when adding the next.
                String rest = key.substring("dimension.".length());
                int dot = rest.indexOf('.');
                if (dot < 0) {
                    throw new IllegalStateException("malformed dimension key [" + key + "]");
                }
                String name = rest.substring(0, dot);
                String attribute = rest.substring(dot + 1);
                int sub = attribute.indexOf('.');
                String head = sub < 0 ? attribute : attribute.substring(0, sub);
                String tail = sub < 0 ? null : attribute.substring(sub + 1);
                switch (head) {
                    case "values" -> values.put(name, splitList(requireBare(key, tail, value)));
                    case "applies_to" -> appliesTo.put(name, new LinkedHashSet<>(splitList(requireBare(key, tail, value))));
                    case "binds" -> binds.put(name, requireBare(key, tail, value));
                    case "read_key" -> readKeys.put(name, requireBare(key, tail, value));
                    // Routed to the directive or pragma map after the loop, from `binds` -- doing it here
                    // would depend on `binds` having been seen first, which is alphabetical luck.
                    case "key" -> declaredKeys.put(name, requireBare(key, tail, value));
                    case "default" -> {
                        if (tail == null) {
                            defaults.put(name, value);
                        } else {
                            formatDefaults.computeIfAbsent(name, k -> new LinkedHashMap<>()).put(tail, value);
                        }
                    }
                    case "derived" -> {
                        if (tail == null) {
                            derived.put(name, value);
                        } else {
                            derivedValues.computeIfAbsent(name, k -> new LinkedHashMap<>()).put(tail, value);
                        }
                    }
                    case "value" -> directiveValues.computeIfAbsent(name, k -> new LinkedHashMap<>())
                        .put(requireQualified(key, tail), value);
                    case "backend" -> backends.computeIfAbsent(name, k -> new LinkedHashMap<>()).put(requireQualified(key, tail), value);
                    // `gap.<v>`, `gap.<v>.<format>`, and the `rule.` pair. The kind comes from the key and
                    // the reason text must repeat it, so a copied line whose text says the other kind is a
                    // build failure rather than a report that quietly contradicts itself.
                    case "gap", "rule" -> {
                        String slot = requireQualified(key, tail);
                        if (value.startsWith(head + ":") == false) {
                            throw new IllegalStateException(
                                "absence [" + key + "] is declared as [" + head + "] but its reason does not start with [" + head + ":]"
                            );
                        }
                        absences.computeIfAbsent(name, k -> new LinkedHashMap<>()).put(slot, value);
                    }
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

        // A declared `key` means different things depending on how the dimension binds, and the
        // difference is load-bearing: a pragma key reaching directiveKeyByName would make distribution
        // look directive-expressible, change the per-format vector counts, and inject an unknown
        // setting into every dataset WITH clause.
        for (Map.Entry<String, String> declared : declaredKeys.entrySet()) {
            String owner = declared.getKey();
            if ("pragma".equals(binds.get(owner))) {
                pragmaKeys.put(owner, declared.getValue());
            } else {
                directiveKeys.put(owner, declared.getValue());
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
            // `binds = directive` says the value travels in the WITH clause; it does not say what it
            // becomes there. Without one of these a vector cannot be turned into a query, and the
            // omission would show up as a suite that silently runs its default everywhere.
            boolean isDirective = "directive".equals(binds.get(name));
            boolean hasKey = directiveKeys.containsKey(name);
            boolean isDerived = derived.containsKey(name);
            if (isDirective && hasKey == false && isDerived == false) {
                throw new IllegalStateException(
                    "directive-bound dimension [" + name + "] declares neither a key nor derived -- nothing can express its value"
                );
            }
            if (hasKey && isDerived) {
                throw new IllegalStateException("dimension [" + name + "] declares both a key and derived; they are alternatives");
            }
            if (isDirective == false && (hasKey || isDerived)) {
                throw new IllegalStateException(
                    "dimension [" + name + "] binds as [" + binds.get(name) + "] but declares a directive key or derived"
                );
            }
        }
        for (Map.Entry<String, Map<String, String>> e : derivedValues.entrySet()) {
            if (values.containsKey(e.getKey()) == false) {
                throw new IllegalStateException("derived value declared for unknown dimension [" + e.getKey() + "]");
            }
            for (String v : e.getValue().keySet()) {
                if (values.get(e.getKey()).contains(v) == false) {
                    throw new IllegalStateException(
                        "derived value [" + e.getKey() + ".derived." + v + "] names a value the dimension does not declare"
                    );
                }
            }
        }
        for (Map.Entry<String, Map<String, String>> e : directiveValues.entrySet()) {
            if (values.containsKey(e.getKey()) == false) {
                throw new IllegalStateException("value mapping declared for unknown dimension [" + e.getKey() + "]");
            }
            for (String v : e.getValue().keySet()) {
                if (values.get(e.getKey()).contains(v) == false) {
                    throw new IllegalStateException(
                        "value mapping [" + e.getKey() + ".value." + v + "] names a value the dimension does not declare"
                    );
                }
            }
        }
        for (String name : names) {
            if (BINDS.contains(binds.get(name)) == false) {
                throw new IllegalStateException(
                    "dimension [" + name + "] binds as [" + binds.get(name) + "], which is not one of " + BINDS
                );
            }
        }
        // A read key announces a fixture-bound value to the reader. On any other binding nothing would
        // consume it, so it would sit in the file looking like wiring that exists.
        for (String name : readKeys.keySet()) {
            requireDeclaredDimension(values, name, "read_key");
            if ("fixture".equals(binds.get(name)) == false) {
                throw new IllegalStateException(
                    "dimension ["
                        + name
                        + "] declares a read_key but binds as ["
                        + binds.get(name)
                        + "]; only fixture-bound values are announced"
                );
            }
        }
        List<String> declaredFormats = values.get("format");
        for (Map.Entry<String, Map<String, String>> entry : formatDefaults.entrySet()) {
            requireDeclaredDimension(values, entry.getKey(), "per-format default");
            for (Map.Entry<String, String> perFormat : entry.getValue().entrySet()) {
                if (declaredFormats.contains(perFormat.getKey()) == false) {
                    throw new IllegalStateException(
                        "per-format default [" + entry.getKey() + ".default." + perFormat.getKey() + "] names an undeclared format"
                    );
                }
                if (values.get(entry.getKey()).contains(perFormat.getValue()) == false) {
                    throw new IllegalStateException(
                        "per-format default ["
                            + entry.getKey()
                            + ".default."
                            + perFormat.getKey()
                            + "] is not one of the dimension's values"
                    );
                }
            }
        }
        // A backend correspondence is all-or-nothing: a partial map reads as "these values have a
        // backend and the rest do not", which is indistinguishable from a forgotten line.
        for (Map.Entry<String, Map<String, String>> entry : backends.entrySet()) {
            requireDeclaredDimension(values, entry.getKey(), "backend mapping");
            for (String mapped : entry.getValue().keySet()) {
                if (values.get(entry.getKey()).contains(mapped) == false) {
                    throw new IllegalStateException(
                        "backend mapping [" + entry.getKey() + ".backend." + mapped + "] names a value the dimension does not declare"
                    );
                }
            }
            for (String declared : values.get(entry.getKey())) {
                if (entry.getValue().containsKey(declared) == false) {
                    throw new IllegalStateException(
                        "dimension [" + entry.getKey() + "] declares backend mappings but none for value [" + declared + "]"
                    );
                }
            }
        }
        for (Map.Entry<String, Map<String, String>> entry : absences.entrySet()) {
            String owner = entry.getKey();
            requireDeclaredDimension(values, owner, "absence");
            for (String slot : entry.getValue().keySet()) {
                int at = slot.indexOf('.');
                String absent = at < 0 ? slot : slot.substring(0, at);
                String format = at < 0 ? null : slot.substring(at + 1);
                if (values.get(owner).contains(absent) == false) {
                    throw new IllegalStateException("absence [" + owner + "." + slot + "] names a value the dimension does not declare");
                }
                if (format != null && declaredFormats.contains(format) == false) {
                    throw new IllegalStateException("absence [" + owner + "." + slot + "] names an undeclared format");
                }
                // An absence on the value a vector falls back to would declare the BASELINE missing,
                // which cannot be true -- every vector carries it.
                // Not getOrDefault with a null key: Map.of() rejects one outright, and a bare absence
                // (no format segment) is exactly the case that supplies null here.
                String effectiveDefault = defaults.get(owner);
                if (format != null) {
                    effectiveDefault = formatDefaults.getOrDefault(owner, Map.of()).getOrDefault(format, effectiveDefault);
                }
                if (absent.equals(effectiveDefault)) {
                    throw new IllegalStateException(
                        "absence [" + owner + "." + slot + "] is declared on the effective default value, which every vector carries"
                    );
                }
            }
        }
        return new FixtureDimensions(
            names,
            values,
            defaults,
            appliesTo,
            binds,
            directiveKeys,
            directiveValues,
            derived,
            derivedValues,
            readKeys,
            pragmaKeys,
            formatDefaults,
            backends,
            absences,
            verdicts
        );
    }

    private static void requireDeclaredDimension(Map<String, List<String>> values, String name, String what) {
        if (values.containsKey(name) == false) {
            throw new IllegalStateException(what + " declared for unknown dimension [" + name + "]");
        }
    }

    /** An attribute that takes no value name; a trailing segment means the declaration is malformed. */
    private static String requireBare(String key, String tail, String value) {
        if (tail != null) {
            throw new IllegalStateException("dimension attribute in [" + key + "] takes no qualifier");
        }
        return value;
    }

    /** An attribute that requires a value name, so a bare form cannot silently mean "all values". */
    private static String requireQualified(String key, String tail) {
        if (tail == null) {
            throw new IllegalStateException("dimension attribute in [" + key + "] requires a value name");
        }
        return tail;
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
     * A vector's identity: its off-default slots, or {@code defaults} when it has none.
     *
     * <p>Stable and short, because it becomes part of a test's name -- a failure has to say which
     * combination broke without the reader decoding eighteen slots, seventeen of which are the baseline.
     */
    public String render(Map<String, String> vector) {
        StringBuilder out = new StringBuilder();
        String format = vector.get("format");
        for (String name : names) {
            String value = vector.get(name);
            // Against the vector's OWN format: quoted is the baseline on csv and a variation on tsv, so a
            // global comparison names the wrong slots off-default and hides the ones that are.
            if (value == null || value.equals(defaultValue(name, format))) {
                continue;
            }
            out.append(out.isEmpty() ? "" : ",").append(name).append('=').append(value);
        }
        return out.isEmpty() ? "defaults" : out.toString();
    }

    /**
     * The inverse of {@link #render}: the off-default slots a rendered name carries.
     *
     * <p>Validated rather than trusted. The name survives a round trip through a test parameter, and a
     * dimension or value that no longer exists would otherwise inject a setting the reader rejects, or
     * -- worse -- silently inject nothing and let the case pass as the baseline it is not.
     */
    public Map<String, String> parseRendered(String rendered) {
        Map<String, String> out = new LinkedHashMap<>();
        if (rendered.equals("defaults")) {
            return out;
        }
        for (String slot : rendered.split(",")) {
            int eq = slot.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("malformed vector name [" + rendered + "]");
            }
            String dimension = slot.substring(0, eq);
            String value = slot.substring(eq + 1);
            if (valuesByName.containsKey(dimension) == false) {
                throw new IllegalArgumentException("vector name [" + rendered + "] names unknown dimension [" + dimension + "]");
            }
            if (valuesByName.get(dimension).contains(value) == false) {
                throw new IllegalArgumentException(
                    "vector name [" + rendered + "] gives [" + dimension + "] a value it does not declare [" + value + "]"
                );
            }
            out.put(dimension, value);
        }
        return out;
    }

    /**
     * The vectors for one format that a directive alone can express: every off-default slot binds as a
     * directive AND declares a constant key.
     *
     * <p>This is deliberately a small subset of {@link #vectors()}. A slot bound to a fixture, a cluster
     * setting, a pragma or a backend needs something built before it can be run, and a derived slot needs
     * the dataset. Those are not skipped quietly -- they are simply not expressible through this seam, and
     * naming the seam in the method is what keeps that visible at the call site.
     */
    public List<Map<String, String>> directiveExpressibleVectors(String format) {
        List<Map<String, String>> out = new ArrayList<>();
        Set<String> rendered = new LinkedHashSet<>();
        forEachVector(vector -> {
            if (format.equals(vector.get("format")) == false) {
                return;
            }
            for (Map.Entry<String, String> slot : vector.entrySet()) {
                String dimension = slot.getKey();
                if (dimension.equals("format") || slot.getValue().equals(defaultValue(dimension, format))) {
                    continue;
                }
                if (directiveKeyByName.containsKey(dimension) == false) {
                    return;
                }
                if (derivedFromForValue(dimension, slot.getValue()) != null) {
                    return;
                }
            }
            if (rendered.add(render(vector))) {
                out.add(Map.copyOf(vector));
            }
        });
        return out;
    }

    /**
     * Feeds every vector to the consumer without materialising the set.
     *
     * <p>Vectors are a product, so the count grows multiplicatively with the declaration -- eleven
     * thousand today, and a single added dimension multiplies rather than adds. Holding them all is a
     * heap cost with no purpose for a caller that only iterates, so this is the primary form and
     * {@link #vectors()} is the convenience that pays for a list.
     *
     * <p>Deduplication is unavoidable here -- every group shares the all-defaults baseline -- but it
     * costs one set of maps rather than a set of maps plus a rendered key per vector.
     */
    public void forEachVector(Consumer<Map<String, String>> consumer) {
        Set<Map<String, String>> seen = new LinkedHashSet<>();
        for (Set<String> group : groups()) {
            Set<String> formats = formatsFor(group);
            if (formats.isEmpty()) {
                continue;
            }
            for (Map<String, String> assignment : crossProduct(new ArrayList<>(group), formats)) {
                // The baseline has to be filled with THIS format's defaults, not the global ones. A
                // reader default can be per-extension -- .tsv reads plain where .csv reads quoted -- so a
                // globally-filled baseline hands every tsv vector an off-default text_mode it never asked
                // for. That stays invisible while the selection predicate makes the same mistake, and the
                // two cancel; it surfaces as a silent drop the moment either side is corrected alone.
                String format = assignment.get("format");
                Map<String, String> vector = new LinkedHashMap<>();
                for (String d : names) {
                    vector.put(d, defaultValue(d, format));
                }
                vector.putAll(assignment);
                if (seen.add(Map.copyOf(vector))) {
                    consumer.accept(vector);
                }
            }
        }
    }

    /**
     * Every vector as a list, for callers that genuinely need one -- a {@code @ParametersFactory}
     * cannot stream. Prefer {@link #forEachVector} anywhere else.
     */
    public List<Map<String, String>> vectors() {
        List<Map<String, String>> out = new ArrayList<>();
        forEachVector(out::add);
        return List.copyOf(out);
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
