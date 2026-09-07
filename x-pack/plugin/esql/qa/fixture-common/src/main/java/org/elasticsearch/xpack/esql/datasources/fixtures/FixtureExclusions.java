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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The spec cases a suite does not run, and why.
 * <p>
 * Read from {@code fixture-exclusions.properties}. Before this, the same information lived in five
 * separate {@code SKIPPED_TESTS} sets across five suite classes, so "what are we not testing?" could
 * only be answered by reading all five — and it was possible to work in this area at length without
 * knowing two of them existed.
 * <p>
 * Each suite still enforces its own exclusions; it reads them from here instead of holding its own
 * copy. Making membership itself declarative — a suite runs everything unless excluded, rather than
 * only what it names — is the remaining half, tracked separately.
 */
public final class FixtureExclusions {

    private static final String RESOURCE = "fixture-exclusions.properties";
    private static final FixtureExclusions INSTANCE = load();

    /** Kind of exclusion: a defect to fix, or something the suite cannot express at all. */
    public enum Kind {
        /**
         * A defect, or a capability the reader has not implemented yet. Either way the case is right
         * and the reader is not, so removing the entry is how a fix gets verified. If a reason says
         * "re-enable once ...", it is this kind.
         */
        BUG,
        /**
         * The suite cannot express the case at all, and no fix would change that -- the format has no
         * such type, or its schema is inferred rather than declared. Permanent. Nothing to re-enable.
         */
        RULE
    }

    /** One exclusion: which suite, which case, what kind, and the reason in full. */
    /**
     * @param vectorSlots the {@code dimension.value} slots a vector must ALL carry for this to apply,
     *                    comma-separated, or null for every vector.
     */
    public record Exclusion(String suite, String spec, String caseName, String vectorSlots, Kind kind, String reason) {
        /**
         * Whether this exclusion covers a case running under the given vector.
         *
         * <p>Every named slot must match, because some defects are reachable only by a PAIR of values and
         * naming one of them takes away far more than the defect. #1880 is the case that forced this:
         * escaped mode splits a data row at an escaped delimiter, so on tsv it fails under
         * {@code escaped + semicolon} while {@code escaped} on its own and {@code semicolon} on its own
         * both pass -- measured, 4 failing instances against about 20 passing ones on the same case. A
         * single-slot exclusion on either value would have deleted every one of those passing cells and
         * reported a green suite for doing it.
         *
         * <p>This mirrors what the dimension declaration already learned separately: its
         * {@code value_disjoint} exists because a per-cell grammar cannot say that only a COMBINATION is
         * impossible. The same is true of defects.
         */
        public boolean appliesTo(Map<String, String> vector) {
            if (vectorSlots == null) {
                return true;
            }
            for (String slot : vectorSlots.split(",")) {
                String trimmed = slot.trim();
                int dot = trimmed.indexOf('.');
                if (trimmed.substring(dot + 1).equals(vector.get(trimmed.substring(0, dot))) == false) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Suite token -> (spec, case) -> exclusion. The spec is part of the KEY, not merely a field on the
     * value: keying by case name alone let two same-named cases in different specs of one suite clobber
     * each other, silently re-enabling the loser. The three-argument {@link #find} used to compensate by
     * filtering after the lookup, which cannot recover an entry the map never stored.
     */
    private final Map<String, Map<SpecCase, Exclusion>> bySuite;

    /** Slot list in a stable order, so two spellings of one conjunction are the same key. */
    private static String canonicalSlots(String slots) {
        if (slots == null) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        for (String slot : slots.split(",", -1)) {
            parts.add(slot.trim());
        }
        Collections.sort(parts);
        return String.join(",", parts);
    }

    /** The identity of an excluded case: its spec and its name. */
    private record SpecCase(String spec, String caseName, String vectorSlots) {}

    private final Set<String> declaredSuites;

    public static FixtureExclusions get() {
        return INSTANCE;
    }

    private static FixtureExclusions load() {
        Properties props = new Properties();
        try (InputStream in = FixtureExclusions.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(
                    "exclusion declaration ["
                        + RESOURCE
                        + "] is not on the classpath; the module reading it must "
                        + "depend on esql:qa:fixture-common"
                );
            }
            props.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read [" + RESOURCE + "]", e);
        }
        return new FixtureExclusions(props);
    }

    /**
     * Parses a declaration. Separate from {@link #load()} so a test can exercise the GRAMMAR -- the
     * bug-versus-rule distinction, suite validation, the vector-slot qualifier -- against a constructed
     * Properties rather than against the live corpus.
     *
     * <p>It exists because the alternative bit. A grammar test that reached into the real declaration for
     * an example of a {@code bug:} entry named one specific defect, and deleting that entry when its defect
     * was fixed broke the test -- so the corpus could not be kept current without editing a test that has
     * nothing to do with the fix. Mirrors {@link FixtureDimensions#parse} for the same reason.
     */
    static FixtureExclusions parse(Properties props) {
        return new FixtureExclusions(props);
    }

    private FixtureExclusions(Properties props) {
        Map<String, Map<SpecCase, Exclusion>> parsed = new LinkedHashMap<>();
        String suitesValue = props.getProperty("suites");
        if (suitesValue == null || suitesValue.isBlank()) {
            throw new IllegalStateException("fixture-exclusions.properties must declare a 'suites' list");
        }
        Set<String> declaredSuites = new LinkedHashSet<>();
        for (String token : suitesValue.split(",")) {
            String trimmed = token.trim();
            if (trimmed.isEmpty() == false) {
                declaredSuites.add(trimmed);
            }
        }

        for (String key : props.stringPropertyNames()) {
            if (key.equals("suites") || key.startsWith("reason.")) {
                continue;
            }
            // Fail on an unrecognised key rather than skipping it. FixtureMatrix has always rejected
            // unknown keys; this file silently ignored them, so a mistyped or newly-invented key -- a
            // `frozen.<suite>` before the parser learned it, a `reason` misspelt as `reasons` -- read as
            // an absent declaration and did nothing at all.
            if (key.startsWith("exclude.") == false) {
                throw new IllegalStateException(
                    "unknown key [" + key + "] in [" + RESOURCE + "]; expected 'suites', 'reason.<name>' or 'exclude.<suite>.<spec>.<case>'"
                );
            }
            String rest = key.substring("exclude.".length());
            // FIRST dot: the key is exclude.<suite>.<spec>.<case>, and the suite token is the leading
            // segment. lastIndexOf would swallow the spec into the suite name.
            int dot = rest.indexOf('.');
            if (dot < 0) {
                throw new IllegalStateException("malformed exclusion key [" + key + "]; expected exclude.<suite>.<caseName>");
            }
            String suite = rest.substring(0, dot);
            String afterSuite = rest.substring(dot + 1);
            int specDot = afterSuite.indexOf('.');
            if (specDot < 0) {
                throw new IllegalStateException(
                    "malformed exclusion key ["
                        + key
                        + "]; expected exclude.<suite>.<spec>.<caseName>. The spec segment is required: case names are "
                        + "NOT unique across spec files, so a key without it silences every same-named case."
                );
            }
            String spec = afterSuite.substring(0, specDot);
            String caseOnly = afterSuite.substring(specDot + 1);
            if (declaredSuites.contains(suite) == false) {
                throw new IllegalStateException(
                    "exclusion ["
                        + key
                        + "] names suite ["
                        + suite
                        + "], which is not in the declared 'suites' list "
                        + declaredSuites
                        + ". A typo here creates a phantom suite whose entries never apply to any test."
                );
            }
            String value = props.getProperty(key).trim();

            int colon = value.indexOf(':');
            if (colon < 0) {
                throw new IllegalStateException(
                    "exclusion ["
                        + key
                        + "] has no kind. Write 'bug: <symptom>' for a defect, or 'rule: <why>' for "
                        + "something the suite cannot express."
                );
            }
            String kindText = value.substring(0, colon).trim();
            Kind kind = switch (kindText) {
                case "bug" -> Kind.BUG;
                case "rule" -> Kind.RULE;
                default -> throw new IllegalStateException(
                    "exclusion [" + key + "] has unknown kind [" + kindText + "]; expected 'bug' or 'rule'"
                );
            };
            String reason = value.substring(colon + 1).trim();
            if (reason.isEmpty()) {
                throw new IllegalStateException("exclusion [" + key + "] states a kind but no reason");
            }
            // A reason of the form @name resolves to the shared reason.<name>. One defect usually disables
            // several cases, and repeating its paragraph per case meant N copies that drift the moment one
            // is corrected -- roughly 300 of this file's 367 lines were verbatim duplicates.
            if (reason.startsWith("@")) {
                String reasonKey = reason.substring(1).trim();
                String shared = props.getProperty("reason." + reasonKey);
                if (shared == null || shared.isBlank()) {
                    throw new IllegalStateException(
                        "exclusion ["
                            + key
                            + "] references shared reason [@"
                            + reasonKey
                            + "], which is not declared as "
                            + "[reason."
                            + reasonKey
                            + "]"
                    );
                }
                reason = shared.trim();
            }
            // A repeated key is a real hazard rather than a tidiness issue: Properties.load keeps the
            // last value silently, so two entries for one case resolve to whichever happens to be lower
            // in the file. This file carried four such duplicates, one of them with two DIFFERENT
            // reasons. The composite key means a duplicate now collides here, where it can be reported.
            // `case@dimension.value` narrows an exclusion to the vectors that carry that slot. Without it a
            // per-vector defect costs the whole case: hivePartitionWhereIsNull fails under six vectors and
            // PASSES under twenty-one, and a case-wide exclusion would silently discard all twenty-one.
            //
            // Several slots may be named, comma-separated, and a vector must carry them ALL. That is not a
            // convenience: a defect reachable only by a value PAIR cannot be narrowed to one of its values
            // without deleting every cell where the other value is fine on its own.
            int at = caseOnly.indexOf('@');
            String bare = at < 0 ? caseOnly : caseOnly.substring(0, at);
            String slots = at < 0 ? null : caseOnly.substring(at + 1);
            // `@dimension.value`, not `@dimension=value`: Properties splits a key on the first unescaped
            // '=', so an equals here is read as the key/value separator and the qualifier silently becomes
            // part of the reason. A dot cannot collide -- dimension names and values are snake_case.
            if (slots != null) {
                // -1 keeps trailing empty segments: `split(",")` drops them, so a trailing comma would
                // pass validation and then silently mean nothing.
                for (String slot : slots.split(",", -1)) {
                    String trimmed = slot.trim();
                    if (trimmed.isEmpty() || trimmed.indexOf('.') < 0) {
                        throw new IllegalStateException(
                            "exclusion [" + key + "] has a vector qualifier [" + trimmed + "] that is not <dimension>.<value>"
                        );
                    }
                }
            }
            // The KEY canonicalises the slot list, the stored value keeps the author's spelling. Without
            // this, `@a.x,b.y` and `@b.y,a.x` are two entries for one cell -- they mean the same
            // conjunction, so the second is a duplicate that would silently shadow rather than collide.
            Exclusion previous = parsed.computeIfAbsent(suite, k -> new LinkedHashMap<>())
                .put(new SpecCase(spec, bare, canonicalSlots(slots)), new Exclusion(suite, spec, bare, slots, kind, reason));
            if (previous != null) {
                throw new IllegalStateException(
                    "duplicate exclusion ["
                        + key
                        + "] in ["
                        + RESOURCE
                        + "]; it is declared more than once, and only the last declaration would take effect"
                );
            }
        }
        this.bySuite = Map.copyOf(parsed);
        this.declaredSuites = Set.copyOf(declaredSuites);
    }

    /** The case names the given suite does not run. */
    public Set<String> casesFor(String suite) {
        return bySuite.getOrDefault(suite, Map.of()).keySet().stream().map(SpecCase::caseName).collect(Collectors.toSet());
    }

    /** Every exclusion the given suite declares. */
    public Iterable<Exclusion> forSuite(String suite) {
        return bySuite.getOrDefault(suite, Map.of()).values();
    }

    /**
     * Bug-classified exclusions citing no filed issue.
     *
     * <p>Reported rather than thrown. This class is a singleton every suite loads, so a policy failure
     * here fails every test in every module at class initialisation -- which is not enforcement, it is an
     * outage. The check belongs in a gate task that can go red on its own, exactly as the dimension
     * contract's does; the loader's job is to parse.
     *
     * <p>Why the policy exists: without a ticket, {@code bug:} is a promise nobody is holding. The case
     * stops running, the reason ages in a properties file, and nothing tracks the fix. A {@code rule:}
     * needs no ticket -- it records a decision, not an outage.
     */
    public List<Exclusion> uncitedBugs() {
        List<Exclusion> uncited = new ArrayList<>();
        for (Map<SpecCase, Exclusion> bySpec : bySuite.values()) {
            for (Exclusion exclusion : bySpec.values()) {
                if (exclusion.kind() == Kind.BUG && FixtureDimensions.ISSUE_REFERENCE.matcher(exclusion.reason()).find() == false) {
                    uncited.add(exclusion);
                }
            }
        }
        return uncited;
    }

    /** The exclusion for a case on a suite, or {@code null} if the suite runs it. */
    public Exclusion find(String suite, String caseName) {
        return bySuite.getOrDefault(suite, Map.of())
            .entrySet()
            .stream()
            .filter(e -> e.getKey().caseName().equals(caseName))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
    }

    /**
     * The exclusion for a case in a specific spec, or {@code null} if that suite runs it.
     *
     * <p>The spec is part of the key because case names are not unique across spec files. Within a
     * single suite's routed set the collisions are few -- promoting the Hive-shadow spec to the shared
     * directory put {@code noneStopsTheColumnSubstitution} and {@code shadowedColumnSubstitutionAndWarning}
     * into two specs the parquet suite both loads -- but one is enough to silence the wrong case, and
     * across the whole spec corpus dozens of names repeat.
     */
    public Exclusion find(String suite, String spec, String caseName) {
        return find(suite, spec, caseName, Map.of());
    }

    /**
     * The exclusion covering a case under one vector, or {@code null} if it runs.
     *
     * <p>An unqualified entry covers every vector; a {@code @dimension=value} entry covers only the
     * vectors carrying that slot. Without the distinction a defect that appears under one configuration
     * would disable the case under all of them, which is coverage lost to bookkeeping.
     */
    public Exclusion find(String suite, String spec, String caseName, Map<String, String> vector) {
        for (Map.Entry<SpecCase, Exclusion> entry : bySuite.getOrDefault(suite, Map.of()).entrySet()) {
            if (entry.getKey().spec().equals(spec) == false || entry.getKey().caseName().equals(caseName) == false) {
                continue;
            }
            if (entry.getValue().appliesTo(vector)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * Every suite token the declaration recognises -- the single authority. Tests and callers read this
     * rather than restating it: the previous hand-copied lists (one in the test asserting five tokens,
     * another asserting eight) drifted from each other and from this file, which is the same
     * duplicate-registry failure the declaration exists to remove.
     */
    public Set<String> declaredSuites() {
        return declaredSuites;
    }

    /** Every suite that declares at least one exclusion. */
    public Set<String> suites() {
        return bySuite.keySet();
    }

    /** Total number of declared exclusions, across every suite. */
    public int size() {
        return bySuite.values().stream().mapToInt(Map::size).sum();
    }
}
