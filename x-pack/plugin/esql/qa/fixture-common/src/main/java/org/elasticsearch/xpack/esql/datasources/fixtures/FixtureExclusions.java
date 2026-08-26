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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
    public record Exclusion(String suite, String spec, String caseName, Kind kind, String reason) {}

    private final Map<String, Map<String, Exclusion>> bySuite;
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

    private FixtureExclusions(Properties props) {
        Map<String, Map<String, Exclusion>> parsed = new LinkedHashMap<>();
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
            if (key.startsWith("exclude.") == false) {
                continue;
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
            parsed.computeIfAbsent(suite, k -> new LinkedHashMap<>()).put(caseOnly, new Exclusion(suite, spec, caseOnly, kind, reason));
        }
        this.bySuite = Map.copyOf(parsed);
        this.declaredSuites = Set.copyOf(declaredSuites);
    }

    /** The case names the given suite does not run. */
    public Set<String> casesFor(String suite) {
        return bySuite.getOrDefault(suite, Map.of()).keySet();
    }

    /** Every exclusion the given suite declares. */
    public Iterable<Exclusion> forSuite(String suite) {
        return bySuite.getOrDefault(suite, Map.of()).values();
    }

    /** The exclusion for a case on a suite, or {@code null} if the suite runs it. */
    public Exclusion find(String suite, String caseName) {
        return bySuite.getOrDefault(suite, Map.of()).get(caseName);
    }

    /**
     * The exclusion for a case in a specific spec, or {@code null} if that suite runs it.
     *
     * <p>The spec is part of the identity because case names are NOT unique across spec files -- 24 names
     * are duplicated, 48 instances -- so matching on the bare name silences every same-named case. A
     * lookup that ignored the spec would, for example, take the exclusion declared against
     * external-multivalue and apply it to csv-multivalue's identically-named case as well.
     */
    public Exclusion find(String suite, String spec, String caseName) {
        Exclusion candidate = bySuite.getOrDefault(suite, Map.of()).get(caseName);
        return candidate != null && candidate.spec().equals(spec) ? candidate : null;
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
