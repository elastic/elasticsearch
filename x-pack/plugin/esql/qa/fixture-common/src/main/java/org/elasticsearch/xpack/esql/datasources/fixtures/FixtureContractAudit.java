/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Answers one question for every cell of the contract: can a vector carrying this value actually run,
 * and if not, does the declaration say why?
 *
 * <p>A cell is (dimension, value, format). Cells at the format's effective default are skipped -- every
 * vector carries the baseline, so it is exercised by construction. Every other cell is either reachable
 * through a seam, or licensed by a typed {@code gap.}/{@code rule.} reason, or it is an UNDECLARED
 * ABSENCE and the build fails.
 *
 * <p><b>Reachable means a vector that runs, not bytes that exist.</b> ORC fixtures are generated for
 * every layout, but no ORC vector suite consumes them, so ORC's 360 vectors run nowhere. Defining
 * reachability as "the generator wrote something" would call that cell covered and let the real absence
 * hide behind a full directory.
 *
 * <p>The fixture seam is deliberately NOT satisfied by a {@code read_key}. A read key says how a value
 * would announce itself if the bytes existed; it says nothing about whether anything writes them. Only
 * an explicit row in {@link FixtureCapabilities} counts, and a row is added by the same change that
 * implements the rendering -- so the gate can go red the day it lands rather than two increments later.
 */
public final class FixtureContractAudit {

    /** Values of a resolver-bound dimension the suites can currently ask for. None until that seam lands. */
    private static final Set<String> RESOLVER_CAPABILITIES = Set.of();

    private static boolean resolverServes(String dimension, String value, String format) {
        return RESOLVER_CAPABILITIES.contains(dimension + "=" + value + "@" + format);
    }

    /**
     * Whether the pragma seam is wired. A declared pragma key alone never made a value runnable; what
     * makes it runnable is EsqlSpecTestCase.addSuitePragmas carrying it onto the query, which
     * AbstractExternalSourceSpecTestCase overrides. Flipped with that wiring, not before it.
     */
    private static final boolean PRAGMA_SEAM_WIRED = true;

    private FixtureContractAudit() {}

    /** One cell's verdict: how it is reachable, or how its absence is licensed, or neither. */
    record Cell(String dimension, String value, String format, String verdict, boolean violation, String detail) {}

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: FixtureContractAudit <reportFile>");
        }
        Path report = Path.of(args[0]);
        List<Cell> cells = audit(FixtureDimensions.get());

        List<String> out = new ArrayList<>();
        out.add("dimension contract audit");
        out.add("");
        out.add("Every (dimension, value, format) cell off its effective default. A cell is reachable through");
        out.add("a seam, licensed by a typed reason, or a violation. Cells at the default are omitted: every");
        out.add("vector carries the baseline.");
        out.add("");
        for (Cell cell : cells) {
            out.add(
                String.format(
                    Locale.ROOT,
                    "  %-20s %-22s %-8s %-14s %s",
                    cell.dimension(),
                    cell.value(),
                    cell.format(),
                    cell.verdict(),
                    cell.detail()
                )
            );
        }
        // The same standard the contract holds itself to, applied to the exclusions: a defect that stops a
        // case running must name a filed issue. Checked HERE rather than in the loader -- FixtureExclusions
        // is a singleton every suite loads, so throwing there fails every test in every module at class
        // init, which is an outage rather than enforcement.
        List<FixtureExclusions.Exclusion> uncited = FixtureExclusions.get().uncitedBugs();
        for (FixtureExclusions.Exclusion exclusion : uncited) {
            out.add(
                String.format(
                    Locale.ROOT,
                    "  %-20s %-22s %-8s %-14s %s",
                    exclusion.suite(),
                    exclusion.caseName(),
                    "-",
                    "UNCITED-BUG",
                    "excluded as a defect with no filed issue"
                )
            );
        }
        List<Cell> violations = cells.stream().filter(Cell::violation).toList();
        out.add("");
        out.add("cells=" + cells.size() + "  violations=" + violations.size());
        Files.createDirectories(report.getParent());
        Files.writeString(report, String.join("\n", out) + "\n", StandardCharsets.UTF_8);

        if (uncited.isEmpty() == false) {
            StringBuilder message = new StringBuilder(uncited.size() + " exclusion(s) claim a defect with no filed issue:\n");
            for (FixtureExclusions.Exclusion exclusion : uncited) {
                message.append("  ").append(exclusion.suite()).append('.').append(exclusion.caseName()).append('\n');
            }
            message.append("Cite elastic/<repo>#<n>, or re-classify as 'rule:' when no fix is owed.");
            throw new IllegalStateException(message.toString());
        }
        if (violations.isEmpty() == false) {
            StringBuilder message = new StringBuilder("dimension contract audit failed with " + violations.size() + " violation(s):\n");
            for (Cell violation : violations) {
                message.append("  ")
                    .append(violation.dimension())
                    .append('=')
                    .append(violation.value())
                    .append(" on ")
                    .append(violation.format())
                    .append(" -- ")
                    .append(violation.detail())
                    .append('\n');
            }
            message.append("see ").append(report);
            throw new IllegalStateException(message.toString());
        }
    }

    static List<Cell> audit(FixtureDimensions dimensions) {
        List<Cell> cells = new ArrayList<>();
        List<String> formats = dimensions.values("format");
        for (String dimension : dimensions.names()) {
            Set<String> scope = dimensions.appliesTo(dimension);
            for (String value : dimensions.values(dimension)) {
                for (String format : formats) {
                    if (scope.isEmpty() == false && scope.contains(format) == false) {
                        continue;
                    }
                    // The format axis names itself: the cell (format, tsv) only exists on tsv.
                    if (dimension.equals("format") && value.equals(format) == false) {
                        continue;
                    }
                    if (value.equals(dimensions.defaultValue(dimension, format))) {
                        continue;
                    }
                    cells.add(classify(dimensions, dimension, value, format));
                }
            }
        }
        return cells;
    }

    private static Cell classify(FixtureDimensions dimensions, String dimension, String value, String format) {
        String reachable = reachableThrough(dimensions, dimension, value, format);
        String reason = dimensions.absenceReason(dimension, value, format);
        if (reachable != null && reason != null) {
            // Rot direction: the cell became reachable and the licence outlived it. Deleting the entry is
            // the verification that the work landed, so the gate has to notice.
            return new Cell(
                dimension,
                value,
                format,
                "STALE-ABSENCE",
                true,
                "reachable via " + reachable + " but still declares [" + reason + "]; delete the entry"
            );
        }
        if (reachable != null) {
            return new Cell(dimension, value, format, reachable, false, "");
        }
        if (reason != null) {
            String kind = reason.startsWith("rule:") ? "RULE" : "GAP";
            return new Cell(dimension, value, format, kind, false, reason);
        }
        return new Cell(
            dimension,
            value,
            format,
            "UNDECLARED-ABSENCE",
            true,
            "no seam can express it and nothing declares why; add a typed gap. or rule. entry, or wire the seam"
        );
    }

    /** The seam that can make this cell real, or null when none can. */
    private static String reachableThrough(FixtureDimensions dimensions, String dimension, String value, String format) {
        if (dimensions.derivedFrom(dimension) != null || dimensions.derivedFromForValue(dimension, value) != null) {
            return "DERIVED";
        }
        return switch (dimensions.binds(dimension)) {
            case "directive" -> dimensions.directiveKey(dimension) != null ? "DIRECTIVE" : null;
            case "pragma" -> PRAGMA_SEAM_WIRED && dimensions.pragmaKey(dimension) != null ? "PRAGMA" : null;
            case "backend" -> dimensions.backendFor(dimension, value) != null
                ? "BACKEND(" + dimensions.backendFor(dimension, value) + ")"
                : null;
            case "resolver" -> resolverServes(dimension, value, format) ? "RESOLVER" : null;
            // read_key presence is NOT enough: it describes how bytes would announce themselves, not
            // whether anything writes them.
            case "fixture" -> FixtureCapabilities.renders(dimension, value, format) ? "FIXTURE" : null;
            case "cluster" -> null;
            default -> throw new IllegalStateException("unhandled binds [" + dimensions.binds(dimension) + "]");
        };
    }

    private static String cell(String dimension, String value, String format) {
        return dimension + "=" + value + "@" + format;
    }

    /** The distinct dimensions with at least one violating cell, for a caller that wants a summary. */
    static Set<String> violatingDimensions(List<Cell> cells) {
        Set<String> out = new LinkedHashSet<>();
        for (Cell cell : cells) {
            if (cell.violation()) {
                out.add(cell.dimension());
            }
        }
        return out;
    }

    static Map<String, Long> countByVerdict(List<Cell> cells) {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (Cell cell : cells) {
            counts.merge(cell.verdict(), 1L, Long::sum);
        }
        return counts;
    }
}
