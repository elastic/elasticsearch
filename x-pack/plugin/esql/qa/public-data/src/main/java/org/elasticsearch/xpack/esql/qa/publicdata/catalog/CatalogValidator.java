/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pure, offline structural validation of the catalog and the shipped workload specs — the single
 * source of truth called by the unit tests, the {@code validatePublicDataCatalog} Gradle task and
 * the parameter factories. Returns human-readable error strings; an empty list means valid.
 *
 * <p>The rules encode the suite's safety and coverage invariants: no {@code file://} anywhere (the
 * suite must be structurally incapable of reading a local copy), anonymous-only S3 (CI instance
 * roles must not change behaviour), metadata pins on every onboarded variant, every uncovered
 * dimension value declared as a gap, all four read shapes per corpus, and no silently disabled or
 * silently trimmed test.
 */
public final class CatalogValidator {

    /** The capability every dataset-backed spec test must declare. */
    public static final String DATASET_CAPABILITY = "dataset_in_from_command";

    static final int DEFAULT_MAX_ROWS = 300;
    static final int ABSOLUTE_MAX_ROWS = 1000;

    private static final Pattern GLOB_METACHARS = Pattern.compile("[*?\\[{]");
    /** The {@code {{corpus:<name>}}} form a multi-source test uses to address one named fragment. */
    private static final Pattern SUB_RESOURCE_TEMPLATE = Pattern.compile("\\{\\{corpus:([a-z0-9_]+)}}");
    private static final Pattern SORT_CLAUSE = Pattern.compile("\\bSORT\\b(?<keys>[^|]*)", Pattern.CASE_INSENSITIVE);

    private CatalogValidator() {}

    /** Validates {@code catalog} against the workload specs, keyed by spec file name. */
    public static List<String> validate(PublicDataCatalog catalog, Map<String, WorkloadSpec> workloads) {
        List<String> errors = new ArrayList<>();
        if (catalog.version() < 1) {
            errors.add("catalog version must be >= 1, got [" + catalog.version() + "]");
        }
        validateCorpora(catalog, workloads, errors);
        validateDimensionCoverage(catalog, errors);
        validateWorkloads(catalog, workloads, errors);
        return List.copyOf(errors);
    }

    private static void validateCorpora(PublicDataCatalog catalog, Map<String, WorkloadSpec> workloads, List<String> errors) {
        Set<String> corpusIds = new HashSet<>();
        Set<String> variantLabels = new HashSet<>();
        Map<String, String> workloadOwners = new HashMap<>();
        for (CorpusSpec corpus : catalog.corpora()) {
            if (corpusIds.add(corpus.id()) == false) {
                errors.add("duplicate corpus id [" + corpus.id() + "]");
            }
            switch (corpus.kind()) {
                case WORKLOAD -> {
                    if (corpus.workload() == null) {
                        errors.add("corpus [" + corpus.id() + "] is kind: workload but declares no workload spec file");
                    } else {
                        String previousOwner = workloadOwners.put(corpus.workload(), corpus.id());
                        if (previousOwner != null) {
                            errors.add(
                                "workload [" + corpus.workload() + "] is claimed by both [" + previousOwner + "] and [" + corpus.id() + "]"
                            );
                        }
                        if (workloads.containsKey(corpus.workload()) == false) {
                            errors.add("corpus [" + corpus.id() + "] declares workload [" + corpus.workload() + "] but no such spec file");
                        }
                    }
                    corpus.variants()
                        .stream()
                        .filter(v -> v.expectFailure() != null)
                        .forEach(
                            v -> errors.add(
                                "variant [" + v.label() + "] declares expect_failure but corpus [" + corpus.id() + "] is kind: workload"
                            )
                        );
                }
                case FAILURE_ONLY -> {
                    if (corpus.workload() != null) {
                        errors.add("corpus [" + corpus.id() + "] is kind: failure-only but declares a workload spec file");
                    }
                    if (corpus.variants().stream().noneMatch(v -> v.expectFailure() != null)) {
                        errors.add("corpus [" + corpus.id() + "] is kind: failure-only but declares no expect_failure variant");
                    }
                }
            }
            for (VariantSpec variant : corpus.variants()) {
                validateVariant(corpus, variant, variantLabels, errors);
            }
        }
        for (String specFile : workloads.keySet()) {
            if (workloadOwners.containsKey(specFile) == false) {
                errors.add("spec file [" + specFile + "] is not claimed by any corpus in the catalog");
            }
        }
    }

    private static void validateVariant(CorpusSpec corpus, VariantSpec variant, Set<String> variantLabels, List<String> errors) {
        String label = variant.label();
        if (variantLabels.add(label) == false) {
            errors.add("duplicate variant label [" + label + "] (variant identity is derived; adjust the dimensions)");
        }
        // a resource is one string, but the engine accepts a comma-separated list of full URIs
        // (its multi-location form): every entry must carry the provider's scheme
        String[] locations = variant.resource().split(",");
        for (String location : locations) {
            if (variant.provider().matchesScheme(location.trim()) == false) {
                errors.add(
                    "variant [" + label + "] resource entry [" + location.trim() + "] does not match provider [" + variant.provider() + "]"
                );
            }
        }
        checkNoFileScheme(label, variant.resource(), errors);
        variant.dataSourceSettings().values().forEach(v -> checkNoFileScheme(label, String.valueOf(v), errors));
        variant.datasetSettings().values().forEach(v -> checkNoFileScheme(label, String.valueOf(v), errors));
        boolean hasGlob = GLOB_METACHARS.matcher(variant.resource()).find();
        if (hasGlob && variant.supportsGlob() == false) {
            errors.add("variant [" + label + "] uses a glob but provider [" + variant.provider() + "] cannot list objects");
        }
        if (variant.layout().multiFile() && hasGlob == false && locations.length < 2 && "none".equals(variant.partitioning())) {
            errors.add("variant [" + label + "] declares a multi-file layout but neither a glob, a comma-list, nor partitioning");
        }
        if (variant.provider() == Provider.S3 && "anonymous".equals(variant.dataSourceSettings().get("auth")) == false) {
            errors.add(
                "variant ["
                    + label
                    + "] is on S3 but data_source_settings.auth is not [anonymous]; CI agents carrying instance"
                    + " roles would send signed requests and behave differently from a workstation"
            );
        }
        if (variant.isBackup() == false) {
            // expect_failure variants may point at deliberately nonexistent resources, which have
            // no metadata to pin; when they do pin something real, it must still be sound
            if (variant.pin() == null) {
                if (variant.expectFailure() == null) {
                    errors.add("variant [" + label + "] has no pin: block; onboarded variants must be pinned by HEAD/LIST metadata");
                }
            } else if (variant.pin().degenerate()) {
                errors.add("variant [" + label + "] has a degenerate pin (zero objects, or no samples/verified_at)");
            } else if (variant.pin().isVolatile()) {
                // A volatile pin silences ETag drift, so it must never be reachable by accident:
                // it needs a written reason, and an expected table frozen against moving bytes
                // would be a slow-motion false alarm, so the corpus must assert invariants instead.
                if (variant.notes() == null || variant.notes().isBlank()) {
                    errors.add("variant [" + label + "] has a volatile pin but no notes: justifying why the bytes move");
                }
                if (corpus.assertionMode() != CorpusSpec.AssertionMode.INVARIANT) {
                    errors.add(
                        "variant ["
                            + label
                            + "] has a volatile pin but corpus ["
                            + corpus.id()
                            + "] is assertion_mode exact; bytes that move nightly cannot carry frozen expected tables"
                    );
                }
                if (variant.pin().sizeTolerancePercent() <= 0 || variant.pin().sizeTolerancePercent() >= 100) {
                    errors.add("variant [" + label + "] volatile pin size_tolerance_pct must be in (0, 100)");
                }
            }
        }
        validateSubResources(variant, label, errors);
        validateExtensions(corpus, variant, label, hasGlob, errors);
        if (variant.querySubset().isEmpty() == false && corpus.workload() == null) {
            errors.add("variant [" + label + "] declares a query_subset but its corpus has no workload");
        }
    }

    /**
     * Named fragments obey exactly the rules their parent resource does — same provider scheme, no
     * {@code file://}, globs only where the provider can list. A fragment that slipped past these
     * would be an unreviewed second way into the object store.
     */
    private static void validateSubResources(VariantSpec variant, String label, List<String> errors) {
        variant.subResources().forEach((name, uri) -> {
            if (name.matches("[a-z0-9_]+") == false) {
                errors.add("variant [" + label + "] sub_resource name [" + name + "] must be lower-case alphanumeric with underscores");
            }
            checkNoFileScheme(label, uri, errors);
            for (String location : uri.split(",")) {
                if (variant.provider().matchesScheme(location.trim()) == false) {
                    errors.add(
                        "variant ["
                            + label
                            + "] sub_resource ["
                            + name
                            + "] entry ["
                            + location.trim()
                            + "] does not match provider ["
                            + variant.provider()
                            + "]"
                    );
                }
            }
            if (GLOB_METACHARS.matcher(uri).find() && variant.supportsGlob() == false) {
                errors.add(
                    "variant [" + label + "] sub_resource [" + name + "] uses a glob but provider [" + variant.provider() + "] cannot list"
                );
            }
        });
    }

    private static void validateExtensions(CorpusSpec corpus, VariantSpec variant, String label, boolean hasGlob, List<String> errors) {
        // Deliberately mislabeled/mispointed configurations are exempt: pointing a wrong format or
        // codec at a real object is exactly what the failure-only corpus does.
        if (corpus.quality() == DataQuality.MISLABELED || corpus.quality() == DataQuality.MISPOINTED || variant.expectFailure() != null) {
            return;
        }
        if (hasGlob) {
            return;
        }
        String resource = variant.resource();
        if (variant.format() == Format.PARQUET) {
            // container formats compress internally (snappy/zstd/gzip column chunks); the codec
            // never appears as a file suffix, so suffix consistency is not checkable here
        } else if (variant.codec() == Codec.UNCOMPRESSED) {
            for (Codec codec : Codec.values()) {
                if (codec != Codec.UNCOMPRESSED && codec.matchesExtension(resource)) {
                    errors.add("variant [" + label + "] declares codec uncompressed but the resource carries a " + codec.id() + " suffix");
                }
            }
        } else if (variant.codec() != Codec.SNAPPY && variant.codec().matchesExtension(resource) == false) {
            // snappy is typically internal and carries no file suffix
            errors.add("variant [" + label + "] declares codec " + variant.codec().id() + " but the resource lacks its suffix");
        }
        String withoutCodec = variant.codec().stripExtension(resource);
        for (Format format : Format.values()) {
            if (format != variant.format()
                && format.matchesExtension(withoutCodec)
                && variant.format().matchesExtension(withoutCodec) == false) {
                errors.add(
                    "variant [" + label + "] declares format " + variant.format().id() + " but the resource looks like " + format.id()
                );
            }
        }
    }

    /**
     * Every provider/format/codec/layout value not exercised by an active variant must be a
     * declared gap ({@code dimension=value} cell) — so no hole in the matrix is silent. Structural
     * impossibilities (glob layouts on HTTPS) are per-combination and derived by the coverage
     * inventory as {@code blocked}; at the whole-dimension-value level a gap declaration is
     * required.
     */
    private static void validateDimensionCoverage(PublicDataCatalog catalog, List<String> errors) {
        Set<String> declaredCells = new HashSet<>();
        for (GapSpec gap : catalog.gaps()) {
            declaredCells.addAll(gap.cells().stream().map(c -> c.toLowerCase(Locale.ROOT)).toList());
        }
        List<VariantSpec> active = catalog.corpora().stream().flatMap(c -> c.activeVariants().stream()).toList();
        for (Provider provider : Provider.values()) {
            boolean covered = active.stream().anyMatch(v -> v.provider() == provider);
            requireCoveredOrGap(covered, "provider", provider.id(), declaredCells, errors);
        }
        for (Format format : Format.values()) {
            boolean covered = active.stream().anyMatch(v -> v.format() == format);
            requireCoveredOrGap(covered, "format", format.id(), declaredCells, errors);
        }
        for (Codec codec : Codec.values()) {
            boolean covered = active.stream().anyMatch(v -> v.codec() == codec);
            requireCoveredOrGap(covered, "codec", codec.id(), declaredCells, errors);
        }
        for (Layout layout : Layout.values()) {
            boolean covered = active.stream().anyMatch(v -> v.layout() == layout);
            requireCoveredOrGap(covered, "layout", layout.name().toLowerCase(Locale.ROOT), declaredCells, errors);
        }
    }

    private static void requireCoveredOrGap(boolean covered, String dimension, String value, Set<String> cells, List<String> errors) {
        if (covered == false && cells.contains(dimension + "=" + value) == false) {
            errors.add("matrix cell [" + dimension + "=" + value + "] is neither covered by an active variant nor a declared gap");
        }
    }

    private static void validateWorkloads(PublicDataCatalog catalog, Map<String, WorkloadSpec> workloads, List<String> errors) {
        Map<String, String> testNameToFile = new HashMap<>();
        for (CorpusSpec corpus : catalog.corpora()) {
            if (corpus.kind() != CorpusSpec.Kind.WORKLOAD || corpus.workload() == null) {
                continue;
            }
            WorkloadSpec workload = workloads.get(corpus.workload());
            if (workload == null) {
                continue; // missing spec already reported
            }
            validateWorkload(corpus, workload, testNameToFile, errors);
            validateQuerySubsets(corpus, workload, errors);
        }
    }

    private static void validateWorkload(
        CorpusSpec corpus,
        WorkloadSpec workload,
        Map<String, String> testNameToFile,
        List<String> errors
    ) {
        String file = workload.fileName();
        if (workload.tests().isEmpty()) {
            errors.add("workload [" + file + "] contains no tests");
        }
        Set<ReadShape> shapes = new HashSet<>();
        for (WorkloadSpec.TestSpec test : workload.tests()) {
            String where = file + ":" + test.lineNumber() + " [" + test.name() + "]";
            String previous = testNameToFile.put(test.baseName().toLowerCase(Locale.ROOT), file);
            if (previous != null) {
                errors.add(where + " duplicates a test name already used in [" + previous + "] (names must be unique suite-wide)");
            }
            validateDatasetDirectives(corpus, test, where, errors);
            if (test.requiredCapabilities().contains(DATASET_CAPABILITY) == false) {
                errors.add(where + " must declare required_capability: " + DATASET_CAPABILITY);
            }
            if (test.query().toLowerCase(Locale.ROOT).contains("file://")) {
                errors.add(where + " query contains a file:// URI");
            }
            validateProvenance(corpus, test, where, errors);
            if (test.readShape() != null) {
                try {
                    shapes.add(ReadShape.fromId(test.readShape()));
                } catch (IllegalArgumentException e) {
                    // reported by validateProvenance
                }
            }
            validateDeterminism(test, where, errors);
        }
        for (ReadShape shape : ReadShape.values()) {
            if (shapes.contains(shape) == false) {
                errors.add("workload [" + file + "] does not cover read shape [" + shape.id() + "]; all four are mandatory per corpus");
            }
        }
    }

    /**
     * Every {@code dataset:} directive must bind a template, never a literal resource: {@code
     * {{corpus}}} for the whole corpus, or {@code {{corpus:<name>}}} for one of the variant's
     * declared {@code sub_resources}. More than one directive is the multi-source
     * {@code FROM d1, ..., dN} shape, and then each must name a <em>distinct</em> sub-resource that
     * every active variant of the corpus actually declares — otherwise the test would silently read
     * the same location N times, or fail only on the leg that is missing the name.
     */
    private static void validateDatasetDirectives(CorpusSpec corpus, WorkloadSpec.TestSpec test, String where, List<String> errors) {
        List<String> directives = test.datasetDirectives();
        if (directives.isEmpty()) {
            errors.add(where + " must carry at least one dataset: directive");
            return;
        }
        Set<String> names = new HashSet<>();
        boolean multiSource = directives.size() > 1;
        for (String directive : directives) {
            Matcher matcher = SUB_RESOURCE_TEMPLATE.matcher(directive);
            if (matcher.find()) {
                String name = matcher.group(1);
                if (names.add(name) == false) {
                    errors.add(where + " binds sub_resource [" + name + "] more than once");
                }
                for (VariantSpec variant : corpus.variants()) {
                    if (variant.active() && variant.subResources().containsKey(name) == false) {
                        errors.add(
                            where + " binds {{corpus:" + name + "}} but variant [" + variant.label() + "] declares no such sub_resource"
                        );
                    }
                }
            } else if (directive.contains("{{corpus}}")) {
                if (multiSource) {
                    errors.add(
                        where + " is multi-source but binds the whole-corpus {{corpus}} template; use {{corpus:<name>}} per dataset"
                    );
                }
            } else {
                errors.add(where + " dataset: directive must bind {{corpus}} or {{corpus:<name>}}, not a literal resource");
            }
        }
    }

    private static void validateProvenance(CorpusSpec corpus, WorkloadSpec.TestSpec test, String where, List<String> errors) {
        Map<String, String> provenance = test.provenance();
        if (test.disabled()) {
            if (provenance.containsKey("defect") == false && provenance.containsKey("disabled") == false) {
                errors.add(where + " is -Ignore'd without a // defect: block or an explicit // disabled: reason");
            }
        }
        if (corpus.assertionMode() == CorpusSpec.AssertionMode.INVARIANT) {
            // An invariant claims less than a frozen table, so it must say so and show its work:
            // the value actually observed at authoring time is what lets a reviewer see the
            // threshold is tight enough to catch a truncated read rather than vacuously true.
            if ("invariant".equals(provenance.get("assertion-mode")) == false) {
                errors.add(where + " is in an invariant corpus and must carry // assertion-mode: invariant");
            }
            String observed = provenance.get("oracle-observed");
            if (observed == null || observed.isBlank()) {
                errors.add(where + " must carry // oracle-observed: <the value measured at authoring time>");
            }
        } else if (provenance.containsKey("assertion-mode")) {
            errors.add(where + " declares an assertion-mode but corpus [" + corpus.id() + "] is assertion_mode exact");
        }
        String declaredCorpus = provenance.get("corpus");
        if (declaredCorpus == null || declaredCorpus.equals(corpus.id()) == false) {
            errors.add(where + " must carry // corpus: " + corpus.id() + " (found [" + declaredCorpus + "])");
        }
        if (provenance.getOrDefault("oracle", "").isEmpty()) {
            errors.add(where + " must carry // oracle: <name and version> provenance");
        }
        if (provenance.getOrDefault("oracle-sql", "").isEmpty()) {
            errors.add(where + " must carry non-empty // oracle-sql: provenance");
        }
        String referenceVariant = provenance.get("reference-variant");
        if (referenceVariant == null) {
            errors.add(where + " must carry // reference-variant: provenance");
        } else {
            boolean resolves = corpus.variants().stream().anyMatch(v -> v.label().equals(referenceVariant) && v.isReference());
            if (resolves == false) {
                errors.add(where + " reference-variant [" + referenceVariant + "] does not resolve to a variant tagged reference");
            }
        }
        String readShape = test.readShape();
        if (readShape == null) {
            errors.add(where + " must carry // read-shape: provenance");
        } else {
            try {
                ReadShape.fromId(readShape);
            } catch (IllegalArgumentException e) {
                errors.add(where + " read-shape [" + readShape + "] is not one of scan, aggregate, topn, limit");
            }
        }
        String maxRows = test.maxRows();
        if (maxRows == null) {
            errors.add(where + " must carry // max-rows: provenance");
        } else {
            try {
                int limit = Integer.parseInt(maxRows);
                if (limit > ABSOLUTE_MAX_ROWS) {
                    errors.add(where + " max-rows [" + limit + "] exceeds the absolute cap of " + ABSOLUTE_MAX_ROWS);
                } else if (limit > DEFAULT_MAX_ROWS && provenance.containsKey("max-rows-reason") == false) {
                    errors.add(where + " max-rows [" + limit + "] exceeds the default cap of " + DEFAULT_MAX_ROWS + " without a reason");
                }
                if (test.expectedRowCount() > limit) {
                    errors.add(where + " expected table has " + test.expectedRowCount() + " rows, above its max-rows [" + limit + "]");
                }
            } catch (NumberFormatException e) {
                errors.add(where + " max-rows [" + maxRows + "] is not an integer");
            }
        }
    }

    /**
     * Any multi-row expected table must be deterministic: a {@code SORT} with a tie-breaker (at
     * least two sort keys), or an explicit human-reviewed {@code // sort-reviewed:} marker stating
     * why the order is already total.
     */
    private static void validateDeterminism(WorkloadSpec.TestSpec test, String where, List<String> errors) {
        if (test.expectedRowCount() <= 1 || test.provenance().containsKey("sort-reviewed")) {
            return;
        }
        Matcher sort = SORT_CLAUSE.matcher(test.query());
        boolean deterministic = false;
        while (sort.find()) {
            if (sort.group("keys").split(",").length >= 2) {
                deterministic = true;
            }
        }
        if (deterministic == false) {
            errors.add(
                where
                    + " has a multi-row expected table but no SORT with a tie-breaker; add one or, if the order is already"
                    + " total, an explicit // sort-reviewed: reason"
            );
        }
    }

    private static void validateQuerySubsets(CorpusSpec corpus, WorkloadSpec workload, List<String> errors) {
        Set<String> testNames = new HashSet<>();
        Map<String, ReadShape> shapeByTest = new HashMap<>();
        for (WorkloadSpec.TestSpec test : workload.tests()) {
            testNames.add(test.baseName());
            if (test.readShape() != null) {
                try {
                    shapeByTest.put(test.baseName(), ReadShape.fromId(test.readShape()));
                } catch (IllegalArgumentException e) {
                    // already reported
                }
            }
        }
        for (VariantSpec variant : corpus.variants()) {
            if (variant.querySubset().isEmpty()) {
                continue;
            }
            Set<ReadShape> subsetShapes = new HashSet<>();
            for (String entry : variant.querySubset()) {
                if (testNames.contains(entry) == false) {
                    errors.add("variant [" + variant.label() + "] query_subset entry [" + entry + "] is not a test in the workload");
                } else if (shapeByTest.containsKey(entry)) {
                    subsetShapes.add(shapeByTest.get(entry));
                }
            }
            for (ReadShape shape : ReadShape.values()) {
                if (subsetShapes.contains(shape) == false) {
                    errors.add(
                        "variant ["
                            + variant.label()
                            + "] query_subset does not cover read shape ["
                            + shape.id()
                            + "]; trimmed legs must still cover all four"
                    );
                }
            }
        }
    }

    private static void checkNoFileScheme(String label, String value, List<String> errors) {
        if (value != null && value.toLowerCase(Locale.ROOT).contains("file://")) {
            errors.add("variant [" + label + "] contains a file:// URI [" + value + "]; the suite must never read local files");
        }
    }
}
