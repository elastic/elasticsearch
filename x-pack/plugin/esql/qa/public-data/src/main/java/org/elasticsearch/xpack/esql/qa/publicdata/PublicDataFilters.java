/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata;

import org.elasticsearch.xpack.esql.qa.publicdata.catalog.CorpusSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The {@code -Dtests.public_data.*} filter set. Filtering happens at parameter-factory time, so
 * filtered-out variants never appear as skipped tests — and an empty filter result fails loudly
 * with the list of available labels, because a silent zero-test run is the classic failure mode of
 * filtered suites.
 *
 * @param source            corpus id filter
 * @param spec              workload spec file filter (with or without the {@code .csv-spec} suffix)
 * @param variant           glob over variant labels, e.g. {@code *-s3-parquet-*}
 * @param provider          provider id filter
 * @param format            format id filter
 * @param codec             codec id filter
 * @param layout            layout id filter
 * @param shape             read-shape filter (applies to tests via their {@code // read-shape:})
 * @param scale             corpus scale filter
 * @param quality           corpus quality filter
 * @param record            capture actual results as a mismatch diagnostic
 * @param maxVariantsPerSpec cap on variants enumerated per workload (0 = unlimited)
 * @param maxRetries        bounded retries for transient remote failures
 * @param outputDir         where recorded fragments and reports go
 * @param heap              cluster node heap, e.g. {@code 8g}
 */
public record PublicDataFilters(
    String source,
    String spec,
    String variant,
    String provider,
    String format,
    String codec,
    String layout,
    String shape,
    String scale,
    String quality,
    boolean record,
    int maxVariantsPerSpec,
    int maxRetries,
    String outputDir,
    String heap
) {

    public static final String PROP_PREFIX = "tests.public_data.";

    /** Reads the filter set from system properties. */
    public static PublicDataFilters fromSystemProperties() {
        return new PublicDataFilters(
            property("source"),
            property("spec"),
            property("variant"),
            property("provider"),
            property("format"),
            property("codec"),
            property("layout"),
            property("shape"),
            property("scale"),
            property("quality"),
            Boolean.parseBoolean(System.getProperty(PROP_PREFIX + "record", "false")),
            Integer.parseInt(System.getProperty(PROP_PREFIX + "max_variants_per_spec", "0")),
            Integer.parseInt(System.getProperty(PROP_PREFIX + "max_retries", "3")),
            System.getProperty(PROP_PREFIX + "output_dir", "build/public-data-results"),
            System.getProperty(PROP_PREFIX + "heap", "8g")
        );
    }

    private static String property(String name) {
        String value = System.getProperty(PROP_PREFIX + name);
        return value == null || value.isEmpty() ? null : value.toLowerCase(Locale.ROOT);
    }

    /** Whether the corpus passes the corpus-level filters. */
    public boolean matches(CorpusSpec corpus) {
        if (source != null && corpus.id().toLowerCase(Locale.ROOT).equals(source) == false) {
            return false;
        }
        if (spec != null && corpus.workload() != null) {
            String bare = corpus.workload().replace(".csv-spec", "").toLowerCase(Locale.ROOT);
            if (bare.equals(spec.replace(".csv-spec", "")) == false) {
                return false;
            }
        } else if (spec != null) {
            return false;
        }
        if (scale != null && corpus.scale().id().equals(scale) == false) {
            return false;
        }
        return quality == null || corpus.quality().id().equals(quality);
    }

    /** Whether the variant passes the variant-level filters. */
    public boolean matches(VariantSpec variantSpec) {
        if (provider != null && variantSpec.provider().id().equals(provider) == false) {
            return false;
        }
        if (format != null && variantSpec.format().id().equals(format) == false) {
            return false;
        }
        if (codec != null && variantSpec.codec().id().equals(codec) == false) {
            return false;
        }
        if (layout != null
            && variantSpec.layout().labelId().equals(layout) == false
            && variantSpec.layout().name().toLowerCase(Locale.ROOT).equals(layout) == false) {
            return false;
        }
        return variant == null || globToPattern(variant).matcher(variantSpec.label().toLowerCase(Locale.ROOT)).matches();
    }

    /** The active variants of {@code corpus} that pass the filters, capped by maxVariantsPerSpec. */
    public List<VariantSpec> variants(CorpusSpec corpus) {
        if (matches(corpus) == false) {
            return List.of();
        }
        List<VariantSpec> matching = corpus.activeVariants().stream().filter(v -> v.expectFailure() == null).filter(this::matches).toList();
        if (maxVariantsPerSpec > 0 && matching.size() > maxVariantsPerSpec) {
            return matching.subList(0, maxVariantsPerSpec);
        }
        return matching;
    }

    /**
     * Fails loudly when filtering produced zero tests, listing what is available. A silent
     * zero-test run either vanishes in CI or (locally) surfaces as the misleading
     * "No tests found for given includes" — neither is acceptable for a correctness suite.
     */
    public <T> void failIfEmpty(List<T> parameters, PublicDataCatalog catalog) {
        if (parameters.isEmpty() == false) {
            return;
        }
        String labels = catalog.corpora()
            .stream()
            .flatMap(corpus -> corpus.activeVariants().stream())
            .map(VariantSpec::label)
            .collect(Collectors.joining("\n  "));
        throw new IllegalArgumentException(
            "The -Dtests.public_data.* filters matched no tests.\nActive filters: "
                + describe()
                + "\nAvailable variant labels:\n  "
                + labels
        );
    }

    /** Human-readable listing of the non-default filters, for the fail-loudly message. */
    public String describe() {
        Map<String, String> set = Map.of(
            "source",
            String.valueOf(source),
            "spec",
            String.valueOf(spec),
            "variant",
            String.valueOf(variant),
            "provider",
            String.valueOf(provider),
            "format",
            String.valueOf(format),
            "codec",
            String.valueOf(codec),
            "layout",
            String.valueOf(layout),
            "scale",
            String.valueOf(scale),
            "quality",
            String.valueOf(quality)
        );
        String described = set.entrySet()
            .stream()
            .filter(e -> "null".equals(e.getValue()) == false)
            .map(e -> e.getKey() + "=" + e.getValue())
            .sorted()
            .collect(Collectors.joining(", "));
        return described.isEmpty() ? "(none)" : described;
    }

    static Pattern globToPattern(String glob) {
        StringBuilder regex = new StringBuilder();
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append('.');
                default -> regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return Pattern.compile(regex.toString());
    }

    /** Applies a variant's {@code querySubset} trim: test names not in the subset are dropped. */
    public static <T> List<T> applyQuerySubset(VariantSpec variantSpec, List<T> tests, Function<T, String> testName) {
        if (variantSpec.querySubset().isEmpty()) {
            return tests;
        }
        return tests.stream().filter(t -> variantSpec.querySubset().contains(testName.apply(t))).toList();
    }
}
