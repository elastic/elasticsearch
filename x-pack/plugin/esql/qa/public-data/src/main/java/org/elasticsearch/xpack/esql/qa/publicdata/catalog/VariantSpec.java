/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * One physical incarnation of a corpus: a concrete (provider, format, codec, layout) cell bound to
 * a pinned remote resource. Variant identity is <em>derived</em> from those dimensions (see
 * {@link #label()}), so uniqueness is structural and the JUnit name is self-describing by
 * construction. All variants of a corpus hold the same logical rows and must return identical
 * answers to the corpus's workload.
 *
 * @param corpusId           owning corpus id
 * @param provider           object-store provider
 * @param format             physical format
 * @param codec              compression codec
 * @param layout             physical object layout
 * @param partitioning       {@code none}, {@code hive} or {@code template}
 * @param region             provider region (S3), or null where not applicable (HTTPS)
 * @param resource           the pinned remote resource URI (may contain a glob where supported)
 * @param subResources       optional <em>named</em> fragments of the same corpus, in declaration
 *                           order, addressable from a spec as {@code {{corpus:<name>}}}. Their union
 *                           must be exactly {@link #resource()}'s rows: this is what lets one
 *                           workload read a corpus both as a single dataset and as a multi-source
 *                           {@code FROM d1, ..., dN} union, and assert the two agree. Empty for
 *                           every ordinary variant
 * @param dataSourceSettings settings for the {@code PUT _query/data_source} registration; every S3
 *                           variant must carry {@code auth: anonymous} (validator-enforced) so CI
 *                           agents with instance roles behave identically to a workstation
 * @param datasetSettings    format options merged into the {@code dataset:} directive's WITH JSON
 *                           (e.g. {@code header_row: false} for a headerless CSV)
 * @param datasetMappings    optional declared-schema {@code mappings} block for the dataset PUT
 *                           (order-preserving properties bind text columns positionally; the way a
 *                           headerless CSV leg gets the same column names and types as the
 *                           reference variant)
 * @param pin                metadata pin; required (and non-degenerate) for onboarded variants
 * @param tags               free-form markers: {@code reference} (the variant the oracle read),
 *                           {@code backup} (catalogued but not onboarded)
 * @param querySubset        empty = full workload; otherwise the nightly-budget trim for oversized
 *                           legs (must still cover all four read shapes; validator-enforced)
 * @param notes              human context for reviewers and reports
 * @param expectFailure      non-null only on failure-only corpus variants: querying must fail
 *                           cleanly as declared
 * @param caseId             short kebab-case discriminator for failure variants (e.g.
 *                           {@code zero-byte}): distinct misconfigurations may legitimately share
 *                           one dimension cell, and the label must still be unique
 * @param disabledReason     non-null keeps the variant catalogued but unrun — the failure-variant
 *                           analog of a spec test's {@code -Ignore} + {@code // defect:} block;
 *                           the reason must say why (typically a live defect being triaged)
 */
public record VariantSpec(
    String corpusId,
    Provider provider,
    Format format,
    Codec codec,
    Layout layout,
    String partitioning,
    String region,
    String resource,
    Map<String, String> subResources,
    Map<String, Object> dataSourceSettings,
    Map<String, Object> datasetSettings,
    Map<String, Object> datasetMappings,
    PinSpec pin,
    Set<String> tags,
    Set<String> querySubset,
    String notes,
    FailureSpec expectFailure,
    String caseId,
    String disabledReason
) {

    /** Marker tag for the variant whose bytes the oracle read at authoring time. */
    public static final String TAG_REFERENCE = "reference";
    /** Marker tag for inert catalog entries: written but not onboarded (e.g. HTTPS mirrors). */
    public static final String TAG_BACKUP = "backup";

    /**
     * The derived variant identity: {@code corpusId-provider-format-codec-layout}, plus the
     * {@code case} discriminator on failure variants. Part of the JUnit test name, so a failing
     * leg is greppable straight back to its catalog cell.
     */
    public String label() {
        String base = corpusId + "-" + provider.id() + "-" + format.id() + "-" + codec.id() + "-" + layout.labelId();
        return caseId == null || caseId.isEmpty() ? base : base + "-" + caseId;
    }

    /** The {@code data_source} name to register: one per (provider, region) pair. */
    public String datasetSourceName() {
        String regionPart = region == null || region.isEmpty() ? "default" : region.toLowerCase(Locale.ROOT).replace('-', '_');
        return "pd_ds_" + provider.esType() + "_" + regionPart;
    }

    /** Whether glob/multi-file reads are possible on this variant's provider. */
    public boolean supportsGlob() {
        return provider.supportsGlob();
    }

    /**
     * Resolves a named sub-resource, or throws with the available names — the multi-source
     * counterpart of binding {@code {{corpus}}} to {@link #resource()}.
     */
    public String subResource(String name) {
        String uri = subResources.get(name);
        if (uri == null) {
            throw new IllegalArgumentException(
                "Variant [" + label() + "] has no sub_resource [" + name + "]; declared: " + subResources.keySet()
            );
        }
        return uri;
    }

    /**
     * Whether the object supports random access. Non-seekable legs (gzip text) re-read the whole
     * stream per query; oracle cross-checks and runtime budgets both gate on this.
     */
    public boolean seekable() {
        return format == Format.PARQUET || codec != Codec.GZIP;
    }

    /** Whether this is the variant the oracle read when the expected tables were derived. */
    public boolean isReference() {
        return tags.contains(TAG_REFERENCE);
    }

    /** Whether this entry is inert: catalogued for future activation, never enumerated as a test. */
    public boolean isBackup() {
        return tags.contains(TAG_BACKUP);
    }

    /** Whether this variant runs in the active suite (active provider, not backup, not disabled). */
    public boolean active() {
        return provider.active() && isBackup() == false && disabledReason == null;
    }

    /**
     * The label, not the record dump: variants ride in JUnit test names
     * ({@code q03{corpus-provider-format-codec-layout}}), which must stay greppable. The full cell
     * is recoverable from the catalog by label.
     */
    @Override
    public String toString() {
        return label();
    }
}
