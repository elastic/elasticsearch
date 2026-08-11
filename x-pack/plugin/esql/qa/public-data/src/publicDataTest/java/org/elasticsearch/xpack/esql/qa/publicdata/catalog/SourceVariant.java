/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

/**
 * One already-public, pinned object (or object glob) that a {@link PublicDataSource}'s csv-spec queries
 * can run against, naming exactly the format/codec/provider/layout/scale combination it exercises so the
 * coverage report (plan section 7) can tally them without guessing. Every field describes reality as the
 * upstream publisher exposes it today; the catalog never repartitions, transcodes, or rewrites a variant
 * into existence (plan section 3).
 *
 * @param id                stable identifier referenced from a spec's {@code // variant: <id>} comment
 *                           and from {@code -Dtests.public_data.variant}; unique within its source
 * @param specResource        the classpath resource of the csv-spec file whose queries/expected-results
 *                            this variant answers (e.g. {@code /specs/clickbench.csv-spec}). Declared per
 *                            variant, not once per source, because two variants of one logical source do
 *                            not always expose the very same rows (e.g. ClickBench's 1M-row single-shard
 *                            Parquet vs. its 5M-row 5-shard glob vs. its 100M-row gzip exports): variants
 *                            that genuinely are the same content in a different format/codec/layout share
 *                            one {@code specResource}; variants that are a different data subset must each
 *                            declare their own, so the checked-in expected results always match what the
 *                            variant actually contains
 * @param format             the on-disk encoding
 * @param codec              the compression codec
 * @param provider           the remote transport
 * @param region             the provider region (e.g. {@code us-east-1} for S3), or {@code null} when the
 *                            provider needs none (plain HTTPS)
 * @param resource            the resource URI the {@code dataset:} directive's {@code {{<sourceId>}}}
 *                            template resolves to for this variant; may be a glob (e.g. ending in
 *                            {@code *.parquet}) or a Hive-partitioned prefix for
 *                            {@link PartitionLayout#HIVE_PARTITIONED}/{@link PartitionLayout#NESTED_HIVE_PARTITIONED}
 * @param pinCheckUri         a single, concrete (non-glob) object URI under {@code resource} that
 *                            {@link org.elasticsearch.xpack.esql.qa.publicdata.PinValidator} HEAD-checks
 *                            against {@code pin} before the suite queries it; equal to {@code resource}
 *                            for a {@link PartitionLayout#SINGLE_FILE} variant
 * @param settingsJson        the {@code WITH {...}} format-options JSON object applied when registering
 *                            the dataset (e.g. {@code header_row}/{@code delimiter} for a headerless CSV),
 *                            or {@code null} for none
 * @param partitionLayout     the physical object layout
 * @param scale               the approximate size class
 * @param pin                 the metadata-only pin captured for {@code pinCheckUri}
 * @param crossValidated      whether this variant's expected results were established by an in-place
 *                            DuckDB/ClickHouse query (plan section 6); {@code false} means the expected
 *                            results instead come from the upstream publisher's documented schema/metadata
 *                            only (e.g. a non-seekable gzip object too large to query in place), and the
 *                            coverage report must surface that gap rather than hide it
 * @param notes                free-text provenance: how the resource/pin were found, and, when
 *                            {@code crossValidated} is {@code false}, why
 */
public record SourceVariant(
    String id,
    String specResource,
    PublicDataFormat format,
    PublicDataCodec codec,
    PublicDataProvider provider,
    String region,
    String resource,
    String pinCheckUri,
    String settingsJson,
    PartitionLayout partitionLayout,
    DataScale scale,
    PinInfo pin,
    boolean crossValidated,
    String notes
) {}
