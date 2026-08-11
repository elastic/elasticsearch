/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata.catalog;

/**
 * Metadata-only pin captured for a {@link SourceVariant}'s {@code pinCheckUri}. For the default
 * {@link PinStrategy#ETAG} strategy this is captured via an HTTP {@code HEAD} (or, for an {@code s3://}
 * resource, the equivalent virtual-hosted-style HTTPS {@code HEAD}) -- never a body fetch --, and
 * {@link org.elasticsearch.xpack.esql.qa.publicdata.PinValidator} re-issues the same {@code HEAD} at suite
 * start, failing loudly if the live {@code etag}/{@code sizeBytes} no longer match, which is the guard
 * against a mutable upstream object silently drifting out from under a checked-in expected result (plan
 * section 3).
 * <p>
 * For {@link PinStrategy#CONTENT_SIGNATURE}, {@code etag}/{@code sizeBytes} are recorded for provenance
 * only (the upstream publisher wholesale-regenerates the object on a schedule, so they are expected to
 * drift) and are not asserted against a live {@code HEAD}; {@link #contentSignature} documents the small
 * content fingerprint captured instead, per {@link PinStrategy#CONTENT_SIGNATURE}'s Javadoc.
 *
 * @param etag             the upstream {@code ETag} response header, quotes included, exactly as observed
 *                          at {@code capturedAt}
 * @param sizeBytes        the upstream {@code Content-Length}, in bytes, as observed at {@code capturedAt}
 * @param capturedAt       the ISO-8601 UTC instant this pin was captured, for provenance
 * @param objectCount      for a multi-object resource (a glob or Hive-partitioned prefix), the number of
 *                         objects the upstream listing reported at capture time, or {@code null} for a
 *                         single-object resource; documentation-only -- not re-verified live, since doing
 *                         so for every provider would require a full listing API per provider rather than
 *                         a single {@code HEAD}
 * @param strategy         how this pin is re-verified; see {@link PinStrategy}
 * @param contentSignature required, non-blank, when {@code strategy} is {@link PinStrategy#CONTENT_SIGNATURE};
 *                         {@code null} for {@link PinStrategy#ETAG}. A short human-readable content
 *                         fingerprint (e.g. {@code "rows=4617295;avg_tmax_c=17.43;stations=6920"}) computed
 *                         once via a bounded, in-memory, never-persisted DuckDB/ClickHouse read
 */
public record PinInfo(String etag, long sizeBytes, String capturedAt, Integer objectCount, PinStrategy strategy, String contentSignature) {}
