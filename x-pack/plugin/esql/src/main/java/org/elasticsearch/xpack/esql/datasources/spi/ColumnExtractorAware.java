/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Capability marker for {@link FormatReader} implementations that can re-read columns positionally
 * after the primary forward scan.
 * <p>
 * Presence of this interface is the capability signal: planners use {@code instanceof
 * ColumnExtractorAware} rather than a separate flag. Readers without cheap random access simply
 * omit it. This stays orthogonal to {@link FormatReader#filterPushdownSupport()},
 * {@link FormatReader#aggregatePushdownSupport()}, and {@link FormatReader#withPushedFilter(Object)}
 * — deferred extraction is an additional optional path layered on the same configured reader.
 * <p>
 * When supported, the planner narrows the scan for {@code SORT … | LIMIT}-style queries and relies on
 * {@code InsertExternalFieldExtraction} ({@code
 * org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertExternalFieldExtraction}) plus
 * {@code SourceExtractors} ({@code org.elasticsearch.xpack.esql.datasources.SourceExtractors}) to
 * reload deferred fields above TopN.
 * <p>
 * The runtime handshake — actually producing a {@link ColumnExtractor} matched to a specific
 * forward-scan iterator — is expressed by {@link ColumnExtractorProducer}, which the reader's
 * iterator implements when it emits {@link ColumnExtractor#ROW_POSITION_COLUMN}.
 */
public interface ColumnExtractorAware {}
