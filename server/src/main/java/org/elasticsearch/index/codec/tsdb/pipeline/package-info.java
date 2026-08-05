/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Composable encoding pipeline for TSDB numeric doc values.
 *
 * <p>A pipeline is an ordered sequence of transform stages followed by a terminal
 * payload stage. Transform stages (delta, offset, GCD) reduce value entropy;
 * the payload stage (bit-packing) serializes the result.
 *
 * <h2>Stage types</h2>
 * <p>{@link org.elasticsearch.index.codec.tsdb.pipeline.StageSpec} is a sealed hierarchy
 * with two markers:
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.StageSpec.TransformSpec TransformSpec}
 * for chainable transform stages and
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.StageSpec.PayloadSpec PayloadSpec}
 * for terminal stages that serialize values to bytes. Each stage type has a
 * persisted byte identifier ({@link org.elasticsearch.index.codec.tsdb.pipeline.StageId})
 * that serves as the wire-format contract.
 *
 * <h2>Construction vs wire format</h2>
 * <p>{@link org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig} provides a
 * type-safe fluent builder for assembling pipelines at construction time.
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor} is the compact
 * wire-format representation (stage IDs, block size, data type) persisted in the
 * metadata file, enabling per-field codec selection. The two are deliberately
 * separate: {@code PipelineConfig} carries rich stage specifications
 * ({@link org.elasticsearch.index.codec.tsdb.pipeline.StageSpec}) while
 * {@code PipelineDescriptor} stores only the byte identifiers needed for decoding.
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.FieldDescriptor} wraps the
 * descriptor with a format version for segment compatibility.
 *
 * <h2>Encoding and decoding</h2>
 * <p>{@link org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat} is the block-level
 * entry point for writing and reading encoded values.
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.EncodingContext} and
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.DecodingContext} carry per-block
 * state (bitmap, value count) and provide metadata I/O via
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.MetadataWriter} /
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.MetadataReader}, decoupling
 * individual stages from the block layout.
 *
 * <h2>Block layout</h2>
 * <pre>
 *   [bitmap][payload][stage metadata (reverse stage order)]
 * </pre>
 * <p>Metadata is written after the payload so the decoder can read every section
 * in a single forward pass with no seeking or buffering. See
 * {@link org.elasticsearch.index.codec.tsdb.pipeline.BlockFormat} for details.
 *
 * <h2>Usage</h2>
 *
 * <p>Building a pipeline configuration:
 * <pre>{@code
 * PipelineConfig config = PipelineConfig.forLongs(128)
 *     .delta()
 *     .offset()
 *     .gcd()
 *     .bitPack();
 * }</pre>
 */
package org.elasticsearch.index.codec.tsdb.pipeline;
