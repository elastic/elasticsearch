/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Statistical models used by disk BBQ IVF auto-calibration at merge time.
 *
 * <p>When {@code auto_calibrate} is enabled on {@code bbq_disk} index options, the codec
 * {@link org.elasticsearch.index.codec.vectors.diskbbq.next.IvfAutoCalibration}
 * runs on merged vectors to choose quantization encoding, preconditioning, and rescore oversample.
 * Classes in this package implement the numerical models; they do not perform I/O beyond
 * {@link org.apache.lucene.index.FloatVectorValues} access.
 *
 * <h2>Calibration pipeline</h2>
 * <ol>
 *   <li>{@link CalibrationUtils} samples disjoint query and corpus ordinals from merged vectors.</li>
 *   <li>{@link CalibrationQueries} materializes queries on demand (cosine normalization,
 *       optional Neyshabur lift, optional orthogonal preconditioning).</li>
 *   <li>{@link ManifoldModel} fits a log-linear rank–distance model over the corpus sample.</li>
 *   <li>{@link ErrorModel} fits quantization-error scaling and magnitude models via
 *       hierarchical k-means sweeps and {@link Regression} OLS.</li>
 *   <li>{@link ExpectedRecall} estimates recall@k for candidate encodings and rerank depths.</li>
 * </ol>
 *
 * <h2>Caller requirements</h2>
 * <p>Callers must supply a {@link org.apache.lucene.index.FloatVectorValues} implementation that supports
 * random access via {@link org.apache.lucene.index.FloatVectorValues#vectorValue(int)} (typically a
 * merge-time spill from {@link org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsWriter}).
 * Calibration runs on the merge thread and is not designed for concurrent access to the same vectors.
 */
package org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate;
