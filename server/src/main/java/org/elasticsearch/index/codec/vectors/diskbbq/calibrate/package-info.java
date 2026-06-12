/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Utilities and statistical models for disk BBQ IVF auto-calibration at merge time.
 *
 * <p>When {@code auto_calibrate} is enabled on {@code bbq_disk} index options, the codec
 * {@link org.elasticsearch.index.codec.vectors.diskbbq.IvfAutoCalibration} chooses quantization
 * encoding, preconditioning, and rescore oversample per segment. Classes in this package support
 * that pipeline; they do not perform I/O beyond {@link org.apache.lucene.index.FloatVectorValues}
 * access.
 *
 * <p>{@link CalibrationUtils} provides vector sampling and math helpers used by later calibration
 * models.
 */
package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;
