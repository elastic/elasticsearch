/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

/**
 * Shared read-path state for terms-dictionary field readers. Created once per segment by the
 * producer and passed to {@link TermsDictBlockCodec#createReader}.
 *
 * @param skipTsidLz4Encoding whether this segment was written with the {@code _tsid} terms
 *                              dictionary stored raw (no LZ4); derived from the on-disk format
 *                              version, so legacy segments report {@code false}
 */
public record TermsDictReadContext(boolean skipTsidLz4Encoding) {}
