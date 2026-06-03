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
 * Shared write-path state for terms-dictionary field writers. Created once per segment by the
 * consumer and passed to {@link TermsDictBlockCodec#createWriter}.
 *
 * @param skipTsidLz4Encoding segment-level toggle for writing the {@code _tsid} terms
 *                              dictionary raw (no LZ4); sourced from
 *                              {@link TSDBDocValuesFormatConfig#skipTsidLz4Encoding()}
 */
public record TermsDictWriteContext(boolean skipTsidLz4Encoding) {}
