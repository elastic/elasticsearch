/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.FieldInfo;

import java.io.IOException;

/**
 * Writes the ordinal stream of a multi-valued {@code SortedSet} field to a segment.
 *
 * <p>Created by {@link SortedSetOrdinalCodec#createWriter(NumericWriteContext)} once per field; the shared
 * consumer wire-format code drives {@link #writeOrdinals} to emit the per-field metadata and the
 * encoded ordinal stream. Mirrors {@link SortedOrdinalWriter}, but the stream carries multiple ordinals
 * per document, so the writer also owns however each document's slice of it is delimited: the blocked
 * layout appends a per-doc addresses table through {@link MultiValueAddresses}, while a layout whose
 * slices are self-delimiting writes nothing more. The consumer does not know which it is.
 */
public interface SortedSetOrdinalWriter {

    /**
     * Writes one sorted-set ordinal field and returns its statistics.
     *
     * @param field  field being written
     * @param values source of doc values for this field
     * @param maxOrd maximum ordinal value for this field
     * @return the field's doc value count statistics
     */
    DocValueFieldCountStats writeOrdinals(FieldInfo field, TsdbDocValuesProducer values, long maxOrd) throws IOException;
}
