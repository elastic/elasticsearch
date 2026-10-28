/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.codec.tsdb.NumericFieldWriter;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericBlockEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.IOException;

/**
 * Per-field encoder for the ES95 TSDB format.
 *
 * <p>ES95 differs from ES819 in that the encoding pipeline is selected per field
 * at write time by a {@link org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfigResolver},
 * rather than being fixed for the whole format. Two pieces of output therefore depend on
 * the same per-field resolution:
 * <ul>
 *   <li>the {@link NumericBlockEncoder} that encodes each value block, and</li>
 *   <li>the {@link PipelineDescriptor} written into the field's metadata header so the
 *       reader can reconstruct the matching pipeline at search time.</li>
 * </ul>
 *
 * <p>This class bundles both outputs behind a single per-field handle built from one
 * {@link NumericEncoder} (i.e. one pipeline resolution). It implements
 * {@link NumericFieldWriter.Encoder} so it can be returned from
 * {@code NumericFieldWriter.encoder(FieldContext)} and used to drive per-block encoding,
 * while exposing {@link #descriptor()} to the ES95 writer for the metadata write that
 * follows. Keeping the block encoder and descriptor in lockstep here prevents the field's
 * metadata from drifting from the bytes it describes.
 *
 * <p>ES819 has no equivalent because its pipeline is implicit: there is no per-field
 * descriptor to persist, so a bare {@code Encoder} lambda suffices.
 */
final class ES95NumericFieldEncoder implements NumericFieldWriter.Encoder {

    private final NumericBlockEncoder blockEncoder;
    private final PipelineDescriptor descriptor;

    ES95NumericFieldEncoder(final NumericEncoder numericEncoder) {
        this.blockEncoder = numericEncoder.newBlockEncoder();
        this.descriptor = numericEncoder.descriptor();
    }

    @Override
    public void encodeBlock(final long[] values, final int blockSize, final IndexOutput data) throws IOException {
        blockEncoder.encode(values, blockSize, data);
    }

    PipelineDescriptor descriptor() {
        return descriptor;
    }
}
