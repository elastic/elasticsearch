/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CuVSResources;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;

/**
 * Codec format for GPU-accelerated vector indexes. This format is designed to
 * leverage GPU processing capabilities for vector search operations.
 */
public class GPUVectorsFormat extends KnnVectorsFormat {

    private static final Logger LOG = LogManager.getLogger(GPUVectorsFormat.class);

    public static final String NAME = "GPUVectorsFormat";
    public static final String GPU_IDX_EXTENSION = "gpuidx";
    public static final String GPU_META_EXTENSION = "mgpu";

    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;

    private static final FlatVectorsFormat rawVectorFormat = new Lucene99FlatVectorsFormat(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    public GPUVectorsFormat() {
        super(NAME);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new GPUVectorsWriter(state, rawVectorFormat.fieldsWriter(state));
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new GPUVectorsReader(state, rawVectorFormat.fieldsReader(state));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return 4096;
    }

    @Override
    public String toString() {
        return NAME + "()";
    }

    static GPUVectorsReader getGPUReader(KnnVectorsReader vectorsReader, String fieldName) {
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
            vectorsReader = candidateReader.getFieldReader(fieldName);
        }
        if (vectorsReader instanceof GPUVectorsReader reader) {
            return reader;
        }
        return null;
    }

    /** Tells whether the platform supports cuvs. */
    public static boolean supported() {
        try (var resources = CuVSResources.create()) {
            return true;
        } catch (UnsupportedOperationException uoe) {
            var msg = uoe.getMessage() == null ? "" : ": " + uoe.getMessage();
            LOG.warn("cuvs is not supported on this platform or java version" + msg);
        } catch (Throwable t) {
            if (t instanceof ExceptionInInitializerError ex) {
                t = ex.getCause();
            }
            LOG.warn("Exception occurred during creation of cuvs resources. " + t);
        }
        return false;
    }
}
