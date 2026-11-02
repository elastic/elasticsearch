/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.FormatVersion;

import java.io.Closeable;
import java.io.IOException;

/**
 * A column's files in a test directory — data, addressing, lengths and navigation — with headers and footers as
 * the format writes them.
 */
public final class ColumnTestFiles {

    // NOTE: the skip file is not named cns, which the numeric tests write themselves beside these files.
    private static final String[] EXTENSIONS = { "cnd", "cna", "cnl", "cnn", "cnk" };
    private static final String CODEC = "ColumNARTestColumn";

    private ColumnTestFiles() {}

    /** A column's outputs, closed with their footers. */
    public static final class Outputs implements Closeable {
        private final ColumnOutputs outputs;
        private final IndexOutput skipIndex;

        private Outputs(ColumnOutputs outputs, IndexOutput skipIndex) {
            this.outputs = outputs;
            this.skipIndex = skipIndex;
        }

        public ColumnOutputs outputs() {
            return outputs;
        }

        public IndexOutput skipIndex() {
            return skipIndex;
        }

        @Override
        public void close() throws IOException {
            boolean success = false;
            try {
                CodecUtil.writeFooter(outputs.data());
                CodecUtil.writeFooter(outputs.addressing());
                CodecUtil.writeFooter(outputs.lengths());
                CodecUtil.writeFooter(outputs.navigation());
                CodecUtil.writeFooter(skipIndex);
                success = true;
            } finally {
                if (success) {
                    IOUtils.close(outputs.data(), outputs.addressing(), outputs.lengths(), outputs.navigation(), skipIndex);
                } else {
                    IOUtils.closeWhileHandlingException(
                        outputs.data(),
                        outputs.addressing(),
                        outputs.lengths(),
                        outputs.navigation(),
                        skipIndex
                    );
                }
            }
        }
    }

    /** A column's inputs, each checksummed whole and its header checked. */
    public static final class Inputs implements Closeable {
        private final ColumnInputs inputs;
        private final IndexInput skipIndex;

        private Inputs(ColumnInputs inputs, IndexInput skipIndex) {
            this.inputs = inputs;
            this.skipIndex = skipIndex;
        }

        public ColumnInputs inputs() {
            return inputs;
        }

        public IndexInput skipIndex() {
            return skipIndex;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(inputs.data(), inputs.addressing(), inputs.lengths(), inputs.navigation(), skipIndex);
        }
    }

    public static Outputs create(Directory dir, String name, byte[] segmentId) throws IOException {
        final IndexOutput[] out = new IndexOutput[EXTENSIONS.length];
        boolean success = false;
        try {
            for (int i = 0; i < out.length; i++) {
                out[i] = dir.createOutput(name + "." + EXTENSIONS[i], IOContext.DEFAULT);
                ColumnarCodecUtil.writeHeader(out[i], CODEC, FormatVersion.CURRENT, segmentId, "");
            }
            success = true;
            return new Outputs(new ColumnOutputs(out[0], out[1], out[2], out[3]), out[4]);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(out);
            }
        }
    }

    /** Bytes the column's files hold together, headers and footers included. */
    public static long length(Directory dir, String name) throws IOException {
        long total = 0;
        for (String extension : EXTENSIONS) {
            total += dir.fileLength(name + "." + extension);
        }
        return total;
    }

    public static Inputs open(Directory dir, String name, byte[] segmentId) throws IOException {
        final IndexInput[] in = new IndexInput[EXTENSIONS.length];
        boolean success = false;
        try {
            for (int i = 0; i < in.length; i++) {
                in[i] = dir.openInput(name + "." + EXTENSIONS[i], IOContext.DEFAULT);
                CodecUtil.checksumEntireFile(in[i]);
                ColumnarCodecUtil.checkHeader(in[i], CODEC, segmentId, "");
            }
            success = true;
            return new Inputs(new ColumnInputs(in[0], in[1], in[2], in[3]), in[4]);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(in);
            }
        }
    }
}
