/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process.writer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A writer for sending control messages to the a native C++ process.
 */
public abstract class AbstractControlMsgWriter {

    /**
     * This should be the same size as the buffer in the C++ native process.
     */
    public static final int FLUSH_SPACES_LENGTH = 8192;

    protected final LengthEncodedWriter lengthEncodedWriter;
    private final int numberOfFields;

    /**
     * Construct the control message writer with a LengthEncodedWriter
     *
     * @param lengthEncodedWriter The writer
     * @param numberOfFields      The number of fields the process expects in each record
     */
    public AbstractControlMsgWriter(LengthEncodedWriter lengthEncodedWriter, int numberOfFields) {
        this.lengthEncodedWriter = Objects.requireNonNull(lengthEncodedWriter);
        this.numberOfFields = numberOfFields;
    }

    // todo(hendrikm): workaround, see
    // https://github.com/elastic/machine-learning-cpp/issues/123
    protected void fillCommandBuffer() throws IOException {
        char[] spaces = new char[FLUSH_SPACES_LENGTH];
        Arrays.fill(spaces, ' ');
        writeMessage(new String(spaces));
    }

    /**
     * Transform the supplied control message to length encoded values and
     * write to the OutputStream.
     *
     * @param message The control message to write.
     */
    protected void writeMessage(String message) throws IOException {

        lengthEncodedWriter.writeNumFields(numberOfFields);

        // Write blank values for all fields other than the control field
        for (int i = 1; i < numberOfFields; ++i) {
            lengthEncodedWriter.writeField("");
        }

        // The control field comes last
        lengthEncodedWriter.writeField(message);
    }
}
