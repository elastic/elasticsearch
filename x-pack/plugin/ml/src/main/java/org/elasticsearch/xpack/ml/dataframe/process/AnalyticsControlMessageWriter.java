/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.xpack.ml.process.writer.AbstractControlMsgWriter;
import org.elasticsearch.xpack.ml.process.writer.LengthEncodedWriter;

import java.io.IOException;

public class AnalyticsControlMessageWriter extends AbstractControlMsgWriter {

    /**
     * This must match the code defined in the api::CDataFrameAnalyzer C++ class.
     * The constant there is referred as RUN_ANALYSIS_CONTROL_MESSAGE_FIELD_VALUE
     * but in the context of the java side it is more descriptive to call this the
     * end of data message.
     */
    private static final String END_OF_DATA_MESSAGE_CODE = "$";

    /**
     * Construct the control message writer with a LengthEncodedWriter
     *
     * @param lengthEncodedWriter The writer
     * @param numberOfFields      The number of fields the process expects in each record
     */
    public AnalyticsControlMessageWriter(LengthEncodedWriter lengthEncodedWriter, int numberOfFields) {
        super(lengthEncodedWriter, numberOfFields);
    }

    public void writeEndOfData() throws IOException {
        writeMessage(END_OF_DATA_MESSAGE_CODE);
        fillCommandBuffer();
        lengthEncodedWriter.flush();
    }
}
