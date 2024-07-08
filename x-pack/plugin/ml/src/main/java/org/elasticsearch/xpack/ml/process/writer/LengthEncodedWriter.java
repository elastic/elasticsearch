/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process.writer;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.ml.process.writer.RecordWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes the data records to the outputIndex stream as length encoded pairs.
 * Each record consists of number of fields followed by length/value pairs. The
 * first call to one the of the <code>writeRecord() </code> methods should be
 * with the header fields, once the headers are written records can be written
 * sequentially.
 * <p>
 * See CLengthEncodedInputParser.h in the C++ code for a more detailed
 * description.
 * </p>
 */
public class LengthEncodedWriter implements RecordWriter {
    private OutputStream outputStream;
    // In case customer setting "keep_job_data" is set to true, we will write the data to a file
    // additionally to the output stream.
    private OutputStream fileOutputStream;
    private ByteBuffer lengthBuffer;

    private Logger logger = LogManager.getLogger(LengthEncodedWriter.class);

    /**
     * Create the writer on the OutputStream <code>os</code>.
     * This object will never close <code>os</code>.
     */
    public LengthEncodedWriter(OutputStream os) {
        this(os, null);
    }

    public LengthEncodedWriter(OutputStream os, Path filePath) {
        outputStream = os;
        try {
            if (filePath != null) {
                logger.info("Opening file: " + filePath + " for writing.");
                fileOutputStream = Files.newOutputStream(filePath);
            } else {
                fileOutputStream = null;
            }
        } catch (IOException e) {
            logger.error("Failed to open file: " + filePath + " for writing.", e.getMessage(), e);
            fileOutputStream = null;
        }
        // This will be used to convert 32 bit integers to network byte order
        lengthBuffer = ByteBuffer.allocate(4); // 4 == sizeof(int)
    }

    // Add public destructor
    public void close() {
        if (fileOutputStream != null) {
            try {
                fileOutputStream.close();
            } catch (IOException e) {
                logger.error("Failed to close file output stream.", e.getMessage(), e);
            }
        }
    }

    /**
     * Convert each String in the record array to a length/value encoded pair
     * and write to the outputstream.
     */
    @Override
    public void writeRecord(String[] record) throws IOException {
        writeNumFields(record.length);

        for (String field : record) {
            writeField(field);
        }
    }

    /**
     * Convert each String in the record list to a length/value encoded
     * pair and write to the outputstream.
     */
    @Override
    public void writeRecord(List<String> record) throws IOException {
        writeNumFields(record.size());

        for (String field : record) {
            writeField(field);
        }
    }

    /**
     * Lower level functions to write records individually.
     * After this function is called {@link #writeField(String)}
     * must be called <code>numFields</code> times.
     */
    public void writeNumFields(int numFields) throws IOException {
        // number fields
        lengthBuffer.clear();
        lengthBuffer.putInt(numFields);
        outputStream.write(lengthBuffer.array());
        if (fileOutputStream != null) {
            fileOutputStream.write(lengthBuffer.array());
        }
    }

    /**
     * Lower level functions to write record fields individually.
     * {@linkplain #writeNumFields(int)} must be called first
     */
    public void writeField(String field) throws IOException {
        byte[] utf8Bytes = field.getBytes(StandardCharsets.UTF_8);
        lengthBuffer.clear();
        lengthBuffer.putInt(utf8Bytes.length);
        outputStream.write(lengthBuffer.array());
        outputStream.write(utf8Bytes);
        if (fileOutputStream != null) {
            fileOutputStream.write(lengthBuffer.array());
            fileOutputStream.write(utf8Bytes);
        }
    }

    @Override
    public void flush() throws IOException {
        outputStream.flush();
        if (fileOutputStream != null) {
            fileOutputStream.flush();
        }
    }
}
