/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.BiConsumer;

/**
 * Interface defining writers to a
 * {@link org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess}
 */
public interface DataToProcessWriter {

    /**
     * Write the header.
     * The header is created from the list of analysis input fields,
     * the time field and the control field.
     */
    void writeHeader() throws IOException;

    /**
     * Write the contents of <code>inputStream</code>.
     * If any of the fields in <code>analysisFields</code> or the
     * <code>DataDescription</code>s timeField is missing
     * a <code>MissingFieldException</code> is thrown
     */
    void write(InputStream inputStream, CategorizationAnalyzer categorizationAnalyzer, XContentType xContentType,
               BiConsumer<DataCounts, Exception> handler) throws IOException;

    /**
     * Flush the outputstream
     */
    void flushStream() throws IOException;
}
