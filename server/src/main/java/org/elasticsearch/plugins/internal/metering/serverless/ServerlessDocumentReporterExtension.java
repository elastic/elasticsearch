/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal.metering.serverless;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.plugins.internal.metering.DocumentReporterExtension;
import org.elasticsearch.xcontent.XContentParser;

import java.util.concurrent.atomic.AtomicLong;

public class ServerlessDocumentReporterExtension implements DocumentReporterExtension {
    private final Logger logger = LogManager.getLogger(ServerlessDocumentReporter.class);

    private AtomicLong counter = new AtomicLong();
    private String index = null;

    @Override
    public XContentParser wrapParser(XContentParser parser) {
        return new MeteringParser(parser, counter);
    }

    @Override
    public void reportDocumentParsed(String index) {
        // in serverless it should always be CountingDocumentParserContext
        logger.error("REPORTING " + counter + " on index " + index, new RuntimeException());
        // reportManager.report(index,counter)
    }

}
