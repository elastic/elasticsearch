/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal.metering;

/**
 * An internal plugin that will return a DocumentReporterFactory. This allows to implement a reporting upon document parsing
 */
public interface DocumentReporterPlugin {

    /**
     * @return a documentReporterFactory which will report upon parsing
     */
    DocumentReporterFactory getDocumentReporter();
}
