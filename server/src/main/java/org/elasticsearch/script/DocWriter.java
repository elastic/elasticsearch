/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.script.field.Metadata;

import java.util.Map;

/**
 * Access JSON as a document both old-style ctx[''] and new style field('')
 */
public interface DocWriter {
    Metadata meta();
    Map<String, Object> asCtx();
    IngestDocument asIngestDocument();
}
