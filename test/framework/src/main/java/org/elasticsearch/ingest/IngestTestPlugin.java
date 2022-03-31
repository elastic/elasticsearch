/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.Map;

/**
 * Adds an ingest processor to be used in tests.
 */
public class IngestTestPlugin extends Plugin implements IngestPlugin {
    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.singletonMap(
            "test",
            (factories, tag, description, config) -> new TestProcessor("id", "test", "description", doc -> {
                doc.setFieldValue("processed", true);
                if (doc.hasField("fail") && doc.getFieldValue("fail", Boolean.class)) {
                    throw new IllegalArgumentException("test processor failed");
                }
                if (doc.hasField("drop") && doc.getFieldValue("drop", Boolean.class)) {
                    return null;
                }
                return doc;
            })
        );
    }
}
