/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import java.util.Map;

/**
 * Drop processor only returns {@code null} for the execution result to indicate that any document
 * executed by it should not be indexed.
 */
public final class DropProcessor extends AbstractProcessor {

    public static final String TYPE = "drop";

    private DropProcessor(final String tag, final String description) {
        super(tag, description);
    }

    @Override
    public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
        return null;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public Processor create(
            final Map<String, Processor.Factory> processorFactories,
            final String tag,
            final String description,
            final Map<String, Object> config
        ) {
            return new DropProcessor(tag, description);
        }
    }
}
