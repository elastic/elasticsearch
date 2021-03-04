/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import java.util.function.Consumer;

class FakeProcessor implements Processor {
    private String type;
    private String tag;
    private String description;
    private Consumer<IngestDocument> executor;

    FakeProcessor(String type, String tag, String description, Consumer<IngestDocument> executor) {
        this.type = type;
        this.tag = tag;
        this.description = description;
        this.executor = executor;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        executor.accept(ingestDocument);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
