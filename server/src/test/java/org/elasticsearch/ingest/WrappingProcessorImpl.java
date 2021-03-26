/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import java.util.function.Consumer;

class WrappingProcessorImpl extends FakeProcessor implements WrappingProcessor {

    WrappingProcessorImpl(String type, String tag, String description, Consumer<IngestDocument> executor) {
        super(type, tag, description, executor);
    }

    @Override
    public Processor getInnerProcessor() {
        String theType = getType();
        String theTag = getTag();
        String theDescription = getDescription();
        return new Processor() {
            @Override
            public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                return ingestDocument;
            }

            @Override
            public String getType() {
                return theType;
            }

            @Override
            public String getTag() {
                return theTag;
            }

            @Override
            public String getDescription() {
                return theDescription;
            }
        };
    }
}
