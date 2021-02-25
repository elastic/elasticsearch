/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

/**
 * An Abstract Processor that holds tag and description information
 * about the processor.
 */
public abstract class AbstractProcessor implements Processor {
    protected final String tag;
    protected final String description;

    protected AbstractProcessor(String tag, String description) {
        this.tag = tag;
        this.description = description;
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
