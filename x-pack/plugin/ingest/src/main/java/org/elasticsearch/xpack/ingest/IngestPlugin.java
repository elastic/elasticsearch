/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ingest;

import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

public class IngestPlugin extends Plugin implements org.elasticsearch.plugins.IngestPlugin {

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return org.elasticsearch.common.collect.Map.of(UriPartsProcessor.TYPE, new UriPartsProcessor.Factory());
    }
}
