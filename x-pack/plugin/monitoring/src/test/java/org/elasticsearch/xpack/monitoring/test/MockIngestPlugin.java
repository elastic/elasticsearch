/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.test;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Mock representation of the Ingest Common plugin for the subset of processors that are needed for the pipelines in Monitoring's exporters.
 */
public class MockIngestPlugin extends Plugin implements IngestPlugin {

    @Override
    public Map<String, Processor.Factory> getProcessors(final Processor.Parameters parameters) {
        final Map<String, String[]> processorFields = MapBuilder.<String, String[]>newMapBuilder()
                .put("gsub", new String[] { "field", "pattern", "replacement" })
                .put("rename", new String[] { "field", "target_field" })
                .put("set", new String[] { "field", "value" })
                .put("script", new String[] { "source" })
                .map();

        return processorFields.entrySet()
                              .stream()
                              .map(MockProcessorFactory::new)
                              .collect(Collectors.toMap(factory -> factory.type, factory -> factory));
    }

    static class MockProcessorFactory implements Processor.Factory {

        private final String type;
        private final String[] fields;

        MockProcessorFactory(final Map.Entry<String, String[]> factory) {
            this(factory.getKey(), factory.getValue());
        }

        MockProcessorFactory(final String type, final String[] fields) {
            this.type = type;
            this.fields = fields;
        }

        @Override
        public Processor create(Map<String, Processor.Factory> processorFactories,
                                String tag,
                                Map<String, Object> config) throws Exception {
            // read fields so the processor succeeds
            for (final String field : fields) {
                ConfigurationUtils.readObject(type, tag, config, field);
            }

            return new MockProcessor(type, tag);
        }

    }

    static class MockProcessor implements Processor {

        private final String type;
        private final String tag;

        MockProcessor(final String type, final String tag) {
            this.type = type;
            this.tag = tag;
        }

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            // mock processor does nothing
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

    }

}
