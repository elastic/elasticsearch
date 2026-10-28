/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.test;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Mock representation of the Ingest Common plugin for the subset of processors that are needed for the pipelines in Monitoring's exporters.
 */
public class MockIngestPlugin extends Plugin implements IngestPlugin {

    @Override
    public Map<String, Processor.Factory> getProcessors(final Processor.Parameters parameters) {
        return Stream.of(
            new MockProcessorFactory("gsub", new String[] { "field", "pattern", "replacement" }),
            new MockProcessorFactory("rename", new String[] { "field", "target_field" }),
            new MockProcessorFactory("set", new String[] { "field", "value" }),
            new MockProcessorFactory("script", new String[] { "source" })
        ).collect(Collectors.toMap(factory -> factory.type, Function.identity()));
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
        public Processor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            // read fields so the processor succeeds
            for (final String field : fields) {
                ConfigurationUtils.readObject(type, tag, config, field);
            }

            return new MockProcessor(type, tag, description);
        }

    }

    static class MockProcessor implements Processor {

        private final String type;
        private final String tag;
        private final String description;

        MockProcessor(final String type, final String tag, final String description) {
            this.type = type;
            this.tag = tag;
            this.description = description;
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

        @Override
        public String getDescription() {
            return description;
        }

    }

}
