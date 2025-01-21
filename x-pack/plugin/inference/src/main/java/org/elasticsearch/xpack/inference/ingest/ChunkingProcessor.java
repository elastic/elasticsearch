/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.ingest;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.xpack.inference.chunking.Chunker;
import org.elasticsearch.xpack.inference.chunking.ChunkerBuilder;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ChunkingProcessor extends AbstractProcessor {

    public static final String TYPE = "chunking";

    private final List<Factory.InputConfig> inputConfigs;
    private final ChunkingSettings chunkingSettings;

    public ChunkingProcessor(String tag, String description, List<Factory.InputConfig> inputConfigs, ChunkingSettings chunkingSettings) {
        super(tag, description);
        this.inputConfigs = inputConfigs;
        this.chunkingSettings = chunkingSettings;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        for (var inputConfig : inputConfigs) {
            var text = document.getFieldValue(inputConfig.inputField, String.class);
            var chunks = ChunkerBuilder.fromChunkingStrategy(chunkingSettings.getChunkingStrategy()).chunk(text, chunkingSettings);
            document.setFieldValue(inputConfig.outputField, toChunkText(chunks, text));
        }

        return document;
    }

    private List<String> toChunkText(List<Chunker.ChunkOffset> offsets, String text) {
        return offsets.stream().map(o -> text.substring(o.start(), o.end())).collect(Collectors.toList());
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public Processor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config
        ) {
            List<InputConfig> inputConfigs = parseInputConfigs(tag, ConfigurationUtils.readList(TYPE, tag, config, "input_output"));
            ChunkingSettings chunkingSettings = ChunkingSettingsBuilder.fromMap(
                ConfigurationUtils.readMap(TYPE, tag, config, "chunking_settings")
            );
            return new ChunkingProcessor(tag, description, inputConfigs, chunkingSettings);
        }

        private List<InputConfig> parseInputConfigs(String tag, List<Map<String, Object>> inputConfigMaps) {
            List<InputConfig> inputConfigs = new ArrayList<>();
            for (var inputConfigMap : inputConfigMaps) {
                String inputField = ConfigurationUtils.readStringProperty(TYPE, tag, inputConfigMap, "input_field");
                String outputField = ConfigurationUtils.readStringProperty(TYPE, tag, inputConfigMap, "output_field");
                inputConfigs.add(new InputConfig(inputField, outputField));
            }
            return inputConfigs;
        }

        public record InputConfig(String inputField, String outputField) {}
    }
}
