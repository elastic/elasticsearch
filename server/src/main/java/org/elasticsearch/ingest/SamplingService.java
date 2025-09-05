/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.plugins.internal.XContentParserDecorator;
import org.elasticsearch.sample.TransportPutSampleConfigAction;
import org.elasticsearch.script.DynamicMap;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.ConditionalProcessor.FUNCTIONS;

public class SamplingService {
    private final ScriptService scriptService;
    private final Map<String, List<IndexRequest>> samples = new HashMap<>();

    public SamplingService(ScriptService scriptService) {
        this.scriptService = scriptService;
    }

    public void maybeSample(ProjectMetadata projectMetadata, IndexRequest indexRequest) throws IOException {
        maybeSample(
            projectMetadata,
            indexRequest,
            new IngestDocument(
                indexRequest.index(),
                indexRequest.id(),
                indexRequest.version(),
                indexRequest.routing(),
                indexRequest.versionType(),
                indexRequest.sourceAsMap(XContentParserDecorator.NOOP)
            )
        );
    }

    public void maybeSample(ProjectMetadata projectMetadata, IndexRequest indexRequest, IngestDocument ingestDocument) throws IOException {
        TransportPutSampleConfigAction.SamplingConfigCustomMetadata samplingConfig = projectMetadata.custom(
            TransportPutSampleConfigAction.SamplingConfigCustomMetadata.NAME
        );
        if (samplingConfig != null) {
            String samplingIndex = samplingConfig.indexName;
            if (samplingIndex.equals(indexRequest.index())) {
                List<IndexRequest> samplesForIndex = samples.computeIfAbsent(samplingIndex, k -> new ArrayList<>());
                if (samplesForIndex.size() < samplingConfig.maxSamples) {
                    String condition = samplingConfig.condition;
                    if (evaluateCondition(ingestDocument, condition)) {
                        if (Math.random() < samplingConfig.rate) {
                            samplesForIndex.add(indexRequest);
                            System.out.println("Sampling " + indexRequest);
                        }
                    }
                }
            }
        }
    }

    public List<IndexRequest> getSamples(String index) {
        return samples.get(index);
    }

    private boolean evaluateCondition(IngestDocument ingestDocument, String condition) throws IOException {
        if (condition == null) {
            return true;
        }
        IngestConditionalScript.Factory factory = null;// precompiledConditionalScriptFactory;
        Script script = getScript(condition);
        if (factory == null) {
            factory = scriptService.compile(script, IngestConditionalScript.CONTEXT);
        }
        return factory.newInstance(
            script.getParams(),
            new ConditionalProcessor.UnmodifiableIngestData(new DynamicMap(ingestDocument.getSourceAndMetadata(), FUNCTIONS))
        ).execute();
    }

    private static Script getScript(String conditional) throws IOException {
        if (conditional != null) {
            try (
                XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).map(Map.of("source", conditional));
                XContentParser parser = XContentHelper.createParserNotCompressed(
                    LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                    BytesReference.bytes(builder),
                    XContentType.JSON
                )
            ) {
                return Script.parse(parser);
            }
        }
        return null;
    }
}
