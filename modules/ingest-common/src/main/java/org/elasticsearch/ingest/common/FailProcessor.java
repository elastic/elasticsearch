/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.util.Map;

/**
 * Processor that raises a runtime exception with a provided
 * error message.
 */
public final class FailProcessor extends AbstractProcessor {

    public static final String TYPE = "fail";

    private final TemplateScript.Factory message;

    FailProcessor(String tag, String description, TemplateScript.Factory message) {
        super(tag, description);
        this.message = message;
    }

    public TemplateScript.Factory getMessage() {
        return message;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        throw new FailProcessorException(document.renderTemplate(message));
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        private final ScriptService scriptService;

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public FailProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            String message = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "message");
            TemplateScript.Factory compiledTemplate = ConfigurationUtils.compileTemplate(
                TYPE,
                processorTag,
                "message",
                message,
                scriptService
            );
            return new FailProcessor(processorTag, description, compiledTemplate);
        }
    }
}
