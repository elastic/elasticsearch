/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.List;
import java.util.Map;

public class IngestAttachmentPlugin extends Plugin implements IngestPlugin {

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(AttachmentProcessor.MAX_FIELD_SIZE_SETTING, AttachmentProcessor.MAX_FIELD_SIZE_MESSAGE_SUFFIX_SETTING);
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(AttachmentProcessor.TYPE, new AttachmentProcessor.Factory(parameters.env.settings()));

    }

}
