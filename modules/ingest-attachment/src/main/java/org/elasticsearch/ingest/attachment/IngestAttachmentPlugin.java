/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;
import java.util.function.UnaryOperator;

public class IngestAttachmentPlugin extends Plugin implements IngestPlugin {

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(AttachmentProcessor.TYPE, new AttachmentProcessor.Factory());
    }

    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    // This can be removed in V10. It's not possible to create an instance without the remove_binary property in V9, and all instances
    // created by V8 or earlier will have been fixed when upgraded to V9.
    @Override
    public Map<String, UnaryOperator<Metadata.Custom>> getCustomMetadataUpgraders() {
        return Map.of(
            IngestMetadata.TYPE,
            ingestMetadata -> ((IngestMetadata) ingestMetadata).maybeUpgradeProcessors(
                AttachmentProcessor.TYPE,
                AttachmentProcessor::maybeUpgradeConfig
            )
        );
    }
}
