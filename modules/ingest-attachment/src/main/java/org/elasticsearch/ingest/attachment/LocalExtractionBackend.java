/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.apache.tika.exception.ZeroByteFileException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link ExtractionBackend} that runs Apache Tika in-process.
 *
 * <p>The {@link #extract} method always invokes the listener <em>synchronously</em>, before
 * returning. Callers that depend on this guarantee must not mix instances of this class with
 * {@link TikaServerExtractionBackend} in any async-sensitive context.
 */
final class LocalExtractionBackend implements ExtractionBackend {

    @Override
    public void extract(byte[] content, @Nullable String resourceName, int maxChars, ActionListener<ExtractionResult> listener) {
        Metadata tikaMetadata = new Metadata();
        if (resourceName != null) {
            tikaMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, resourceName);
        }
        try {
            String parsedContent = "";
            try {
                parsedContent = TikaImpl.parse(content, tikaMetadata, maxChars);
            } catch (ZeroByteFileException e) {
                // tika 1.17+ throws on zero-byte input; preserve the prior behaviour of returning empty
            }
            listener.onResponse(new ExtractionResult(parsedContent, buildMetadataMap(tikaMetadata)));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Copies all Tika metadata into a plain {@code Map<String, String>}, taking the first value
     * for multi-valued fields.
     */
    private static Map<String, String> buildMetadataMap(Metadata tikaMetadata) {
        Map<String, String> map = new HashMap<>();
        for (String name : tikaMetadata.names()) {
            String value = tikaMetadata.get(name);
            if (value != null) {
                map.put(name, value);
            }
        }
        return map;
    }

    @Override
    public void close() throws IOException {
        // nothing to close
    }
}
