/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.CtxMap;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Map containing ingest source and metadata.
 *
 * The Metadata values in {@link IngestDocument.Metadata} are validated when put in the map.
 * _index, _id and _routing must be a String or null
 * _version_type must be a lower case VersionType or null
 * _version must be representable as a long without loss of precision or null
 * _dynamic_templates must be a map
 * _if_seq_no must be a long or null
 * _if_primary_term must be a long or null
 *
 * The map is expected to be used by processors, server code should the typed getter and setters where possible.
 */
class IngestCtxMap extends CtxMap<IngestDocMetadata> {

    /**
     * Create an IngestCtxMap with the given metadata, source and default validators
     */
    IngestCtxMap(
        String index,
        String id,
        long version,
        String routing,
        VersionType versionType,
        ZonedDateTime timestamp,
        Map<String, Object> source
    ) {
        super(new HashMap<>(source), new IngestDocMetadata(index, id, version, routing, versionType, timestamp));
    }

    /**
     * Create IngestCtxMap from a source and metadata
     *
     * @param source the source document map
     * @param metadata the metadata map
     */
    IngestCtxMap(Map<String, Object> source, IngestDocMetadata metadata) {
        super(source, metadata);
    }

    /**
     * In ingest, all non-metadata keys are source keys, so the {@link #source} map is accessed directly from ctx.
     */
    @Override
    protected boolean directSourceAccess() {
        return true;
    }

    /**
     * Fetch the timestamp from the ingestMetadata, if it exists
     * @return the timestamp for the document or null
     */
    public static ZonedDateTime getTimestamp(Map<String, Object> ingestMetadata) {
        if (ingestMetadata == null) {
            return null;
        }
        Object ts = ingestMetadata.get(IngestDocument.TIMESTAMP);
        if (ts instanceof ZonedDateTime timestamp) {
            return timestamp;
        } else if (ts instanceof String str) {
            return ZonedDateTime.parse(str);
        }
        return null;
    }
}
