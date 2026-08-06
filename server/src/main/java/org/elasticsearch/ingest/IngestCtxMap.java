/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.CtxMap;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Map containing ingest source and metadata.
 * <p>
 * The Metadata values in {@link IngestDocument.Metadata} are validated when put in the map:
 * <ul>
 *   <li>{@code _index}, {@code _id} and {@code _routing} must be a String or null</li>
 *   <li>{@code _version_type} must be a lower case VersionType or null</li>
 *   <li>{@code _version} must be representable as a long without loss of precision or null</li>
 *   <li>{@code _dynamic_templates} must be a map</li>
 *   <li>{@code _if_seq_no} must be a long or null</li>
 *   <li>{@code _if_primary_term} must be a long or null</li>
 * </ul>
 * <p>
 * The map is expected to be used by processors, server code should the typed getter and setters where possible.
 * <p>
 * Ingest metadata is exposed for key lookup only ({@code get}, {@code getOrDefault} and {@code containsKey}), and only when the
 * source does not itself contain a field named {@code _ingest} (a source field of that name takes precedence). All other
 * {@link Map} operations operate on the source (and document metadata) only, as implemented in {@code CtxMap}.
 */
final class IngestCtxMap extends CtxMap<IngestDocMetadata> {

    private final Map<String, Object> ingestMetadata;

    /**
     * Create an IngestCtxMap with the given metadata, source and default validators
     * <p>
     * Ingest metadata will be initialized.
     * The passed-in source map is used directly (that is, it's neither shallowly nor deeply copied). mutation-like methods (e.g. setters,
     * put, etc.) may rely on the map being mutable, and will fail if the passed-in map isn't mutable.
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
        this(source, new IngestDocMetadata(index, id, version, routing, versionType, timestamp), null);
    }

    /**
     * Create IngestCtxMap from a source, metadata and ingest metadata.
     *
     * @param source the source document map
     * @param metadata the metadata map
     * @param ingestMetadata the ingest metadata map to expose as {@code _ingest}. If {@code null}, it will be initialized.
     */
    IngestCtxMap(Map<String, Object> source, IngestDocMetadata metadata, Map<String, Object> ingestMetadata) {
        super(source, metadata);
        if (ingestMetadata != null) {
            this.ingestMetadata = ingestMetadata;
        } else {
            this.ingestMetadata = new HashMap<>();
            this.ingestMetadata.put(IngestDocument.TIMESTAMP, metadata.getNow());
        }
    }

    Map<String, Object> getIngestMetadata() {
        return ingestMetadata;
    }

    /**
     * In ingest, all non-metadata keys are source keys, so the {@link #source} map is accessed directly from ctx.
     */
    @Override
    protected boolean directSourceAccess() {
        return true;
    }

    @Override
    public boolean containsKey(Object key) {
        if (IngestDocument.INGEST_KEY.equals(key)) {
            return true;
        }
        return super.containsKey(key);
    }

    @Override
    public Object get(Object key) {
        if (IngestDocument.INGEST_KEY.equals(key) && source.containsKey(key) == false) {
            return ingestMetadata;
        }
        return super.get(key);
    }

    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
        if (IngestDocument.INGEST_KEY.equals(key) && source.containsKey(key) == false) {
            return ingestMetadata;
        }
        return super.getOrDefault(key, defaultValue);
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
