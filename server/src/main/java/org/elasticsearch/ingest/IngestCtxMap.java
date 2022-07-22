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
import org.elasticsearch.script.RestrictedMap;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Map containing ingest source and metadata.
 *
 * The Metadata values in {@link IngestDocument.Metadata} are validated when put in the map.
 * _index, _id and _routing must be a String or null
 * _version_type must be a lower case VersionType or null
 * _version must be representable as a long without loss of precision or null
 * _dyanmic_templates must be a map
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

    @Override
    public Map<String, Object> getSource() {
        return source.asMap();
    }

    @Override
    protected RestrictedMap wrapSource(Map<String, Object> source) {
        return new RawSourceMap(source);
    }

    /**
     * RawSourceMap adapts a normal map to the {@link RestrictedMap} interface.
     * The ingest context does not access source via _source, as other update contexts,
     * so there is no need to protect the source map.
     */
    public static class RawSourceMap implements RestrictedMap {
        protected Map<String, Object> source;

        RawSourceMap(Map<String, Object> source) {
            this.source = source;
        }

        @Override
        public boolean isAvailable(String key) {
            return true;
        }

        @Override
        public Object put(String key, Object value) {
            return source.put(key, value);
        }

        @Override
        public boolean containsKey(String key) {
            return source.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            return source.containsValue(value);
        }

        @Override
        public Object get(String key) {
            return source.get(key);
        }

        @Override
        public Object remove(String key) {
            return source.remove(key);
        }

        @Override
        public Set<String> keySet() {
            return source.keySet();
        }

        @Override
        public int size() {
            return source.size();
        }

        @Override
        public RestrictedMap clone() {
            return new RawSourceMap(new HashMap<>(source));
        }

        @Override
        public Map<String, Object> asMap() {
            return source;
        }

        @Override
        public void clear() {
            source.clear();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof RawSourceMap) == false) return false;
            RawSourceMap that = (RawSourceMap) o;
            return source.equals(that.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source);
        }
    }
}
