/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestSourceAndMetadata;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.ingest.IngestDocument.Metadata;

public class UpdateSourceAndMetadata extends IngestSourceAndMetadata {
    protected static final Map<String, BiConsumer<String, Object>> VALIDATORS;
    static {
        Map<String, BiConsumer<String, Object>> v = new HashMap<>(IngestSourceAndMetadata.VALIDATORS);
        v.put("_now", )
    }

    public UpdateSourceAndMetadata(String index,
                                   String id,
                                   long version,
                                   String routing,
                                   Op op,
                                   long epochMilli,
                                   String type,
                                   Map<String, Object> source) {
        super(updateMetadataMap(index, id, version, routing, epochMilli, op, type), source, epochToZDT(epochMilli), VALIDATORS);
    }

    protected static Map<String, Object> updateMetadataMap(String index,
                                       String id,
                                       long version,
                                       String routing,
                                       long epochMilli,
                                       Op op,
                                       String type) {
        Map<String, Object> metadata = Maps.newHashMapWithExpectedSize(7);
        metadata.put(Metadata.INDEX.getFieldName(), index);
        metadata.put(Metadata.ID.getFieldName(), id);
        metadata.put(Metadata.VERSION.getFieldName(), version);
        if (routing != null) {
            metadata.put(Metadata.ROUTING.getFieldName(), routing);
        }
        metadata.put("_now", epochToZDT(epochMilli));
        metadata.put("_op", op.toString());
        metadata.put(Metadata.TYPE.getFieldName(), type);
        return metadata;
    }

    protected static ZonedDateTime epochToZDT(long epochMilli) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC);
    }

    String getIndex();
    String getId();
    String getRouting();
    long getVersion();
    boolean hasVersion();
    String getOp();
    void setOp(String op);
    ZonedDateTime getTimestamp();
    String getType();
}
