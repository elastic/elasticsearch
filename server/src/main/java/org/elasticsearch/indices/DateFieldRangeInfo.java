/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.IndexLongFieldRange;

/**
 * Data holder of timestamp fields held in cluster state IndexMetadata.
 */
public final class DateFieldRangeInfo {

    private final DateFieldMapper.DateFieldType timestampFieldType;
    private final IndexLongFieldRange timestampRange;
    private final DateFieldMapper.DateFieldType eventIngestedFieldType;
    private final IndexLongFieldRange eventIngestedRange;

    public DateFieldRangeInfo(
        DateFieldMapper.DateFieldType timestampFieldType,
        IndexLongFieldRange timestampRange,
        DateFieldMapper.DateFieldType eventIngestedFieldType,
        IndexLongFieldRange eventIngestedRange
    ) {
        this.timestampFieldType = timestampFieldType;
        this.timestampRange = timestampRange;
        this.eventIngestedFieldType = eventIngestedFieldType;
        this.eventIngestedRange = eventIngestedRange;
    }

    public DateFieldMapper.DateFieldType getTimestampFieldType() {
        return timestampFieldType;
    }

    public IndexLongFieldRange getTimestampRange() {
        return timestampRange;
    }

    public DateFieldMapper.DateFieldType getEventIngestedFieldType() {
        return eventIngestedFieldType;
    }

    public IndexLongFieldRange getEventIngestedRange() {
        return eventIngestedRange;
    }
}
