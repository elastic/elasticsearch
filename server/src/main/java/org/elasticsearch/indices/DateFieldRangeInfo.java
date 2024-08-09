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
 * @param timestampFieldType field type for the @timestamp field
 * @param timestampRange min/max range for the @timestamp field (in a specific index)
 * @param eventIngestedFieldType field type for the 'event.ingested' field
 * @param eventIngestedRange min/max range for the 'event.ingested' field (in a specific index)
 */
public record DateFieldRangeInfo(
    DateFieldMapper.DateFieldType timestampFieldType,
    IndexLongFieldRange timestampRange,
    DateFieldMapper.DateFieldType eventIngestedFieldType,
    IndexLongFieldRange eventIngestedRange
) {

}
