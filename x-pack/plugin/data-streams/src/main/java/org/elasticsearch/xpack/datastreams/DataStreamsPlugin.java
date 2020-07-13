/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.datastreams;

import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.xpack.datastreams.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.action.ActionModule.DATASTREAMS_FEATURE_ENABLED;

public class DataStreamsPlugin extends Plugin implements MapperPlugin {

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        if (DATASTREAMS_FEATURE_ENABLED) {
            return Collections.singletonMap(DataStreamTimestampFieldMapper.NAME, new DataStreamTimestampFieldMapper.TypeParser());
        } else {
            return Collections.emptyMap();
        }
    }
}
