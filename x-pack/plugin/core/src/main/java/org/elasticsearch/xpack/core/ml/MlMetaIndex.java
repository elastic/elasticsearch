/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;

public final class MlMetaIndex {
    /**
     * Where to store the ml info in Elasticsearch - must match what's
     * expected by kibana/engineAPI/app/directives/mlLogUsage.js
     */
    public static final String INDEX_NAME = ".ml-meta";

    private MlMetaIndex() {}

    public static XContentBuilder docMapping() throws IOException {
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
            builder.startObject(SINGLE_MAPPING_NAME);
                ElasticsearchMappings.addMetaInformation(builder);
                ElasticsearchMappings.addDefaultMapping(builder);
                builder.startObject(ElasticsearchMappings.PROPERTIES)
                    .startObject(Calendar.ID.getPreferredName())
                        .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.KEYWORD)
                    .endObject()
                    .startObject(Calendar.JOB_IDS.getPreferredName())
                        .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.KEYWORD)
                    .endObject()
                    .startObject(Calendar.DESCRIPTION.getPreferredName())
                        .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.KEYWORD)
                    .endObject()
                    .startObject(ScheduledEvent.START_TIME.getPreferredName())
                        .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.DATE)
                    .endObject()
                    .startObject(ScheduledEvent.END_TIME.getPreferredName())
                        .field(ElasticsearchMappings.TYPE, ElasticsearchMappings.DATE)
                    .endObject()
                .endObject()
            .endObject()
        .endObject();
        return builder;
    }
}
