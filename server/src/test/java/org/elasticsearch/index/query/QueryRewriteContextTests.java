/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.indices.DateFieldRangeInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.elasticsearch.index.query.CoordinatorRewriteContext.TIER_FIELD_TYPE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class QueryRewriteContextTests extends ESTestCase {

    public void testGetTierPreference() {
        {
            // cold->hot tier preference
            IndexMetadata metadata = newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(DataTier.TIER_PREFERENCE, "data_cold,data_warm,data_hot")
                    .build()
            );
            QueryRewriteContext context = new QueryRewriteContext(
                parserConfig(),
                null,
                System::currentTimeMillis,
                null,
                MappingLookup.EMPTY,
                Collections.emptyMap(),
                new IndexSettings(metadata, Settings.EMPTY),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false
            );

            assertThat(context.getTierPreference(), is("data_cold"));
        }

        {
            // missing tier preference
            IndexMetadata metadata = newIndexMeta(
                "index",
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build()
            );
            QueryRewriteContext context = new QueryRewriteContext(
                parserConfig(),
                null,
                System::currentTimeMillis,
                null,
                MappingLookup.EMPTY,
                Collections.emptyMap(),
                new IndexSettings(metadata, Settings.EMPTY),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false
            );

            assertThat(context.getTierPreference(), is(nullValue()));
        }

        {
            // coordinator rewrite context
            CoordinatorRewriteContext coordinatorRewriteContext = new CoordinatorRewriteContext(
                parserConfig(),
                null,
                System::currentTimeMillis,
                new DateFieldRangeInfo(null, null, new DateFieldMapper.DateFieldType(IndexMetadata.EVENT_INGESTED_FIELD_NAME), null),
                "data_frozen"
            );

            assertThat(coordinatorRewriteContext.getTierPreference(), is("data_frozen"));
        }

        {
            // coordinator rewrite context empty tier
            CoordinatorRewriteContext coordinatorRewriteContext = new CoordinatorRewriteContext(
                parserConfig(),
                null,
                System::currentTimeMillis,
                new DateFieldRangeInfo(null, null, new DateFieldMapper.DateFieldType(IndexMetadata.EVENT_INGESTED_FIELD_NAME), null),
                ""
            );

            assertThat(coordinatorRewriteContext.getTierPreference(), is(nullValue()));
        }

        {
            // null date field range info
            CoordinatorRewriteContext coordinatorRewriteContext = new CoordinatorRewriteContext(
                parserConfig(),
                null,
                System::currentTimeMillis,
                null,
                "data_frozen"
            );
            assertThat(coordinatorRewriteContext.getFieldRange(IndexMetadata.EVENT_INGESTED_FIELD_NAME), is(nullValue()));
            assertThat(coordinatorRewriteContext.getFieldRange(IndexMetadata.EVENT_INGESTED_FIELD_NAME), is(nullValue()));
            // tier field doesn't have a range
            assertThat(coordinatorRewriteContext.getFieldRange(CoordinatorRewriteContext.TIER_FIELD_NAME), is(nullValue()));
            assertThat(coordinatorRewriteContext.getFieldType(IndexMetadata.EVENT_INGESTED_FIELD_NAME), is(nullValue()));
            assertThat(coordinatorRewriteContext.getFieldType(IndexMetadata.EVENT_INGESTED_FIELD_NAME), is(nullValue()));
            // _tier field type should still work even without the data field info
            assertThat(coordinatorRewriteContext.getFieldType(CoordinatorRewriteContext.TIER_FIELD_NAME), is(TIER_FIELD_TYPE));
        }
    }

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        return IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 1).put(indexSettings)).build();
    }

}
