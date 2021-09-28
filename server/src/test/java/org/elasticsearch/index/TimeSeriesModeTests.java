/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceToParse;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TimeSeriesModeTests extends MapperServiceTestCase {
    public void testPartitioned() {
        Settings s = Settings.builder()
            .put(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.getKey(), 2)
            .put(IndexSettings.MODE.getKey(), "time_series")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.routing_partition_size]"));
    }

    public void testSortField() {
        Settings s = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), "a")
            .put(IndexSettings.MODE.getKey(), "time_series")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.field]"));
    }

    public void testSortMode() {
        Settings s = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_MISSING_SETTING.getKey(), "_last")
            .put(IndexSettings.MODE.getKey(), "time_series")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.missing]"));
    }

    public void testSortOrder() {
        Settings s = Settings.builder()
            .put(IndexSortConfig.INDEX_SORT_ORDER_SETTING.getKey(), "desc")
            .put(IndexSettings.MODE.getKey(), "time_series")
            .build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> IndexSettings.MODE.get(s));
        assertThat(e.getMessage(), equalTo("[index.mode=time_series] is incompatible with [index.sort.order]"));
    }

    public void testValidateTimestamp() throws IOException {
        long startTime = Math.min(randomMillisUpToYear9999(), DateUtils.MAX_MILLIS_BEFORE_9999 - 86400000);
        long endTime = startTime + randomLongBetween(1000, 86400000);
        Settings s = Settings.builder()
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), startTime)
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), endTime)
            .put(IndexSettings.MODE.getKey(), "time_series")
            .build();

        DocumentMapper mapper = createMapperService(s, mapping(b -> b.startObject("@timestamp").field("type", "date").endObject()))
            .documentMapper();
        // timestamp missing
        {
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> mapper.parse(
                    new SourceToParse(
                        "test",
                        "1",
                        BytesReference.bytes(XContentFactory.jsonBuilder().startObject().endObject()),
                        XContentType.JSON
                    )
                )
            );
            assertThat(e.getRootCause().getMessage(), containsString("time series index @timestamp field is missing"));
        }

        // timestamp smaller than start_time
        {
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> mapper.parse(
                    new SourceToParse(
                        "test",
                        "1",
                        BytesReference.bytes(
                            XContentFactory.jsonBuilder()
                                .startObject()
                                .field("@timestamp", Math.max(startTime - randomLongBetween(10, 1000), 0))
                                .endObject()
                        ),
                        XContentType.JSON
                    )
                )
            );
            assertThat(e.getRootCause().getMessage(), containsString("must be larger than"));
        }

        // timestamp larger than end_time
        {
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> mapper.parse(
                    new SourceToParse(
                        "test",
                        "1",
                        BytesReference.bytes(
                            XContentFactory.jsonBuilder()
                                .startObject()
                                .field("@timestamp", endTime + randomLongBetween(10, 1000))
                                .endObject()
                        ),
                        XContentType.JSON
                    )
                )
            );
            assertThat(e.getRootCause().getMessage(), containsString("must be smaller than"));
        }

        // timestamp correct
        {
            mapper.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder().startObject().field("@timestamp", randomLongBetween(startTime, endTime)).endObject()
                    ),
                    XContentType.JSON
                )
            );
        }
    }
}
