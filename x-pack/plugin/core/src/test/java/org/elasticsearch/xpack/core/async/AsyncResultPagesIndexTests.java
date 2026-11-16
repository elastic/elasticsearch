/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ShardBatchMapper;
import org.elasticsearch.transport.BytesRefRecycler;

import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.EXPIRATION_TIME_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.FIRST_ROW_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.PAGE_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.PAGE_SEQ_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.ROW_COUNT_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.STREAM_ID_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncResultPagesIndex.TRANSPORT_VERSION_FIELD;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Page documents must be indexed through the columnar batch path, which falls back to the row path without failing
 * when a mapping is outside what it supports.
 */
public class AsyncResultPagesIndexTests extends MapperServiceTestCase {

    public void testPagesResolveForBatchIndexing() throws Exception {
        MapperService mapperService = createMapperService(
            AsyncResultPagesIndex.settings(),
            AsyncResultPagesIndex.getSystemIndexDescriptor().getMappings()
        );
        BytesRefRecycler recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (EscfBatchBuilder builder = new EscfBatchBuilder(recycler)) {
            for (int seq = 0; seq < 3; seq++) {
                EscfRowBuffer row = builder.beginRow();
                byte[] asyncId = "result-id".getBytes(StandardCharsets.UTF_8);
                row.stringField(STREAM_ID_FIELD, asyncId, 0, asyncId.length);
                row.longField(PAGE_SEQ_FIELD, seq);
                row.longField(FIRST_ROW_FIELD, seq * 100L);
                row.longField(ROW_COUNT_FIELD, 100);
                row.longField(TRANSPORT_VERSION_FIELD, 1);
                if (seq == 0) {
                    row.longField(EXPIRATION_TIME_FIELD, 1L);
                }
                row.binaryField(PAGE_FIELD, new BytesRef(randomByteArrayOfLength(between(1, 64))));
                row.finishRow();
                builder.commit(0);
            }
            try (EscfBatch batch = builder.buildPartition(0)) {
                assertThat(
                    ShardBatchMapper.resolveMappers(batch, mapperService.mappingLookup(), mapperService.getIndexSettings()),
                    notNullValue()
                );
            }
        }
    }
}
