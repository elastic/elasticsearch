/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0"; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ConstantKeywordColumnarNullRejectionTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new ConstantKeywordMapperPlugin());
    }

    private void assertRejected(MapperService mapperService, CheckedConsumer<XContentBuilder, IOException> document) {
        final Exception e = expectThrows(Exception.class, () -> mapperService.documentMapper().parse(source(b -> {
            b.field("@timestamp", "2026-01-01T00:00:30.000Z");
            document.accept(b);
        })));
        assertThat(ExceptionsHelper.stackTrace(e), containsString("doesn't accept [null] values"));
    }

    public void testConstantKeywordRejectsANull() throws IOException {
        for (final IndexMode mode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR, IndexMode.VECTORDB_COLUMNAR)) {
            final MapperService mapperService = createMapperService(
                IndexVersion.current(),
                Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).build(),
                mapping(b -> {
                    b.startObject("@timestamp").field("type", "date").endObject();
                    b.startObject("ck").field("type", "constant_keyword").field("value", "a").endObject();
                })
            );
            assertRejected(mapperService, b -> b.nullField("ck"));
            assertRejected(mapperService, b -> b.startArray("ck").nullValue().endArray());
            assertRejected(mapperService, b -> b.startArray("ck").value("a").nullValue().endArray());
        }
    }
}
