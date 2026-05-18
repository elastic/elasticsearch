/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class TranslogOperationAsserterTests extends EngineTestCase {

    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.name())
            .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
            .build();
    }

    Translog.Index toIndexOp(String source) throws IOException {
        XContentParser parser = createParser(XContentType.JSON.xContent(), source);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.copyCurrentStructure(parser);
        return new Translog.Index(
            "1",
            0,
            1,
            1,
            new BytesArray(Strings.toString(builder)),
            null,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP
        );
    }

    EngineConfig engineConfig(boolean useSyntheticSource) {
        EngineConfig config = engine.config();
        Settings.Builder settings = Settings.builder().put(config.getIndexSettings().getSettings());
        if (useSyntheticSource) {
            settings.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.name());
            settings.put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true);
        } else {
            settings.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.STORED.name());
            settings.put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), false);
        }
        IndexMetadata imd = IndexMetadata.builder(config.getIndexSettings().getIndexMetadata()).settings(settings).build();
        return config(
            new IndexSettings(imd, Settings.EMPTY),
            config.getStore(),
            config.getTranslogConfig().getTranslogPath(),
            config.getMergePolicy(),
            null
        );
    }

    public void testBasic() throws Exception {
        TranslogOperationAsserter syntheticAsserter = TranslogOperationAsserter.withEngineConfig(engineConfig(true));
        TranslogOperationAsserter regularAsserter = TranslogOperationAsserter.withEngineConfig(engineConfig(false));
        {
            var o1 = toIndexOp("""
                {
                    "value": "value-1"
                }
                """);
            var o2 = toIndexOp("""
                {
                    "value": [ "value-1" ]
                }
                """);
            assertTrue(syntheticAsserter.assertSameIndexOperation(o1, o2));
            assertFalse(regularAsserter.assertSameIndexOperation(o1, o2));
        }
        {
            var o1 = toIndexOp("""
                {
                    "value": [ "value-1", "value-2" ]
                }
                """);
            var o2 = toIndexOp("""
                {
                    "value": [ "value-1", "value-2" ]
                }
                """);
            assertTrue(syntheticAsserter.assertSameIndexOperation(o1, o2));
            assertTrue(regularAsserter.assertSameIndexOperation(o1, o2));
        }
        {
            var o1 = toIndexOp("""
                {
                    "value": [ "value-2", "value-1" ]
                }
                """);
            var o2 = toIndexOp("""
                {
                    "value": [ "value-1", "value-2" ]
                }
                """);
            assertTrue(syntheticAsserter.assertSameIndexOperation(o1, o2));
            assertFalse(regularAsserter.assertSameIndexOperation(o1, o2));
        }
        {
            var o1 = toIndexOp("""
                {
                    "value": [ "value-1", "value-2" ]
                }
                """);
            var o2 = toIndexOp("""
                {
                    "value": [ "value-1", "value-2", "value-2" ]
                }
                """);
            assertTrue(syntheticAsserter.assertSameIndexOperation(o1, o2));
            assertFalse(regularAsserter.assertSameIndexOperation(o1, o2));
        }
        {
            var o1 = toIndexOp("""
                {
                    "value": [ "value-1", "value-2" ]
                }
                """);
            var o2 = toIndexOp("""
                {
                    "value": [ "value-1", "value-2", "value-3" ]
                }
                """);
            assertFalse(syntheticAsserter.assertSameIndexOperation(o1, o2));
            assertFalse(regularAsserter.assertSameIndexOperation(o1, o2));
        }
    }
}
