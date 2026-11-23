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

    @Override
    protected String defaultMapping() {
        return """
            {
                "dynamic": false,
                "properties": {
                    "value": { "type": "keyword" },
                    "other": { "type": "keyword" },
                    "point": { "type": "geo_point" },
                    "range": { "type": "integer_range" },
                    "obj": {
                        "properties": {
                            "a": { "type": "keyword" },
                            "b": { "type": "keyword" },
                            "inner": {
                                "properties": {
                                    "c": { "type": "keyword" }
                                }
                            }
                        }
                    }
                }
            }
            """;
    }

    Translog.Index toIndexOp(String source) throws IOException {
        return toIndexOp(1, source);
    }

    Translog.Index toIndexOp(long seqNo, String source) throws IOException {
        XContentParser parser = createParser(XContentType.JSON.xContent(), source);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.copyCurrentStructure(parser);
        return new Translog.Index(
            "1",
            seqNo,
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
            config.getMergePolicy()
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

    public void testObjectFieldsAreComparedRecursively() throws Exception {
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engineConfig(true));
        {
            var o1 = toIndexOp("""
                { "obj": { "a": "x", "b": [ "z", "y" ], "inner": { "c": [ "v" ] } } }
                """);
            var o2 = toIndexOp("""
                { "obj": { "a": [ "x" ], "b": [ "y", "z" ], "inner": { "c": "v" } } }
                """);
            assertTrue(asserter.assertSameIndexOperation(o1, o2));
        }
        {
            // a difference in a deeply nested leaf is still detected
            var o1 = toIndexOp("""
                { "obj": { "a": "x", "inner": { "c": "v" } } }
                """);
            var o2 = toIndexOp("""
                { "obj": { "a": "x", "inner": { "c": "w" } } }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
        }
        {
            // a field missing from a nested object is detected
            var o1 = toIndexOp("""
                { "obj": { "a": "x", "b": "y" } }
                """);
            var o2 = toIndexOp("""
                { "obj": { "a": "x" } }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
        }
    }

    /**
     * Synthesizing turns a dotted field name into an object, so the original and synthesized forms of
     * an operation name the same field with different keys.
     */
    public void testDottedFieldNames() throws Exception {
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engineConfig(true));
        {
            var o1 = toIndexOp("""
                { "obj.a": "x" }
                """);
            var o2 = toIndexOp("""
                { "obj.a": "y" }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
            assertFalse(asserter.assertSameIndexOperation(o2, o1));
        }
        {
            var o1 = toIndexOp("""
                { "obj.inner.c": "x", "value": "same" }
                """);
            var o2 = toIndexOp("""
                { "obj.inner.c": "y", "value": "same" }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
            assertFalse(asserter.assertSameIndexOperation(o2, o1));
        }
        {
            // one operation names the field with a dotted key, the other with an object
            var o1 = toIndexOp("""
                { "obj.a": "x", "obj.inner.c": "v" }
                """);
            var o2 = toIndexOp("""
                { "obj": { "a": "y", "inner": { "c": "v" } } }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
            assertFalse(asserter.assertSameIndexOperation(o2, o1));
        }
        {
            var o1 = toIndexOp("""
                { "obj.a": "x", "obj.inner.c": "v" }
                """);
            var o2 = toIndexOp("""
                { "obj": { "a": [ "x" ], "inner": { "c": "v" } } }
                """);
            assertTrue(asserter.assertSameIndexOperation(o1, o2));
            assertTrue(asserter.assertSameIndexOperation(o2, o1));
        }
        {
            // a dotted name inside an object
            var o1 = toIndexOp("""
                { "obj": { "inner.c": "x" } }
                """);
            var o2 = toIndexOp("""
                { "obj": { "inner.c": "y" } }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
            assertFalse(asserter.assertSameIndexOperation(o2, o1));
        }
        {
            // the same field given both as a dotted key and inside an object
            var o1 = toIndexOp("""
                { "obj.a": "x", "obj": { "a": "y" } }
                """);
            var o2 = toIndexOp("""
                { "obj.a": "z", "obj": { "a": "y" } }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
            assertFalse(asserter.assertSameIndexOperation(o2, o1));
        }
    }

    /**
     * Synthesizing turns an array of objects into an object of arrays, so the original and synthesized
     * forms hold the fields at different depths.
     */
    public void testArraysOfObjects() throws Exception {
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engineConfig(true));
        {
            var o1 = toIndexOp("""
                { "obj": [ { "a": "x" }, { "a": "y" } ] }
                """);
            var o2 = toIndexOp("""
                { "obj": [ { "a": "x" }, { "a": "z" } ] }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
            assertFalse(asserter.assertSameIndexOperation(o2, o1));
        }
        {
            var o1 = toIndexOp("""
                { "obj": { "inner": [ { "c": "x" } ] } }
                """);
            var o2 = toIndexOp("""
                { "obj": { "inner": [ { "c": "y" } ] } }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
            assertFalse(asserter.assertSameIndexOperation(o2, o1));
        }
        {
            var o1 = toIndexOp("""
                { "obj": [ { "a": "x" }, { "a": "y" } ] }
                """);
            var o2 = toIndexOp("""
                { "obj": { "a": [ "x", "y" ] } }
                """);
            assertTrue(asserter.assertSameIndexOperation(o1, o2));
            assertTrue(asserter.assertSameIndexOperation(o2, o1));
        }
    }

    /**
     * A field value can itself be an object, and synthesizing can write it with other keys: a GeoJSON
     * point becomes {@code lat} and {@code lon}, and a range given with {@code gt} and {@code lt}
     * becomes {@code gte} and {@code lte}. A key that only the original or only the synthesized forms
     * have must not match.
     */
    public void testFieldValuesThatAreObjects() throws Exception {
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engineConfig(true));
        {
            var o1 = toIndexOp("""
                { "point": { "type": "Point", "coordinates": [ 2.0, 1.0 ] } }
                """);
            var o2 = toIndexOp("""
                { "point": { "type": "Point", "coordinates": [ 4.0, 3.0 ] } }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
            assertFalse(asserter.assertSameIndexOperation(o2, o1));
        }
        {
            var o1 = toIndexOp("""
                { "range": { "gt": 0, "lt": 6 } }
                """);
            var o2 = toIndexOp("""
                { "range": { "gt": 1, "lt": 6 } }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
            assertFalse(asserter.assertSameIndexOperation(o2, o1));
        }
        {
            var o1 = toIndexOp("""
                { "range": { "gte": 1 } }
                """);
            var o2 = toIndexOp("""
                { "range": { "gte": 1, "lt": 9 } }
                """);
            assertFalse(asserter.assertSameIndexOperation(o1, o2));
            assertFalse(asserter.assertSameIndexOperation(o2, o1));
        }
        {
            // the same point and the same range, written differently
            var o1 = toIndexOp("""
                { "point": { "type": "Point", "coordinates": [ 2.0, 1.0 ] }, "range": { "gt": 0, "lt": 6 } }
                """);
            var o2 = toIndexOp("""
                { "point": { "lat": 1.0, "lon": 2.0 }, "range": { "gte": 1, "lte": 5 } }
                """);
            assertTrue(asserter.assertSameIndexOperation(o1, o2));
            assertTrue(asserter.assertSameIndexOperation(o2, o1));
        }
    }

    public void testEveryTopLevelFieldMustMatch() throws Exception {
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engineConfig(true));
        // "value" is equivalent in synthesized form, but "other" really differs
        var o1 = toIndexOp("""
            { "value": "value-1", "other": "x" }
            """);
        var o2 = toIndexOp("""
            { "value": [ "value-1" ], "other": "y" }
            """);
        assertFalse(asserter.assertSameIndexOperation(o1, o2));
    }

    public void testFieldPresentInOnlyOneOperationIsRejected() throws Exception {
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engineConfig(true));
        var o1 = toIndexOp("""
            { "value": "value-1", "other": "x" }
            """);
        var o2 = toIndexOp("""
            { "value": "value-1" }
            """);
        assertFalse(asserter.assertSameIndexOperation(o1, o2));
        assertFalse(asserter.assertSameIndexOperation(o2, o1));
    }

    public void testDifferentArraysAreRejected() throws Exception {
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engineConfig(true));
        var o1 = toIndexOp("""
            { "value": [ "value-1", "value-2" ], "obj": { "a": "x" } }
            """);
        var o2 = toIndexOp("""
            { "value": [ "value-1", "value-3" ], "obj": { "a": "x" } }
            """);
        assertFalse(asserter.assertSameIndexOperation(o1, o2));
    }

    public void testWithAdditionalFieldWithNullValue() throws Exception {
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engineConfig(true));
        var o1 = toIndexOp("""
            { "value": "value-1", "other": null }
            """);
        var o2 = toIndexOp("""
            { "value": [ "value-1" ] }
            """);
        assertTrue(asserter.assertSameIndexOperation(o1, o2));
        assertTrue(asserter.assertSameIndexOperation(o2, o1));
    }

    public void testNonSourceFieldsAreStillCompared() throws Exception {
        TranslogOperationAsserter asserter = TranslogOperationAsserter.withEngineConfig(engineConfig(true));
        String source = """
            { "value": "value-1" }
            """;
        // equivalent sources must not mask a different sequence number
        assertFalse(asserter.assertSameIndexOperation(toIndexOp(1, source), toIndexOp(2, source)));
    }
}
