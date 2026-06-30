/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.KeywordToFlattenedTransformer.FlattenedJunkConfig;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

/**
 * Unit tests for the junk-injection logic in {@link KeywordToFlattenedTransformer}.
 * <p>
 * These tests exercise {@link FlattenedJunkConfig#selectJunkFields} and the
 * {@link KeywordToFlattenedTransformer#wrapKeywordValuesAsFlattened(String, Set, FlattenedJunkConfig, Random)}
 * overload that accepts a junk config.
 */
public class KeywordToFlattenedTransformerTests extends ESTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ── selectJunkFields ────────────────────────────────────────────────────

    /**
     * Empty input set → always returns the empty config, regardless of the random coin.
     */
    public void testSelectJunkFields_emptyInput_returnsEmpty() {
        // Use different seeds to ensure both coin outcomes are covered
        for (long seed : new long[] { 0L, 1L, 42L, Long.MAX_VALUE }) {
            FlattenedJunkConfig cfg = FlattenedJunkConfig.selectJunkFields(Set.of(), new Random(seed));
            assertTrue("expected empty junk fields for empty input (seed=" + seed + ")", cfg.junkFields().isEmpty());
        }
    }

    /**
     * When the coin comes up tails (first {@code nextBoolean()} returns {@code false}) the
     * returned config must be empty.  We use a deterministic stub to force a tails outcome.
     */
    public void testSelectJunkFields_tailsOutcome_returnsEmpty() {
        // Stub: always returns false (tails) from nextBoolean()
        Random tailsRandom = new Random(1L) {
            @Override
            public boolean nextBoolean() {
                return false;
            }
        };
        FlattenedJunkConfig cfg = FlattenedJunkConfig.selectJunkFields(Set.of("a", "b", "c"), tailsRandom);
        assertTrue("tails coin must produce empty junk config", cfg.junkFields().isEmpty());
    }

    /**
     * When the coin comes up heads the returned config must be a non-empty subset of the input.
     * We use a deterministic stub to force a heads outcome.
     */
    public void testSelectJunkFields_headsOutcome_returnsNonEmptySubset() {
        // Stub: always returns true (heads) from nextBoolean(); nextInt() returns 0 to pick minimum count.
        Random headsRandom = new Random(1L) {
            @Override
            public boolean nextBoolean() {
                return true;
            }

            @Override
            public int nextInt(int bound) {
                return 0; // always pick the first element / minimum count
            }
        };
        Set<String> input = Set.of("alpha", "beta", "gamma", "delta");
        FlattenedJunkConfig cfg = FlattenedJunkConfig.selectJunkFields(input, headsRandom);

        assertFalse("heads coin must produce non-empty junk config", cfg.junkFields().isEmpty());
        assertTrue("junk fields count must be <= input size", cfg.junkFields().size() <= input.size());
        assertTrue("all junk fields must be members of the input set", input.containsAll(cfg.junkFields()));
    }

    /**
     * Count is between 1 and {@code size} inclusive: verify min=1 and max=size with stubs.
     */
    public void testSelectJunkFields_countIsInRange() {
        Set<String> input = Set.of("f1", "f2", "f3", "f4", "f5");

        // Stub that picks the minimum count (nextInt always 0 → count = 1 + 0 = 1)
        Random minRandom = new Random(1L) {
            @Override
            public boolean nextBoolean() {
                return true; // heads
            }

            @Override
            public int nextInt(int bound) {
                return 0;
            }
        };
        FlattenedJunkConfig minCfg = FlattenedJunkConfig.selectJunkFields(input, minRandom);
        assertEquals("minimum count stub must yield exactly 1 field", 1, minCfg.junkFields().size());

        // Stub that picks the maximum count (nextInt always bound-1 → count = 1 + (size-1) = size)
        Random maxRandom = new Random(1L) {
            @Override
            public boolean nextBoolean() {
                return true; // heads
            }

            @Override
            public int nextInt(int bound) {
                return bound - 1;
            }
        };
        FlattenedJunkConfig maxCfg = FlattenedJunkConfig.selectJunkFields(input, maxRandom);
        assertEquals("maximum count stub must yield all fields", input.size(), maxCfg.junkFields().size());
        assertTrue("all junk fields must be members of the input", input.containsAll(maxCfg.junkFields()));
    }

    // ── wrapKeywordValuesAsFlattened (junk overload) ─────────────────────

    /**
     * Fields in the junk config must have the {@code "v"} key plus at least one extra key.
     * Fields NOT in the junk config must have exactly one key ({@code "v"}).
     * The result must be parseable as JSON.
     */
    public void testWrapWithJunk_junkFieldsHaveExtraKeys() throws IOException {
        String documentJson = "{\"kw1\":\"hello\",\"kw2\":\"world\",\"kw3\":\"foo\"}";
        Set<String> allPaths = Set.of("kw1", "kw2", "kw3");
        // Force junk on kw1 only; use random() from ESTestCase so the run is reproducible
        FlattenedJunkConfig junk = new FlattenedJunkConfig(Set.of("kw1"));

        String result = KeywordToFlattenedTransformer.wrapKeywordValuesAsFlattened(documentJson, allPaths, junk, random());

        // Must be valid JSON
        JsonNode root = MAPPER.readTree(result);
        assertTrue("result must be a JSON object", root.isObject());

        // kw1 is a junk field: must have "v" + at least one extra key
        JsonNode kw1 = root.get("kw1");
        assertNotNull("kw1 must be present", kw1);
        assertTrue("kw1 must be wrapped as object", kw1.isObject());
        ObjectNode kw1Obj = (ObjectNode) kw1;
        assertTrue("kw1 must have 'v' key", kw1Obj.has(KeywordToFlattenedTransformer.WRAPPER_SUBKEY));
        assertEquals("kw1.v must be the original value", "hello", kw1Obj.get("v").asText());
        assertTrue("kw1 must have at least one extra key (junk)", kw1Obj.size() > 1);

        // kw2 is NOT a junk field: must have exactly "v"
        JsonNode kw2 = root.get("kw2");
        assertNotNull("kw2 must be present", kw2);
        assertTrue("kw2 must be wrapped as object", kw2.isObject());
        ObjectNode kw2Obj = (ObjectNode) kw2;
        assertEquals("kw2 must have exactly one key (v)", 1, kw2Obj.size());
        assertEquals("kw2.v must be the original value", "world", kw2Obj.get("v").asText());

        // kw3 is NOT a junk field either
        JsonNode kw3 = root.get("kw3");
        assertNotNull("kw3 must be present", kw3);
        assertTrue("kw3 must be wrapped as object", kw3.isObject());
        ObjectNode kw3Obj = (ObjectNode) kw3;
        assertEquals("kw3 must have exactly one key (v)", 1, kw3Obj.size());
        assertEquals("kw3.v must be the original value", "foo", kw3Obj.get("v").asText());
    }

    /**
     * When the junk config is empty the new overload produces the same result as the
     * legacy overload.
     */
    public void testWrapWithEmptyJunkConfig_equivalentToLegacy() throws IOException {
        String documentJson = "{\"field1\":\"alpha\",\"field2\":\"beta\"}";
        Set<String> paths = Set.of("field1", "field2");

        String legacy = KeywordToFlattenedTransformer.wrapKeywordValuesAsFlattened(documentJson, paths);
        String withJunk = KeywordToFlattenedTransformer.wrapKeywordValuesAsFlattened(
            documentJson,
            paths,
            FlattenedJunkConfig.EMPTY,
            null  // random not used when junk config is empty
        );

        assertEquals("empty junk config must produce same result as legacy overload", legacy, withJunk);
    }

    /**
     * Missing fields (not present in the document) are silently skipped, as in the legacy overload.
     */
    public void testWrapWithJunk_missingFieldsSkipped() throws IOException {
        String documentJson = "{\"present\":\"value\"}";
        Set<String> paths = Set.of("present", "absent");
        FlattenedJunkConfig junk = new FlattenedJunkConfig(Set.of("present", "absent"));

        String result = KeywordToFlattenedTransformer.wrapKeywordValuesAsFlattened(documentJson, paths, junk, random());

        JsonNode root = MAPPER.readTree(result);
        assertFalse("absent field must not appear in result", root.has("absent"));
        assertTrue("present field must be wrapped", root.get("present").isObject());
    }

    /**
     * A non-object document is returned unchanged even when junk config is non-empty.
     */
    public void testWrapWithJunk_nonObjectDocumentUnchanged() throws IOException {
        String documentJson = "[\"not\",\"an\",\"object\"]";
        Set<String> paths = Set.of("whatever");
        FlattenedJunkConfig junk = new FlattenedJunkConfig(Set.of("whatever"));

        String result = KeywordToFlattenedTransformer.wrapKeywordValuesAsFlattened(documentJson, paths, junk, random());
        assertEquals("non-object document must be returned unchanged", documentJson, result);
    }

    // ── helpers ─────────────────────────────────────────────────────────────

}
