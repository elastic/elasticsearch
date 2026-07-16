/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsOptions;
import org.elasticsearch.xpack.core.inference.chunking.SentenceBoundaryChunkingSettings;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests.createRandomChunkingSettings;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.driverContext;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.evaluator;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.row;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.Chunk.ALLOWED_CHUNKING_SETTING_OPTIONS;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.Chunk.DEFAULT_CHUNKING_SETTINGS;
import static org.elasticsearch.xpack.esql.expression.function.scalar.string.ChunkTests.createChunkingSettings;
import static org.elasticsearch.xpack.esql.expression.function.scalar.util.ChunkUtils.chunkText;
import static org.hamcrest.Matchers.equalTo;

/**
 * Non-parameterized tests for Chunk function.
 */
public class ChunkStaticTests extends ESTestCase {
    private static final String PARAGRAPH_INPUT = """
        The Adirondacks, a vast mountain region in northern New York, offer a breathtaking mix of rugged wilderness, serene lakes,
        and charming small towns. Spanning over six million acres, the Adirondack Park is larger than Yellowstone, Yosemite, and the
        Grand Canyon combined, yet it’s dotted with communities where people live, work, and play amidst nature. Visitors come year-round
        to hike High Peaks trails, paddle across mirror-like waters, or ski through snow-covered forests. The area’s pristine beauty,
        rich history, and commitment to conservation create a unique balance between wild preservation and human presence, making
        the Adirondacks a timeless escape into natural tranquility.
        """;

    public void testDefaults() {
        // Default of 300 is huge, only one chunk returned in this case
        verifyChunks(null, 1);
    }

    public void testSpecifiedChunkingSettings() {
        // We can't randomize here, because we're testing on specifically specified chunk size that's variable.
        int chunkSize = 25;
        int expectedNumChunks = 6;
        ChunkingSettings chunkingSettings = new SentenceBoundaryChunkingSettings(chunkSize, 0);
        verifyChunks(chunkingSettings, expectedNumChunks);
    }

    public void testRandomChunkingSettings() {
        ChunkingSettings chunkingSettings = createRandomChunkingSettings();
        List<String> result = process(PARAGRAPH_INPUT, chunkingSettings);
        assertNotNull(result);
        assertFalse(result.isEmpty());
        // Actual results depend on chunking settings passed in
    }

    // Paranoia check, this test will fail if we add new chunking settings options without updating the Chunk function
    public void testChunkDefinesAllAllowedChunkingSettingsOptions() {
        Set<String> allowedOptions = ALLOWED_CHUNKING_SETTING_OPTIONS.keySet();
        Set<String> allOptions = Arrays.stream(ChunkingSettingsOptions.values())
            .map(ChunkingSettingsOptions::toString)
            .collect(Collectors.toSet());

        assertEquals(allOptions, allowedOptions);
    }

    private void verifyChunks(ChunkingSettings chunkingSettings, int expectedNumChunksReturned) {
        ChunkingSettings chunkingSettingsOrDefault = chunkingSettings != null ? chunkingSettings : DEFAULT_CHUNKING_SETTINGS;
        List<String> expected = chunkText(PARAGRAPH_INPUT, chunkingSettingsOrDefault).stream().map(String::trim).toList();

        List<String> result = process(PARAGRAPH_INPUT, chunkingSettingsOrDefault);
        assertThat(result.size(), equalTo(expectedNumChunksReturned));
        assertThat(result, equalTo(expected));
    }

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    private List<String> process(String str, ChunkingSettings chunkingSettings) {
        MapExpression optionsMap = chunkingSettings == null ? null : createChunkingSettings(chunkingSettings);

        try (
            ExpressionEvaluator eval = evaluator(new Chunk(Source.EMPTY, field("field", DataType.KEYWORD), optionsMap)).get(
                driverContext(breakers)
            );
            Block block = eval.eval(row(List.of(new BytesRef(str))))
        ) {
            if (block.isNull(0)) {
                return null;
            }
            Object result = toJavaObject(block, 0);
            if (result instanceof BytesRef bytesRef) {
                return List.of(bytesRef.utf8ToString());
            } else {
                @SuppressWarnings("unchecked")
                List<BytesRef> list = (List<BytesRef>) result;
                return list.stream().map(BytesRef::utf8ToString).toList();
            }
        }
    }
}
