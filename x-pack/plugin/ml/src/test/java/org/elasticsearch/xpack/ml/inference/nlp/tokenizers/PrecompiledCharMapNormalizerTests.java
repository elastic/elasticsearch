/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.equalTo;

public class PrecompiledCharMapNormalizerTests extends ESTestCase {

    public void testCommonPrefix() throws IOException {
        PrecompiledCharMapNormalizer.Config parsed = loadTestCharMap();
        assertNormalization("\u0008", parsed, "");
        assertNormalization("\uFB01", parsed, "fi");
        assertNormalization("𝔾", parsed, "G");
        assertNormalization("\uD835\uDD60", parsed, "o");
        assertNormalization("\u200D", parsed, " ");
        assertNormalization("เขาไม่ได้พูดสักคำ", parsed, "เขาไม\u0E48ได\u0E49พ\u0E39ดส\u0E31กค\u0E4Dา");
    }

    public void testAdverseScenario() throws IOException {
        PrecompiledCharMapNormalizer.Config parsed = loadTestCharMap();
        assertNormalization("คำ", parsed, "ค\u0e4dา");
    }

    public void testAdverseScenarioHindi() throws IOException {
        PrecompiledCharMapNormalizer.Config parsed = loadTestCharMap();
        assertNormalization("ड़ी दुख", parsed, "ड\u093cी द\u0941ख");
    }

    public void testTwoCharUnicode() throws IOException {
        PrecompiledCharMapNormalizer.Config parsed = loadTestCharMap();
        assertNormalization("آ", parsed, "آ");
    }

    public void testWhitespaceScenario() throws IOException {
        PrecompiledCharMapNormalizer.Config parsed = loadTestCharMap();
        assertNormalization("​​από", parsed, "  από");
    }

    private void assertNormalization(String input, PrecompiledCharMapNormalizer.Config config, String expected) throws IOException {
        PrecompiledCharMapNormalizer normalizer = new PrecompiledCharMapNormalizer(
            config.offsets(),
            config.utf8str(),
            new StringReader(input)
        );
        char[] output = new char[64];
        int read = normalizer.read(output, 0, 64);
        if (read <= 0) {
            assertThat("", equalTo(expected));
        } else {
            assertThat(new String(output, 0, read), equalTo(expected));
        }
    }

    static PrecompiledCharMapNormalizer.Config loadTestCharMap() throws IOException {
        PreCompiledCharMap map = PreCompiledCharMap.fromResource(
            "/org.elasticsearch.xpack.ml.inference.nlp.tokenizers/precompiled_char_map.json"
        );
        return PrecompiledCharMapNormalizer.fromBase64Str(map.charMapStr());
    }
}
