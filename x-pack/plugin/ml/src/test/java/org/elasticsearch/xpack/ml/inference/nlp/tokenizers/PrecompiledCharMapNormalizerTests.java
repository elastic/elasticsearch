/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.OptionalInt;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PrecompiledCharMapNormalizerTests extends ESTestCase {

    public void testCommonPrefix() throws IOException {
        PrecompiledCharMapNormalizer parsed = loadTestCharMap();
        OptionalInt local = parsed.commonPrefix("\uFB01".getBytes(StandardCharsets.UTF_8));
        assertThat(local.isPresent(), is(true));
        assertThat(local.getAsInt(), equalTo(2130));
        String transformed = parsed.normalize("\uFB01");
        assertThat(transformed, equalTo("fi"));
        assertThat(parsed.normalize("𝔾"), equalTo("G"));
        assertThat(parsed.normalize("\uD835\uDD60"), equalTo("o"));
        assertThat(parsed.normalize("\u200D"), equalTo(" "));
        assertThat(parsed.normalize("เขาไม่ได้พูดสักคำ"), equalTo("เขาไม\u0E48ได\u0E49พ\u0E39ดส\u0E31กค\u0E4Dา"));
    }

    public void testAdverseScenario() throws IOException {
        PrecompiledCharMapNormalizer parsed = loadTestCharMap();
        assertThat(parsed.normalize("คำ"), equalTo("ค\u0e4dา"));
    }

    public void testAdverseScenarioHindi() throws IOException {
        PrecompiledCharMapNormalizer parsed = loadTestCharMap();
        assertThat(parsed.normalize("ड़ी दुख"), equalTo("ड\u093cी द\u0941ख"));
    }

    public void testTwoCharUnicode() throws IOException {
        PrecompiledCharMapNormalizer parsed = loadTestCharMap();
        assertThat(parsed.normalize("آ"), equalTo("آ"));
    }

    private static PrecompiledCharMapNormalizer loadTestCharMap() throws IOException {
        PreCompiledCharMap map = PreCompiledCharMap.fromResource(
            "/org.elasticsearch.xpack.ml.inference.nlp.tokenizers/precompiled_char_map.json"
        );
        return PrecompiledCharMapNormalizer.fromBase64Str(map.charMapStr());
    }
}
