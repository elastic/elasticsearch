/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class NerProcessorTests extends ESTestCase {

    public void testBuildIobMap_WithDefault() {
        NerProcessor.IobTag[] map = NerProcessor.buildIobMap(randomBoolean() ? null : Collections.emptyList());
        for (int i=0; i<map.length; i++) {
            assertEquals(i, map[i].ordinal());
        }
    }

    public void testBuildIobMap_Reordered() {
        NerProcessor.IobTag[] tags = new NerProcessor.IobTag[]{
            NerProcessor.IobTag.I_MISC,
            NerProcessor.IobTag.O,
            NerProcessor.IobTag.B_MISC,
            NerProcessor.IobTag.I_PER
        };

        List<String> classLabels = Arrays.stream(tags).map(NerProcessor.IobTag::toString).collect(Collectors.toList());
        NerProcessor.IobTag[] map = NerProcessor.buildIobMap(classLabels);
        for (int i=0; i<map.length; i++) {
            assertNotEquals(i, map[i].ordinal());
        }
        assertArrayEquals(tags, map);
    }

    public void testValidate_DuplicateLabels() {
        NerProcessor.IobTag[] tags = new NerProcessor.IobTag[]{
            NerProcessor.IobTag.I_MISC,
            NerProcessor.IobTag.B_MISC,
            NerProcessor.IobTag.B_MISC,
            NerProcessor.IobTag.O,
        };

        List<String> classLabels = Arrays.stream(tags).map(NerProcessor.IobTag::toString).collect(Collectors.toList());

        ValidationException ve = expectThrows(ValidationException.class, () -> new NerProcessor(mock(BertTokenizer.class), classLabels));
        assertThat(ve.getMessage(),
            containsString("the classification label [B_MISC] is duplicated in the list [I_MISC, B_MISC, B_MISC, O]"));
    }

    public void testValidate_NotAEntityLabel() {
        List<String> classLabels = List.of("foo", NerProcessor.IobTag.B_MISC.toString());

        ValidationException ve = expectThrows(ValidationException.class, () -> new NerProcessor(mock(BertTokenizer.class), classLabels));
        assertThat(ve.getMessage(), containsString("classification label [foo] is not an entity I-O-B tag"));
        assertThat(ve.getMessage(),
            containsString("Valid entity I-O-B tags are [O, B_MISC, I_MISC, B_PER, I_PER, B_ORG, I_ORG, B_LOC, I_LOC]"));
    }
}
