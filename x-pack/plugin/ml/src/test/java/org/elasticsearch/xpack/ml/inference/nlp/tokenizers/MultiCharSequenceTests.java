/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MultiCharSequenceTests extends ESTestCase {

    public void testMultiCharSequence() {

        {
            CharSequence sequence = new MultiCharSequence(List.of("This is a test"));
            assertThat(sequence.length(), equalTo("This is a test".length()));
            assertThat(sequence.charAt(5), equalTo("This is a test".charAt(5)));
            assertThat(sequence.subSequence(2, 7).toString(), equalTo("This is a test".subSequence(2, 7).toString()));
        }

        {
            CharSequence sequence = new MultiCharSequence(List.of("This is a test", "another string"));
            assertThat(sequence.length(), equalTo("This is a test".length() + "another string".length()));
            assertThat(sequence.charAt(5), equalTo("This is a test".charAt(5)));
            assertThat(sequence.charAt(16), equalTo('o'));
            assertThat(sequence.subSequence(2, 7).toString(), equalTo("This is a test".subSequence(2, 7).toString()));
            assertThat(sequence.subSequence(14, 18).toString(), equalTo("anot"));
            assertThat(sequence.subSequence(14, 28).toString(), equalTo("another string"));
            assertThat(sequence.subSequence(13, 18).toString(), equalTo("tanot"));
            assertThat(sequence.subSequence(13, 15).toString(), equalTo("ta"));
        }

        {
            CharSequence sequence = new MultiCharSequence(List.of("This is a test", "another string", "almost last"));
            assertThat(sequence.length(), equalTo("This is a test".length() + "another string".length() + "almost last".length()));
            assertThat(sequence.charAt(5), equalTo("This is a test".charAt(5)));
            assertThat(sequence.charAt(16), equalTo('o'));
            assertThat(sequence.subSequence(2, 7).toString(), equalTo("This is a test".subSequence(2, 7).toString()));
            assertThat(sequence.subSequence(14, 18).toString(), equalTo("anot"));
            assertThat(sequence.subSequence(14, 28).toString(), equalTo("another string"));
            assertThat(sequence.subSequence(13, 18).toString(), equalTo("tanot"));
            assertThat(sequence.subSequence(13, 15).toString(), equalTo("ta"));
            assertThat(sequence.subSequence(2, 30).toString(), equalTo("is is a testanother stringal"));
            assertThat(sequence.subSequence(30, 35).toString(), equalTo("most "));
        }

    }

}
