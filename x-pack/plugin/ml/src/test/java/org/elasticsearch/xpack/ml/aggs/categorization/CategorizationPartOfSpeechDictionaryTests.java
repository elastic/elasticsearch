/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationPartOfSpeechDictionary.PartOfSpeech;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class CategorizationPartOfSpeechDictionaryTests extends ESTestCase {

    public void testPartOfSpeech() throws IOException {

        CategorizationPartOfSpeechDictionary partOfSpeechDictionary = CategorizationPartOfSpeechDictionary.getInstance();

        assertThat(partOfSpeechDictionary.isInDictionary("hello"), is(true));
        assertThat(partOfSpeechDictionary.isInDictionary("Hello"), is(true));
        assertThat(partOfSpeechDictionary.isInDictionary("HELLO"), is(true));
        assertThat(partOfSpeechDictionary.isInDictionary("a"), is(true));
        assertThat(partOfSpeechDictionary.isInDictionary("service"), is(true));
        assertThat(partOfSpeechDictionary.isInDictionary("has"), is(true));
        assertThat(partOfSpeechDictionary.isInDictionary("started"), is(true));

        assertThat(partOfSpeechDictionary.isInDictionary(""), is(false));
        assertThat(partOfSpeechDictionary.isInDictionary("r"), is(false));
        assertThat(partOfSpeechDictionary.isInDictionary("hkjsdfg"), is(false));
        assertThat(partOfSpeechDictionary.isInDictionary("hello2"), is(false));
        assertThat(partOfSpeechDictionary.isInDictionary("HELLO2"), is(false));

        assertThat(partOfSpeechDictionary.getPartOfSpeech("ajksdf"), is(PartOfSpeech.NOT_IN_DICTIONARY));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("callback"), is(PartOfSpeech.UNKNOWN));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("House"), is(PartOfSpeech.NOUN));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("Houses"), is(PartOfSpeech.PLURAL));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("COMPLETED"), is(PartOfSpeech.VERB));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("heavy"), is(PartOfSpeech.ADJECTIVE));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("slowly"), is(PartOfSpeech.ADVERB));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("and"), is(PartOfSpeech.CONJUNCTION));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("without"), is(PartOfSpeech.PREPOSITION));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("gosh"), is(PartOfSpeech.INTERJECTION));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("hers"), is(PartOfSpeech.PRONOUN));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("the"), is(PartOfSpeech.DEFINITE_ARTICLE));
        assertThat(partOfSpeechDictionary.getPartOfSpeech("a"), is(PartOfSpeech.INDEFINITE_ARTICLE));
    }
}
