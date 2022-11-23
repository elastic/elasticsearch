/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident.LanguageExamples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;

public class BpeTokenReaderTests extends ESTestCase {

    private static final Pattern pattern = Pattern.compile(
        "'s|'t|'re|'ve|'m|'ll|'d| ?\\p{L}+| ?\\p{N}+| ?[^\\s\\p{L}\\p{N}]+|\\s+(?!\\S)|\\s+"
    );

    public void testAdverseStringsMatched() {
        String[] testStrings = new String[] {
            """
                 'lk 'll it'll 1'll can'tk *('ll#$%^& out ''ll we'lk  This is a    big complicated\s
                 \s
                 ajs  \s
                \t
                   hh
                t $%^8 t^ 8oi I'll figure 'lk it out '' we'lk  12#$567UJSDKGhbllasdkjn;;; ; ;""",
            " ",
            "  ",
            " \n ",
            "",
            "justasingleword",
            " justasingleword",
            "#$%^&*",
            " #$%^&*",
            "-23456789",
            " -23456789",
            "23456789",
            " 23456789" };
        for (String str : testStrings) {
            Matcher matcher = pattern.matcher(str);
            List<String> results = new ArrayList<>();
            while (matcher.find()) {
                results.add(str.substring(matcher.start(), matcher.end()));
            }
            BpeTokenReader tokenReader = new BpeTokenReader(str);
            Optional<? extends CharSequence> sequence;
            List<String> otherResults = new ArrayList<>();
            while ((sequence = tokenReader.next()).isPresent()) {
                otherResults.add(sequence.get().toString());
            }
            assertThat(otherResults, equalTo(results));
        }
    }

    public void testTypicalMultiLanguageStrings() throws IOException {
        LanguageExamples examples = new LanguageExamples();
        for (var example : examples.getLanguageExamples()) {
            String str = example.getText();
            Matcher matcher = pattern.matcher(str);
            List<String> results = new ArrayList<>();
            while (matcher.find()) {
                results.add(str.substring(matcher.start(), matcher.end()));
            }
            BpeTokenReader tokenReader = new BpeTokenReader(str);
            Optional<? extends CharSequence> sequence;
            List<String> otherResults = new ArrayList<>();
            while ((sequence = tokenReader.next()).isPresent()) {
                otherResults.add(sequence.get().toString());
            }
            assertThat(otherResults, equalTo(results));
        }
    }

}
