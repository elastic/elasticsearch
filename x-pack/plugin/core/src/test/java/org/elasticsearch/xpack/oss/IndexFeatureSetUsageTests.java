/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.oss;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class IndexFeatureSetUsageTests extends AbstractWireSerializingTestCase<IndexFeatureSetUsage> {

    @Override
    protected Reader<IndexFeatureSetUsage> instanceReader() {
        return IndexFeatureSetUsage::new;
    }

    @Override
    protected IndexFeatureSetUsage createTestInstance() {
        Set<String> fields = new HashSet<>();
        if (randomBoolean()) {
            fields.add("keyword");
        }
        if (randomBoolean()) {
            fields.add("integer");
        }

        Set<String> charFilters = new HashSet<>();
        if (randomBoolean()) {
            charFilters.add("pattern_replace");
        }

        Set<String> tokenizers = new HashSet<>();
        if (randomBoolean()) {
            tokenizers.add("whitespace");
        }

        Set<String> tokenFilters = new HashSet<>();
        if (randomBoolean()) {
            tokenFilters.add("stop");
        }

        Set<String> analyzers = new HashSet<>();
        if (randomBoolean()) {
            tokenFilters.add("english");
        }

        Set<String> builtInCharFilters = new HashSet<>();
        if (randomBoolean()) {
            builtInCharFilters.add("html_strip");
        }

        Set<String> builtInTokenizers = new HashSet<>();
        if (randomBoolean()) {
            builtInTokenizers.add("keyword");
        }

        Set<String> builtInTokenFilters = new HashSet<>();
        if (randomBoolean()) {
            builtInTokenFilters.add("trim");
        }

        Set<String> builtInAnalyzers = new HashSet<>();
        if (randomBoolean()) {
            builtInAnalyzers.add("french");
        }

        return new IndexFeatureSetUsage(fields,
                charFilters, tokenizers, tokenFilters, analyzers,
                builtInCharFilters, builtInTokenizers, builtInTokenFilters, builtInAnalyzers);
    }

    @Override
    protected IndexFeatureSetUsage mutateInstance(IndexFeatureSetUsage instance) throws IOException {
        switch (randomInt(8)) {
        case 0:
            Set<String> fields = new HashSet<>(instance.getUsedFieldTypes());
            if (fields.add("keyword") == false) {
                fields.remove("keyword");
            }
            return new IndexFeatureSetUsage(fields, instance.getUsedCharFilterTypes(), instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(), instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 1:
            Set<String> charFilters = new HashSet<>(instance.getUsedCharFilterTypes());
            if (charFilters.add("pattern_replace") == false) {
                charFilters.remove("pattern_replace");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), charFilters, instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(), instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 2:
            Set<String> tokenizers = new HashSet<>(instance.getUsedTokenizerTypes());
            if (tokenizers.add("whitespace") == false) {
                tokenizers.remove("whitespace");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), instance.getUsedCharFilterTypes(), tokenizers,
                    instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(), instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 3:
            Set<String> tokenFilters = new HashSet<>(instance.getUsedTokenFilterTypes());
            if (tokenFilters.add("stop") == false) {
                tokenFilters.remove("stop");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(),
                    tokenFilters, instance.getUsedAnalyzerTypes(), instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 4:
           Set<String> analyzers = new HashSet<>(instance.getUsedAnalyzerTypes());
            if (analyzers.add("english") == false) {
                analyzers.remove("english");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(), instance.getUsedTokenFilterTypes(), analyzers,
                    instance.getUsedBuiltInCharFilters(), instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 5:
            Set<String> builtInCharFilters = new HashSet<>(instance.getUsedBuiltInCharFilters());
            if (builtInCharFilters.add("html_strip") == false) {
                builtInCharFilters.remove("html_strip");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(), builtInCharFilters,
                    instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 6:
            Set<String> builtInTokenizers = new HashSet<>(instance.getUsedBuiltInTokenizers());
            if (builtInTokenizers.add("keyword") == false) {
                builtInTokenizers.remove("keyword");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(), instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(), builtInTokenizers, instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 7:
            Set<String> builtInTokenFilters = new HashSet<>(instance.getUsedBuiltInTokenFilters());
            if (builtInTokenFilters.add("trim") == false) {
                builtInTokenFilters.remove("trim");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(), instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(), instance.getUsedBuiltInTokenizers(), builtInTokenFilters,
                    instance.getUsedBuiltInAnalyzers());
        case 8:
            Set<String> builtInAnalyzers = new HashSet<>(instance.getUsedBuiltInAnalyzers());
            if (builtInAnalyzers.add("french") == false) {
                builtInAnalyzers.remove("french");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(), instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(), instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    builtInAnalyzers);
        default:
            throw new AssertionError();
        }
    }
}
