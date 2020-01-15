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

        return new IndexFeatureSetUsage(fields, charFilters, tokenizers, tokenFilters, analyzers);
    }

    @Override
    protected IndexFeatureSetUsage mutateInstance(IndexFeatureSetUsage instance) throws IOException {
        switch (randomInt(4)) {
        case 0:
            Set<String> fields = new HashSet<>(instance.getUsedFieldTypes());
            if (fields.add("keyword") == false) {
                fields.remove("keyword");
            }
            return new IndexFeatureSetUsage(fields, instance.getUsedCharFilterTypes(), instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes());
        case 1:
            Set<String> charFilters = new HashSet<>(instance.getUsedCharFilterTypes());
            if (charFilters.add("pattern_replace") == false) {
                charFilters.remove("pattern_replace");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), charFilters, instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes());
        case 2:
            Set<String> tokenizers = new HashSet<>(instance.getUsedTokenizerTypes());
            if (tokenizers.add("whitespace") == false) {
                tokenizers.remove("whitespace");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), instance.getUsedCharFilterTypes(), tokenizers,
                    instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes());
        case 3:
            Set<String> tokenFilters = new HashSet<>(instance.getUsedTokenFilterTypes());
            if (tokenFilters.add("stop") == false) {
                tokenFilters.remove("stop");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), instance.getUsedCharFilterTypes(), instance.getUsedTokenizerTypes(),
                    tokenFilters, instance.getUsedAnalyzerTypes());
        case 4:
            Set<String> analyzers = new HashSet<>(instance.getUsedAnalyzerTypes());
            if (analyzers.add("english") == false) {
                analyzers.remove("english");
            }
            return new IndexFeatureSetUsage(instance.getUsedFieldTypes(), instance.getUsedCharFilterTypes(), instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(), analyzers);
        default:
            throw new AssertionError();
        }
    }
}
