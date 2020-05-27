/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AnalysisStatsTests extends AbstractWireSerializingTestCase<AnalysisStats> {

    @Override
    protected Reader<AnalysisStats> instanceReader() {
        return AnalysisStats::new;
    }

    private static IndexFeatureStats randomStats(String name) {
        IndexFeatureStats stats = new IndexFeatureStats(name);
        stats.indexCount = randomIntBetween(1, 5);
        stats.count = randomIntBetween(stats.indexCount, 10);
        return stats;
    }

    @Override
    protected AnalysisStats createTestInstance() {
        Set<IndexFeatureStats> charFilters = new HashSet<>();
        if (randomBoolean()) {
            charFilters.add(randomStats("pattern_replace"));
        }

        Set<IndexFeatureStats> tokenizers = new HashSet<>();
        if (randomBoolean()) {
            tokenizers.add(randomStats("whitespace"));
        }

        Set<IndexFeatureStats> tokenFilters = new HashSet<>();
        if (randomBoolean()) {
            tokenFilters.add(randomStats("stop"));
        }

        Set<IndexFeatureStats> analyzers = new HashSet<>();
        if (randomBoolean()) {
            tokenFilters.add(randomStats("english"));
        }

        Set<IndexFeatureStats> builtInCharFilters = new HashSet<>();
        if (randomBoolean()) {
            builtInCharFilters.add(randomStats("html_strip"));
        }

        Set<IndexFeatureStats> builtInTokenizers = new HashSet<>();
        if (randomBoolean()) {
            builtInTokenizers.add(randomStats("keyword"));
        }

        Set<IndexFeatureStats> builtInTokenFilters = new HashSet<>();
        if (randomBoolean()) {
            builtInTokenFilters.add(randomStats("trim"));
        }

        Set<IndexFeatureStats> builtInAnalyzers = new HashSet<>();
        if (randomBoolean()) {
            builtInAnalyzers.add(randomStats("french"));
        }
        return new AnalysisStats(charFilters, tokenizers, tokenFilters, analyzers,
                builtInCharFilters, builtInTokenizers, builtInTokenFilters, builtInAnalyzers);
    }

    @Override
    protected AnalysisStats mutateInstance(AnalysisStats instance) throws IOException {
        switch (randomInt(7)) {
        case 0:
            Set<IndexFeatureStats> charFilters = new HashSet<>(instance.getUsedCharFilterTypes());
            if (charFilters.removeIf(s -> s.getName().equals("pattern_replace")) == false) {
                charFilters.add(randomStats("pattern_replace"));
            }
            return new AnalysisStats(charFilters, instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(), instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 1:
            Set<IndexFeatureStats> tokenizers = new HashSet<>(instance.getUsedTokenizerTypes());
            if (tokenizers.removeIf(s -> s.getName().equals("whitespace")) == false) {
                tokenizers.add(randomStats("whitespace"));
            }
            return new AnalysisStats(instance.getUsedCharFilterTypes(), tokenizers,
                    instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(), instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 2:
            Set<IndexFeatureStats> tokenFilters = new HashSet<>(instance.getUsedTokenFilterTypes());
            if (tokenFilters.removeIf(s -> s.getName().equals("stop")) == false) {
                tokenFilters.add(randomStats("stop"));
            }
            return new AnalysisStats(instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(),
                    tokenFilters, instance.getUsedAnalyzerTypes(), instance.getUsedBuiltInCharFilters(),
                    instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 3:
           Set<IndexFeatureStats> analyzers = new HashSet<>(instance.getUsedAnalyzerTypes());
            if (analyzers.removeIf(s -> s.getName().equals("english")) == false) {
                analyzers.add(randomStats("english"));
            }
            return new AnalysisStats(instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(), instance.getUsedTokenFilterTypes(), analyzers,
                    instance.getUsedBuiltInCharFilters(), instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 4:
            Set<IndexFeatureStats> builtInCharFilters = new HashSet<>(instance.getUsedBuiltInCharFilters());
            if (builtInCharFilters.removeIf(s -> s.getName().equals("html_strip")) == false) {
                builtInCharFilters.add(randomStats("html_strip"));
            }
            return new AnalysisStats(instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(),
                    instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(), builtInCharFilters,
                    instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 5:
            Set<IndexFeatureStats> builtInTokenizers = new HashSet<>(instance.getUsedBuiltInTokenizers());
            if (builtInTokenizers.removeIf(s -> s.getName().equals("keyword")) == false) {
                builtInTokenizers.add(randomStats("keyword"));
            }
            return new AnalysisStats(instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(), instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(), builtInTokenizers, instance.getUsedBuiltInTokenFilters(),
                    instance.getUsedBuiltInAnalyzers());
        case 6:
            Set<IndexFeatureStats> builtInTokenFilters = new HashSet<>(instance.getUsedBuiltInTokenFilters());
            if (builtInTokenFilters.removeIf(s -> s.getName().equals("trim")) == false) {
                builtInTokenFilters.add(randomStats("trim"));
            }
            return new AnalysisStats(instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(), instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(), instance.getUsedBuiltInTokenizers(), builtInTokenFilters,
                    instance.getUsedBuiltInAnalyzers());
        case 7:
            Set<IndexFeatureStats> builtInAnalyzers = new HashSet<>(instance.getUsedBuiltInAnalyzers());
            if (builtInAnalyzers.removeIf(s -> s.getName().equals("french")) == false) {
                builtInAnalyzers.add(randomStats("french"));
            }
            return new AnalysisStats(instance.getUsedCharFilterTypes(),
                    instance.getUsedTokenizerTypes(), instance.getUsedTokenFilterTypes(), instance.getUsedAnalyzerTypes(),
                    instance.getUsedBuiltInCharFilters(), instance.getUsedBuiltInTokenizers(), instance.getUsedBuiltInTokenFilters(),
                    builtInAnalyzers);
        default:
            throw new AssertionError();
        }

    }
}
