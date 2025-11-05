/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.lang.UProperty;
import com.ibm.icu.lang.UScript;
import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.text.RuleBasedBreakIterator;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.icu.segmentation.DefaultICUTokenizerConfig;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizerConfig;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Factory for creating ICU tokenizers that perform language-aware text segmentation.
 * Supports custom rule-based break iteration for specific scripts.
 */
public class IcuTokenizerFactory extends AbstractTokenizerFactory {

    private final ICUTokenizerConfig config;
    private static final String RULE_FILES = "rule_files";

    /**
     * Constructs an ICU tokenizer factory with optional custom segmentation rules.
     *
     * @param indexSettings the index settings
     * @param environment the environment for resolving rule files
     * @param name the tokenizer name
     * @param settings the tokenizer settings containing:
     *        <ul>
     *        <li>rule_files: list of "script:filepath" pairs for custom segmentation rules</li>
     *        </ul>
     * @throws IllegalArgumentException if rule file format is invalid
     * @throws ElasticsearchException if rule files cannot be loaded or parsed
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "tokenizer": {
     *   "my_icu_tokenizer": {
     *     "type": "icu_tokenizer"
     *   }
     * }
     *
     * // With custom rules
     * "tokenizer": {
     *   "custom_icu": {
     *     "type": "icu_tokenizer",
     *     "rule_files": ["Latin:latin-rules.rbbi", "Hira:hiragana-rules.rbbi"]
     *   }
     * }
     * }</pre>
     */
    public IcuTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        config = getIcuConfig(environment, settings);
    }

    /**
     * Creates a new ICU tokenizer instance with the configured segmentation rules.
     *
     * @return a new {@link ICUTokenizer} with default or custom configuration
     */
    @Override
    public Tokenizer create() {
        if (config == null) {
            return new ICUTokenizer();
        } else {
            return new ICUTokenizer(config);
        }
    }

    /**
     * Builds an ICU tokenizer configuration from settings by loading and parsing rule files.
     *
     * @param env the environment for resolving file paths
     * @param settings the tokenizer settings
     * @return an {@link ICUTokenizerConfig} with custom rules, or null if no custom rules are defined
     * @throws IllegalArgumentException if rule file format is invalid
     * @throws ElasticsearchException if rule files cannot be loaded
     */
    private static ICUTokenizerConfig getIcuConfig(Environment env, Settings settings) {
        Map<Integer, String> tailored = new HashMap<>();

        try {
            List<String> ruleFiles = settings.getAsList(RULE_FILES);

            for (String scriptAndResourcePath : ruleFiles) {
                int colonPos = scriptAndResourcePath.indexOf(':');
                if (colonPos == -1 || colonPos == scriptAndResourcePath.length() - 1) {
                    throw new IllegalArgumentException(RULE_FILES + " should contain comma-separated \"code:rulefile\" pairs");
                }

                String scriptCode = scriptAndResourcePath.substring(0, colonPos).trim();
                String resourcePath = scriptAndResourcePath.substring(colonPos + 1).trim();
                tailored.put(UCharacter.getPropertyValueEnum(UProperty.SCRIPT, scriptCode), resourcePath);
            }

            if (tailored.isEmpty()) {
                return null;
            } else {
                final BreakIterator breakers[] = new BreakIterator[UScript.CODE_LIMIT];
                for (Map.Entry<Integer, String> entry : tailored.entrySet()) {
                    int code = entry.getKey();
                    String resourcePath = entry.getValue();
                    breakers[code] = parseRules(resourcePath, env);
                }
                // cjkAsWords nor myanmarAsWords are not configurable yet.
                ICUTokenizerConfig tokenizerConfig = new DefaultICUTokenizerConfig(true, true) {
                    @Override
                    public RuleBasedBreakIterator getBreakIterator(int script) {
                        if (breakers[script] != null) {
                            return (RuleBasedBreakIterator) breakers[script].clone();
                        } else {
                            return super.getBreakIterator(script);
                        }
                    }
                };
                return tokenizerConfig;
            }
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load ICU rule files", e);
        }
    }

    /**
     * Parses a single rule-based break iterator (RBBi) rule file.
     *
     * @param filename the rule file name relative to the config directory
     * @param env the environment for resolving the file path
     * @return a {@link BreakIterator} configured with the parsed rules
     * @throws IOException if the file cannot be read
     */
    private static BreakIterator parseRules(String filename, Environment env) throws IOException {

        final Path path = env.configDir().resolve(filename);
        String rules = Files.readAllLines(path).stream().filter((v) -> v.startsWith("#") == false).collect(Collectors.joining("\n"));

        return new RuleBasedBreakIterator(rules.toString());
    }
}
