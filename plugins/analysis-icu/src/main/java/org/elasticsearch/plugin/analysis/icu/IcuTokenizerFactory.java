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
import org.elasticsearch.exception.ElasticsearchException;
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

public class IcuTokenizerFactory extends AbstractTokenizerFactory {

    private final ICUTokenizerConfig config;
    private static final String RULE_FILES = "rule_files";

    public IcuTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        config = getIcuConfig(environment, settings);
    }

    @Override
    public Tokenizer create() {
        if (config == null) {
            return new ICUTokenizer();
        } else {
            return new ICUTokenizer(config);
        }
    }

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

    // parse a single RBBi rule file
    private static BreakIterator parseRules(String filename, Environment env) throws IOException {

        final Path path = env.configDir().resolve(filename);
        String rules = Files.readAllLines(path).stream().filter((v) -> v.startsWith("#") == false).collect(Collectors.joining("\n"));

        return new RuleBasedBreakIterator(rules.toString());
    }
}
