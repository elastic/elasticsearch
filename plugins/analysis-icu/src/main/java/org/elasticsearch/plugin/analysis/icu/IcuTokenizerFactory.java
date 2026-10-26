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
import org.elasticsearch.index.analysis.Analysis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcuTokenizerFactory extends AbstractTokenizerFactory {

    private final ICUTokenizerConfig config;
    private final Object sharingKey;
    private static final String RULE_FILES = "rule_files";

    public IcuTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        try {
            Map<Integer, String> rulesByScript = readRules(environment, settings);
            config = buildConfig(rulesByScript);
            // The rules can be large, so a SHA-256 of each script's rules stands in for them.
            Map<Integer, String> rulesSha256ByScript = new HashMap<>();
            for (Map.Entry<Integer, String> entry : rulesByScript.entrySet()) {
                rulesSha256ByScript.put(entry.getKey(), Analysis.sha256Hex(entry.getValue()));
            }
            this.sharingKey = new Key(Map.copyOf(rulesSha256ByScript));
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load ICU rule files", e);
        }
    }

    @Override
    public Tokenizer create() {
        if (config == null) {
            return new ICUTokenizer();
        } else {
            return new ICUTokenizer(config);
        }
    }

    // the RBBI rules of each tailored script, by script code
    private static Map<Integer, String> readRules(Environment env, Settings settings) throws IOException {
        Map<Integer, String> tailored = new HashMap<>();
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

        Map<Integer, String> rulesByScript = new HashMap<>();
        for (Map.Entry<Integer, String> entry : tailored.entrySet()) {
            rulesByScript.put(entry.getKey(), readRuleFile(entry.getValue(), env));
        }
        return rulesByScript;
    }

    private static ICUTokenizerConfig buildConfig(Map<Integer, String> rulesByScript) {
        if (rulesByScript.isEmpty()) {
            return null;
        }
        final BreakIterator breakers[] = new BreakIterator[UScript.CODE_LIMIT];
        for (Map.Entry<Integer, String> entry : rulesByScript.entrySet()) {
            breakers[entry.getKey()] = new RuleBasedBreakIterator(entry.getValue());
        }
        // cjkAsWords nor myanmarAsWords are not configurable yet.
        return new DefaultICUTokenizerConfig(true, true) {
            @Override
            public RuleBasedBreakIterator getBreakIterator(int script) {
                if (breakers[script] != null) {
                    return (RuleBasedBreakIterator) breakers[script].clone();
                } else {
                    return super.getBreakIterator(script);
                }
            }
        };
    }

    // read a single RBBi rule file, without its comments
    private static String readRuleFile(String filename, Environment env) throws IOException {
        final Path path = env.configDir().resolve(filename);
        return Files.readAllLines(path).stream().filter((v) -> v.startsWith("#") == false).collect(Collectors.joining("\n"));
    }

    @Override
    public Object sharingKey() {
        return sharingKey;
    }

    private record Key(Map<Integer, String> rulesSha256ByScript) {}
}
