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

package org.elasticsearch.index.analysis;

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
        super(indexSettings, name, settings);
        config = getIcuConfig(environment, settings);
    }

    @Override
    public Tokenizer create() {
        if (config == null) {
            return new ICUTokenizer();
        }else{
            return new ICUTokenizer(config);
        }
    }

    private ICUTokenizerConfig getIcuConfig(Environment env, Settings settings) {
        Map<Integer, String> tailored = new HashMap<>();

        try {
            List<String> ruleFiles = settings.getAsList(RULE_FILES);

            for (String scriptAndResourcePath : ruleFiles) {
                int colonPos = scriptAndResourcePath.indexOf(":");
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
                ICUTokenizerConfig config = new DefaultICUTokenizerConfig(true, true) {
                    @Override
                    public BreakIterator getBreakIterator(int script) {
                        if (breakers[script] != null) {
                            return (BreakIterator) breakers[script].clone();
                        } else {
                            return super.getBreakIterator(script);
                        }
                    }
                };
                return config;
            }
        } catch (Exception e) {
            throw new ElasticsearchException("failed to load ICU rule files", e);
        }
    }

    //parse a single RBBi rule file
    private BreakIterator parseRules(String filename, Environment env) throws IOException {

        final Path path = env.configFile().resolve(filename);
        String rules = Files.readAllLines(path)
            .stream()
            .filter((v) -> v.startsWith("#") == false)
            .collect(Collectors.joining("\n"));

        return new RuleBasedBreakIterator(rules.toString());
    }
}
