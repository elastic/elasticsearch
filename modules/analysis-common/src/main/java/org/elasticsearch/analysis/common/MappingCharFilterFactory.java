/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractCharFilterFactory;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.NormalizingCharFilterFactory;

import java.io.Reader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MappingCharFilterFactory extends AbstractCharFilterFactory implements NormalizingCharFilterFactory {

    private final NormalizeCharMap normMap;

    MappingCharFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);

        List<String> rules = Analysis.getWordList(env, settings, "mappings");
        if (rules == null) {
            throw new IllegalArgumentException("mapping requires either `mappings` or `mappings_path` to be configured");
        }

        NormalizeCharMap.Builder normMapBuilder = new NormalizeCharMap.Builder();
        parseRules(rules, normMapBuilder);
        normMap = normMapBuilder.build();
    }

    @Override
    public Reader create(Reader tokenStream) {
        return new MappingCharFilter(normMap, tokenStream);
    }

    // source => target
    private static Pattern rulePattern = Pattern.compile("(.*)\\s*=>\\s*(.*)\\s*$");

    /**
     * parses a list of MappingCharFilter style rules into a normalize char map
     */
    private void parseRules(List<String> rules, NormalizeCharMap.Builder map) {
        for (String rule : rules) {
            Matcher m = rulePattern.matcher(rule);
            if (m.find() == false) {
                throw new RuntimeException("Invalid Mapping Rule : [" + rule + "]");
            }
            String lhs = parseString(m.group(1).trim());
            String rhs = parseString(m.group(2).trim());
            if (lhs == null || rhs == null) throw new RuntimeException("Invalid Mapping Rule : [" + rule + "]. Illegal mapping.");
            map.add(lhs, rhs);
        }
    }

    char[] out = new char[256];

    private String parseString(String s) {
        int readPos = 0;
        int len = s.length();
        int writePos = 0;
        while (readPos < len) {
            char c = s.charAt(readPos++);
            if (c == '\\') {
                if (readPos >= len) throw new RuntimeException("Invalid escaped char in [" + s + "]");
                c = s.charAt(readPos++);
                switch (c) {
                    case '\\' -> c = '\\';
                    case 'n' -> c = '\n';
                    case 't' -> c = '\t';
                    case 'r' -> c = '\r';
                    case 'b' -> c = '\b';
                    case 'f' -> c = '\f';
                    case 'u' -> {
                        if (readPos + 3 >= len) throw new RuntimeException("Invalid escaped char in [" + s + "]");
                        c = (char) Integer.parseInt(s.substring(readPos, readPos + 4), 16);
                        readPos += 4;
                    }
                }
            }
            out[writePos++] = c;
        }
        return new String(out, 0, writePos);
    }

}
