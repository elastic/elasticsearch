/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AnalysisUtil {

    /**
     * other attribute extract object.
     * Extracted object group by AttributeClassName
     *
     * @param stream current TokenStream
     * @param includeAttributes filtering attributes
     * @return Map&lt;key value&gt;
     */
    public static Map<String, Object> extractExtendedAttributes(TokenStream stream, final Set<String> includeAttributes) {
        final Map<String, Object> extendedAttributes = new TreeMap<>();

        stream.reflectWith((attClass, key, value) -> {
            if (CharTermAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (PositionIncrementAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (OffsetAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (TypeAttribute.class.isAssignableFrom(attClass)) {
                return;
            }
            if (includeAttributes == null || includeAttributes.isEmpty() || includeAttributes.contains(key.toLowerCase(Locale.ROOT))) {
                if (value instanceof BytesRef) {
                    final BytesRef p = (BytesRef) value;
                    value = p.toString();
                }
                extendedAttributes.put(key, value);
            }
        });

        return extendedAttributes;
    }

    public static TokenStream createStackedTokenStream(
        String source,
        CharFilterFactory[] charFilterFactories,
        TokenizerFactory tokenizerFactory,
        TokenFilterFactory[] tokenFilterFactories,
        int current
    ) {
        Reader reader = new StringReader(source);
        for (CharFilterFactory charFilterFactory : charFilterFactories) {
            reader = charFilterFactory.create(reader);
        }
        Tokenizer tokenizer = tokenizerFactory.create();
        tokenizer.setReader(reader);
        return createStackedTokenStream(tokenizer, tokenFilterFactories, current);
    }

    public static TokenStream createStackedTokenStream(TokenStream tokenStream, TokenFilterFactory[] tokenFilterFactories) {
        return createStackedTokenStream(tokenStream, tokenFilterFactories, tokenFilterFactories.length);
    }

    public static TokenStream createStackedTokenStream(TokenStream tokenStream, TokenFilterFactory[] tokenFilterFactories, int current) {
        for (int i = 0; i < current; i++) {
            tokenStream = tokenFilterFactories[i].create(tokenStream);
        }
        return tokenStream;
    }

    public static String writeCharStream(Reader input) {
        final int BUFFER_SIZE = 1024;
        char[] buf = new char[BUFFER_SIZE];
        int len;
        StringBuilder sb = new StringBuilder();
        do {
            try {
                len = input.read(buf, 0, BUFFER_SIZE);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to analyze (charFiltering)", e);
            }
            if (len > 0) {
                sb.append(buf, 0, len);
            }
        } while (len == BUFFER_SIZE);
        return sb.toString();
    }
}
