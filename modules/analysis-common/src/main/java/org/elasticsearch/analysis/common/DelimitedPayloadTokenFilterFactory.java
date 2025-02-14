/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilter;
import org.apache.lucene.analysis.payloads.FloatEncoder;
import org.apache.lucene.analysis.payloads.IdentityEncoder;
import org.apache.lucene.analysis.payloads.IntegerEncoder;
import org.apache.lucene.analysis.payloads.PayloadEncoder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

public class DelimitedPayloadTokenFilterFactory extends AbstractTokenFilterFactory {

    public static final char DEFAULT_DELIMITER = '|';
    public static final PayloadEncoder DEFAULT_ENCODER = new FloatEncoder();

    static final String ENCODING = "encoding";
    static final String DELIMITER = "delimiter";

    private final char delimiter;
    private final PayloadEncoder encoder;

    DelimitedPayloadTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        String delimiterConf = settings.get(DELIMITER);
        if (delimiterConf != null) {
            delimiter = delimiterConf.charAt(0);
        } else {
            delimiter = DEFAULT_DELIMITER;
        }

        if (settings.get(ENCODING) != null) {
            if (settings.get(ENCODING).equals("float")) {
                encoder = new FloatEncoder();
            } else if (settings.get(ENCODING).equals("int")) {
                encoder = new IntegerEncoder();
            } else if (settings.get(ENCODING).equals("identity")) {
                encoder = new IdentityEncoder();
            } else {
                encoder = DEFAULT_ENCODER;
            }
        } else {
            encoder = DEFAULT_ENCODER;
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new DelimitedPayloadTokenFilter(tokenStream, delimiter, encoder);
    }

}
