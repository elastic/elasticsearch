/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.nori;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.plugin.analysis.api.TokenizerFactory;
import org.elasticsearch.plugin.api.NamedComponent;

@NamedComponent(name = "nori_tokenizer")
public class NoriTokenizerFactory implements TokenizerFactory {

    @Override
    public Tokenizer create() {
        return null;
    }
}
