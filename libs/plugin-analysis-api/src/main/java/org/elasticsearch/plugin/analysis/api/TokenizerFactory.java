/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.api;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.plugin.api.Extensible;
import org.elasticsearch.plugin.api.Nameable;

/**
 * An analysis component used to create tokenizers.
 */
@Extensible
public interface TokenizerFactory extends Nameable {

    /**
     * Creates a Tokenizer instance.
     * @return a tokenizer
     */
    Tokenizer create();
}
