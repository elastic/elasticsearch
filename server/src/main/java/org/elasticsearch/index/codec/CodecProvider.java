/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;

/**
 * Abstracts codec lookup by name, to make CodecService extensible.
 */
public interface CodecProvider {
    Codec codec(String name);

    /**
     * Returns all registered available codec names.
     */
    String[] availableCodecs();
}
