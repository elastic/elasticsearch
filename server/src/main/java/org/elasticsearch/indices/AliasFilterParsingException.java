/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;

import java.io.IOException;

public final class AliasFilterParsingException extends ElasticsearchException {

    public AliasFilterParsingException(Index index, String name, String desc, Throwable ex) {
        super("[" + name + "], " + desc, ex);
        setIndex(index);
    }

    public AliasFilterParsingException(StreamInput in) throws IOException {
        super(in);
    }
}
