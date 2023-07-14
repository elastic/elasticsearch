/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal.metering;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.xcontent.XContentParser;

import java.util.List;
import java.util.function.Function;

public interface MeteringCallback {


    default XContentParser wrapParser(XContentParser context) {
        return context;
    }

    default void reportDocumentParsed(XContentParser context) {

    }
}
