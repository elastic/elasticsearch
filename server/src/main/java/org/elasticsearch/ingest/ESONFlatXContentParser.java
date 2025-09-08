/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

public class ESONFlatXContentParser extends ESONXContentParser {

    private final List<ESONEntry> keyArray;
    private int currentIndex = 0;

    public ESONFlatXContentParser(
        List<ESONEntry> keyArray,
        ESONSource.Values esonFlat,
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        XContentType xContentType
    ) {
        super(esonFlat, registry, deprecationHandler, xContentType);
        this.keyArray = keyArray;
    }

    protected ESONEntry nextEntry() {
        return keyArray.get(currentIndex++);
    }
}
