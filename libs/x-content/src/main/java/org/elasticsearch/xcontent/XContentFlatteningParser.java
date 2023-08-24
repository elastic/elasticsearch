/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import java.io.IOException;

public class XContentFlatteningParser extends XContentSubParser {
    private final String parentName;
    private static final char DELIMITER = '.';

    public XContentFlatteningParser(XContentParser parser, String parentName) {
        super(parser);
        this.parentName = parentName;
    }

    @Override
    public String currentName() throws IOException {
        if (level() == 1) {
            return String.format("%s%s%s", parentName, DELIMITER, delegate().currentName());
        }
        return delegate().currentName();
    }
}
