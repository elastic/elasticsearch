/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.xcontent.XContentParser;

public class FieldNameValidator {
    public static void ensureQualified(String fieldName, XContentParser parser) {
        if (!fieldName.contains(".")) {
            throw new ParsingException(parser.getTokenLocation(),
                "Field name '" + fieldName + "' is unqualified. Use a qualified name like 'object.field' to avoid future ambiguity.");
        }
    }
}
