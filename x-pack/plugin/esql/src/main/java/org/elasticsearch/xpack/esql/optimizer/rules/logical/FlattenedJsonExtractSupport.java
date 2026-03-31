/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;

import java.util.Collections;

/**
 * Helpers to lower {@code JSON_EXTRACT(flattened_root, "dotted.key")} to a synthetic dotted {@link FieldAttribute}
 * that loads via keyed flattened doc values.
 */
public final class FlattenedJsonExtractSupport {

    private FlattenedJsonExtractSupport() {}

    /**
     * @return {@code null} if {@code path} is a valid dotted subfield path for keyed flattened loading; otherwise an error detail string
     */
    public static String validateKeyedFlattenedPath(String path) {
        if (path == null || path.isEmpty()) {
            return "subfield path must not be empty";
        }
        if (path.charAt(0) == '.' || path.charAt(path.length() - 1) == '.') {
            return "subfield path must not start or end with '.'";
        }
        if (path.indexOf("..") >= 0) {
            return "subfield path must not contain empty segments";
        }
        if (path.indexOf('*') >= 0) {
            return "subfield path must not contain wildcards";
        }
        // Bracket / JSONPath / whitespace — fall back to JSON parsing on the root blob
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '[' || c == ']' || c == '$' || c == '\'' || c == '"' || Character.isWhitespace(c)) {
                return "subfield path must use simple dot notation for keyed flattened loading";
            }
        }
        return null;
    }

    public static boolean isKeyedFlattenedSubfieldPath(String path) {
        return validateKeyedFlattenedPath(path) == null;
    }

    /**
     * Synthetic attribute for {@code rootName + "." + dottedSubPath}, backed by {@link KeywordEsField}.
     */
    public static FieldAttribute syntheticKeyedSubfield(Source source, FieldAttribute root, String dottedSubPath) {
        String fullName = root.name() + "." + dottedSubPath;
        int lastDot = fullName.lastIndexOf('.');
        String parentName = lastDot > 0 ? fullName.substring(0, lastDot) : root.name();
        KeywordEsField field = new KeywordEsField(
            fullName,
            Collections.emptyMap(),
            true,
            Integer.MAX_VALUE,
            false,
            false,
            EsField.TimeSeriesFieldType.NONE
        );
        return new FieldAttribute(source, parentName, null, fullName, field, Nullability.TRUE, null, true);
    }
}
