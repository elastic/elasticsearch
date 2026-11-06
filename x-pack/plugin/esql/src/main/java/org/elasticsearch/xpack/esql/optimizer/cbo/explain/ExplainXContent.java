/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * Shared rendering rules for the optimizer report, so that every type writes numbers, enums, and optional fields the same way.
 * A non-finite number is written as {@code null}, and optional fields that are null or empty lists are omitted.
 */
final class ExplainXContent {
    private ExplainXContent() {}

    /** Writes an integral value as a JSON integer and any other finite value as a JSON double. */
    static void numberField(XContentBuilder b, String name, double v) throws IOException {
        if (Double.isFinite(v) == false) {
            b.nullField(name);
        } else if (v == Math.rint(v) && Math.abs(v) < 1e15) {
            b.field(name, (long) v);
        } else {
            b.field(name, v);
        }
    }

    /** Writes a value rounded to the nearest integer. */
    static void roundedField(XContentBuilder b, String name, double v) throws IOException {
        if (Double.isFinite(v) == false) {
            b.nullField(name);
        } else {
            b.field(name, Math.round(v));
        }
    }

    /** Writes a value as a JSON double without converting integral values. */
    static void doubleField(XContentBuilder b, String name, double v) throws IOException {
        if (Double.isFinite(v) == false) {
            b.nullField(name);
        } else {
            b.field(name, v);
        }
    }

    static String lowercase(Enum<?> e) {
        return e.name().toLowerCase(Locale.ROOT);
    }

    static void optionalField(XContentBuilder b, String name, @Nullable String value) throws IOException {
        if (value != null) {
            b.field(name, value);
        }
    }

    static void optionalObject(XContentBuilder b, String name, @Nullable ToXContent value, ToXContent.Params params) throws IOException {
        if (value != null) {
            b.field(name);
            value.toXContent(b, params);
        }
    }

    /** Writes an array of objects, omitting the field when the list is empty. */
    static void optionalArray(XContentBuilder b, String name, List<? extends ToXContent> values, ToXContent.Params params)
        throws IOException {
        if (values.isEmpty()) {
            return;
        }
        b.startArray(name);
        for (ToXContent value : values) {
            value.toXContent(b, params);
        }
        b.endArray();
    }
}
