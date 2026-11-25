/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * One field mapped in both indices of a duel, with the checks that compare it. Several fields can share one index
 * pair: every document writes the same values to each of them, so one corpus covers several mappings.
 *
 * @param name    the field name, which must be unique within the duel
 * @param type    the mapped field type
 * @param indexed whether the field keeps its default {@code index} setting; {@code false} maps {@code index: false}
 * @param checks  the checks that compare this field between the baseline and the contender
 */
public record DuelField(String name, String type, boolean indexed, List<BehaviorCheck> checks) {

    public static DuelField keyword(final String name, final List<BehaviorCheck> checks) {
        return new DuelField(name, "keyword", true, checks);
    }

    public static DuelField text(final String name, boolean indexed, final List<BehaviorCheck> checks) {
        return new DuelField(name, "text", indexed, checks);
    }

    public static DuelField matchOnlyText(final String name, boolean indexed, final List<BehaviorCheck> checks) {
        return new DuelField(name, "match_only_text", indexed, checks);
    }

    /**
     * Writes this field's mapping.
     *
     * @param builder    the builder positioned inside the {@code properties} object
     * @param multiValue whether the scenario allows several values a document
     */
    void mapping(final XContentBuilder builder, boolean multiValue) throws IOException {
        builder.startObject(name).field("type", type);
        if (indexed == false) {
            builder.field("index", false);
        }
        if (multiValue == false) {
            builder.field("multi_value", false);
        }
        builder.endObject();
    }
}
