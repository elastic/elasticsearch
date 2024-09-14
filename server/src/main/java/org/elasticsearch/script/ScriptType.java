/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.Locale;

/**
 * ScriptType represents the way a script is stored and retrieved from the {@link ScriptService}.
 * It's also used to by {@link ScriptService} to determine whether or not a {@link Script} is
 * allowed to be executed based on both default and user-defined settings.
 */
public enum ScriptType implements Writeable {

    /**
     * INLINE scripts are specified in numerous queries and compiled on-the-fly.
     * They will be cached based on the lang and code of the script.
     * They are turned off by default because most languages are insecure
     * (Groovy and others), but can be overridden by the specific {@link ScriptEngine}
     * if the language is naturally secure (Painless, Mustache, and Expressions).
     */
    INLINE(0, new ParseField("source", "inline")),

    /**
     * STORED scripts are saved as part of the {@link org.elasticsearch.cluster.ClusterState}
     * based on user requests.  They will be cached when they are first used in a query.
     * They are turned off by default because most languages are insecure
     * (Groovy and others), but can be overridden by the specific {@link ScriptEngine}
     * if the language is naturally secure (Painless, Mustache, and Expressions).
     */
    STORED(1, new ParseField("id", "stored"));

    /**
     * Reads an int from the input stream and converts it to a {@link ScriptType}.
     * @return The ScriptType read from the stream. Throws an {@link IllegalStateException}
     * if no ScriptType is found based on the id.
     */
    public static ScriptType readFrom(StreamInput in) throws IOException {
        int id = in.readVInt();

        if (STORED.id == id) {
            return STORED;
        } else if (INLINE.id == id) {
            return INLINE;
        } else {
            throw new IllegalStateException(
                "Error reading ScriptType id ["
                    + id
                    + "] from stream, expected one of ["
                    + STORED.id
                    + " ["
                    + STORED.parseField.getPreferredName()
                    + "], "
                    + INLINE.id
                    + " ["
                    + INLINE.parseField.getPreferredName()
                    + "]]"
            );
        }
    }

    private final int id;
    private final ParseField parseField;

    /**
     * Standard constructor.
     * @param id A unique identifier for a type that can be read/written to a stream.
     * @param parseField Specifies the name used to parse input from queries.
     */
    ScriptType(int id, ParseField parseField) {
        this.id = id;
        this.parseField = parseField;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
    }

    /**
     * @return The unique id for this {@link ScriptType}.
     */
    public int getId() {
        return id;
    }

    /**
     * @return The unique name for this {@link ScriptType} based on the {@link ParseField}.
     */
    public String getName() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * @return Specifies the name used to parse input from queries.
     */
    public ParseField getParseField() {
        return parseField;
    }

    /**
     * @return The same as calling {@link #getName()}.
     */
    @Override
    public String toString() {
        return getName();
    }
}
