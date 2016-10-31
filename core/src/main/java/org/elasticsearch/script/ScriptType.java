/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * ScriptType represents the way a script is stored and retrieved from the {@link ScriptService}.
 * It's also used to by {@link ScriptSettings} and {@link ScriptModes} to determine whether or not
 * a {@link Script} is allowed to be executed based on both default and user-defined settings.
 */
public enum ScriptType implements Writeable {
    /**
     * INLINE scripts are specified in numerous queries and compiled on-the-fly.
     * They will be cached based on the lang and code of the script.
     * They are turned off by default for security purposes.
     */
    INLINE ( 0 , "inline" , new ParseField("inline")       , false ),

    /**
     * STORED scripts are saved as part of the {@link org.elasticsearch.cluster.ClusterState}
     * based on user requests.  They will be cached when they are first used in a query.
     * They are turned off by default for security purposes.
     */
    STORED ( 1 , "stored" , new ParseField("stored", "id") , false ),

    /**
     * FILE scripts are loaded from disk either on start-up or on-the-fly depending on
     * user-defined settings.  They will be compiled and cached as soon as they are loaded
     * from disk.  They are turned on by default as they should always be safe to execute.
     */
    FILE   ( 2 , "file"   , new ParseField("file")         , true  );

    /**
     * Reads an int from the input stream and converts it to a {@link ScriptType}.
     * @return The ScriptType read from the stream. Throws an {@link IllegalStateException} if no ScriptType is found based on the id.
     */
    public static ScriptType readFrom(StreamInput in) throws IOException {
        int id = in.readVInt();

        if (FILE.id == id) {
            return FILE;
        } else if (STORED.id == id) {
            return STORED;
        } else if (INLINE.id == id) {
            return INLINE;
        } else {
            throw new IllegalStateException("Error reading ScriptType id [" + id + "] from stream, expected one of [" +
                FILE.id + " [" + FILE.name + "], " + STORED.id + " [" + STORED.name + "], " + INLINE.id + " [" + INLINE.name + "]]");
        }
    }

    private final int id;
    private final String name;
    private final ParseField parseField;
    private final boolean defaultEnabled;

    /**
     * Standard constructor.
     * @param id A unique identifier for a type that can be read/written to a stream.
     * @param name A unique name for a type.
     * @param parseField Specifies the name used to parse input from queries.
     * @param defaultEnabled Whether or not an {@link ScriptType} can be run by default.
     */
    ScriptType(int id, String name, ParseField parseField, boolean defaultEnabled) {
        this.id = id;
        this.name = name;
        this.parseField = parseField;
        this.defaultEnabled = defaultEnabled;
    }

    /**
     * Writes an int to the output stream based on the id of the {@link ScriptType}.
     */
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
     * @return The unique name for this {@link ScriptType}.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Specifies the name used to parse input from queries.
     */
    public ParseField getParseField() {
        return parseField;
    }

    /**
     * @return Whether or not a {@link ScriptType} can be run by default.  Note
     * this can be potentially overriden by any {@link ScriptEngineService}.
     */
    public boolean isDefaultEnabled() {
        return defaultEnabled;
    }

    /**
     * @return The same as calling {@link #getName()}.
     */
    @Override
    public String toString() {
        return getName();
    }
}
