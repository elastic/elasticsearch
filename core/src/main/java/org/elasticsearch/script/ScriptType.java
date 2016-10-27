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

import java.io.IOException;
import java.util.Locale;

/**
 * The type of a script, more specifically where it gets loaded from:
 * - provided dynamically at request time
 * - loaded from an index
 * - loaded from file
 */
public enum ScriptType {

    INLINE(0, "inline", "inline", false),
    STORED(1, "id", "stored", false),
    FILE(2, "file", "file", true);

    private final int val;
    private final ParseField parseField;
    private final String scriptType;
    private final boolean defaultScriptEnabled;

    public static ScriptType readFrom(StreamInput in) throws IOException {
        int scriptTypeVal = in.readVInt();
        for (ScriptType type : values()) {
            if (type.val == scriptTypeVal) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unexpected value read for ScriptType got [" + scriptTypeVal + "] expected one of ["
                + INLINE.val + "," + FILE.val + "," + STORED.val + "]");
    }

    public static void writeTo(ScriptType scriptType, StreamOutput out) throws IOException{
        if (scriptType != null) {
            out.writeVInt(scriptType.val);
        } else {
            out.writeVInt(INLINE.val); //Default to inline
        }
    }

    ScriptType(int val, String name, String scriptType, boolean defaultScriptEnabled) {
        this.val = val;
        this.parseField = new ParseField(name);
        this.scriptType = scriptType;
        this.defaultScriptEnabled = defaultScriptEnabled;
    }

    public ParseField getParseField() {
        return parseField;
    }

    public boolean getDefaultScriptEnabled() {
        return defaultScriptEnabled;
    }

    public String getScriptType() {
        return scriptType;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

}
