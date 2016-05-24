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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a string constant.
 */
public final class LString extends ALink {

    public LString(int line, int offset, String location, String string) {
        super(line, offset, location, -1);

        this.string = string;
    }

    @Override
    ALink analyze(Variables variables) {
        if (before != null) {
            throw new IllegalArgumentException(error("Illegal String constant [" + string + "]."));
        } else if (store) {
            throw new IllegalArgumentException(error("Cannot write to read-only String constant [" + string + "]."));
        } else if (!load) {
            throw new IllegalArgumentException(error("Must read String constant [" + string + "]."));
        }

        after = Definition.STRING_TYPE;

        return this;
    }

    @Override
    void write(MethodWriter writer) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer) {
        writer.push(string);
    }

    @Override
    void store(MethodWriter writer) {
        throw new IllegalStateException(error("Illegal tree structure."));
    }
}
