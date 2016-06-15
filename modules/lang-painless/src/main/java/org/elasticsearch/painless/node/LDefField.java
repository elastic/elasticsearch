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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Locals;
import org.objectweb.asm.Type;
import org.elasticsearch.painless.MethodWriter;

import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_HANDLE;

/**
 * Represents a field load/store or shortcut on a def type.  (Internal only.)
 */
final class LDefField extends ALink implements IDefLink {

    final String value;

    LDefField(Location location, String value) {
        super(location, 1);

        this.value = value;
    }


    @Override
    ALink analyze(Locals locals) {
        after = Definition.DEF_TYPE;

        return this;
    }

    @Override
    void write(MethodWriter writer) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer) {
        writer.writeDebugInfo(location);

        String desc = Type.getMethodDescriptor(after.type, Definition.DEF_TYPE.type);
        writer.invokeDynamic(value, desc, DEF_BOOTSTRAP_HANDLE, DefBootstrap.LOAD);
    }

    @Override
    void store(MethodWriter writer) {
        writer.writeDebugInfo(location);

        String desc = Type.getMethodDescriptor(Definition.VOID_TYPE.type, Definition.DEF_TYPE.type, after.type);
        writer.invokeDynamic(value, desc, DEF_BOOTSTRAP_HANDLE, DefBootstrap.STORE);
    }
}
