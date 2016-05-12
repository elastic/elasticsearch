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

package org.elasticsearch.painless;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.util.TraceClassVisitor;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/** quick and dirty tools for debugging */
final class Debugger {

    /** compiles source to bytecode, and returns debugging output */
    static String toString(String source) {
        return toString(source, new CompilerSettings());
    }

    /** compiles to bytecode, and returns debugging output */
    static String toString(String source, CompilerSettings settings) {
        byte[] bytes = Compiler.compile("debugger", source, Definition.INSTANCE, settings);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintWriter outputWriter = new PrintWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));
        ClassReader reader = new ClassReader(bytes);
        reader.accept(new TraceClassVisitor(outputWriter), 0);
        outputWriter.flush();
        try {
            return output.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
