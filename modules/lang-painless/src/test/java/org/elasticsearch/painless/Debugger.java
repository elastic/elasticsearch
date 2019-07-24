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

import org.elasticsearch.painless.action.PainlessExecuteAction.PainlessTestScript;
import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.spi.Whitelist;
import org.objectweb.asm.util.Textifier;

import java.io.PrintWriter;
import java.io.StringWriter;

/** quick and dirty tools for debugging */
final class Debugger {

    /** compiles source to bytecode, and returns debugging output */
    static String toString(final String source) {
        return toString(PainlessTestScript.class, source, new CompilerSettings());
    }

    /** compiles to bytecode, and returns debugging output */
    static String toString(Class<?> iface, String source, CompilerSettings settings) {
        StringWriter output = new StringWriter();
        PrintWriter outputWriter = new PrintWriter(output);
        Textifier textifier = new Textifier();
        try {
            new Compiler(iface, null, null, PainlessLookupBuilder.buildFromWhitelists(Whitelist.BASE_WHITELISTS))
                    .compile("<debugging>", source, settings, textifier);
        } catch (RuntimeException e) {
            textifier.print(outputWriter);
            e.addSuppressed(new Exception("current bytecode: \n" + output));
            throw e;
        }

        textifier.print(outputWriter);
        return output.toString();
    }
}
