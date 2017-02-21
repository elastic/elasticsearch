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

import org.elasticsearch.script.ScriptException;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Abstract superclass on top of which all Painless scripts are built.
 */
public abstract class PainlessScript {
    /**
     * Name of the script set at compile time.
     */
    private final String name;
    /**
     * Source of the script.
     */
    private final String source;
    /**
     * Character number of the start of each statement.
     */
    private final BitSet statements;

    protected PainlessScript(String name, String source, BitSet statements) {
        this.name = name;
        this.source = source;
        this.statements = statements;
    }

    /**
     * Adds stack trace and other useful information to exceptions thrown
     * from a Painless script.
     * @param t The throwable to build an exception around.
     * @return The generated ScriptException.
     */
    protected final ScriptException convertToScriptException(Throwable t, Map<String, List<String>> extraMetadata) {
        // create a script stack: this is just the script portion
        List<String> scriptStack = new ArrayList<>();
        for (StackTraceElement element : t.getStackTrace()) {
            if (WriterConstants.CLASS_NAME.equals(element.getClassName())) {
                // found the script portion
                int offset = element.getLineNumber();
                if (offset == -1) {
                    scriptStack.add("<<< unknown portion of script >>>");
                } else {
                    offset--; // offset is 1 based, line numbers must be!
                    int startOffset = getPreviousStatement(offset);
                    if (startOffset == -1) {
                        assert false; // should never happen unless we hit exc in ctor prologue...
                        startOffset = 0;
                    }
                    int endOffset = getNextStatement(startOffset);
                    if (endOffset == -1) {
                        endOffset = source.length();
                    }
                    // TODO: if this is still too long, truncate and use ellipses
                    String snippet = source.substring(startOffset, endOffset);
                    scriptStack.add(snippet);
                    StringBuilder pointer = new StringBuilder();
                    for (int i = startOffset; i < offset; i++) {
                        pointer.append(' ');
                    }
                    pointer.append("^---- HERE");
                    scriptStack.add(pointer.toString());
                }
                break;
            // but filter our own internal stacks (e.g. indy bootstrap)
            } else if (!shouldFilter(element)) {
                scriptStack.add(element.toString());
            }
        }
        // build a name for the script:
        final String name;
        if (PainlessScriptEngineService.INLINE_NAME.equals(this.name)) {
            name = source;
        } else {
            name = this.name;
        }
        ScriptException scriptException = new ScriptException("runtime error", t, scriptStack, name, PainlessScriptEngineService.NAME);
        for (Map.Entry<String, List<String>> entry : extraMetadata.entrySet()) {
            scriptException.addMetadata(entry.getKey(), entry.getValue());
        }
        return scriptException;
    }

    /** returns true for methods that are part of the runtime */
    private static boolean shouldFilter(StackTraceElement element) {
        return element.getClassName().startsWith("org.elasticsearch.painless.") ||
               element.getClassName().startsWith("java.lang.invoke.") ||
               element.getClassName().startsWith("sun.invoke.");
    }

    /**
     * Finds the start of the first statement boundary that is on or before {@code offset}. If one is not found, {@code -1} is returned.
     */
    private int getPreviousStatement(int offset) {
        return statements.previousSetBit(offset);
    }

    /**
     * Finds the start of the first statement boundary that is after {@code offset}. If one is not found, {@code -1} is returned.
     */
    private int getNextStatement(int offset) {
        return statements.nextSetBit(offset + 1);
    }
}
