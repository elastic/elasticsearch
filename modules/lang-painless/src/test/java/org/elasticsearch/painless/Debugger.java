/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.WriteScope;
import org.objectweb.asm.util.Textifier;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

/** quick and dirty tools for debugging */
final class Debugger {

    /** compiles source to bytecode, and returns debugging output */
    static String toString(final String source) {
        return toString(PainlessTestScript.class, source, new CompilerSettings(), PainlessPlugin.BASE_WHITELISTS);
    }

    /** compiles to bytecode, and returns debugging output */
    static String toString(Class<?> iface, String source, CompilerSettings settings, List<Whitelist> whitelists) {
        StringWriter output = new StringWriter();
        PrintWriter outputWriter = new PrintWriter(output);
        Textifier textifier = new Textifier();
        try {
            new Compiler(iface, null, null, PainlessLookupBuilder.buildFromWhitelists(whitelists)).compile(
                "<debugging>",
                source,
                settings,
                textifier
            );
        } catch (RuntimeException e) {
            textifier.print(outputWriter);
            e.addSuppressed(new Exception("current bytecode: \n" + output));
            throw e;
        }

        textifier.print(outputWriter);
        return output.toString();
    }

    /** compiles to bytecode, and returns debugging output */
    private static String tree(
        Class<?> iface,
        String source,
        CompilerSettings settings,
        List<Whitelist> whitelists,
        UserTreeVisitor<ScriptScope> semanticPhaseVisitor,
        UserTreeVisitor<ScriptScope> irPhaseVisitor,
        IRTreeVisitor<WriteScope> asmPhaseVisitor
    ) {
        StringWriter output = new StringWriter();
        PrintWriter outputWriter = new PrintWriter(output);
        Textifier textifier = new Textifier();
        try {
            new Compiler(iface, null, null, PainlessLookupBuilder.buildFromWhitelists(whitelists)).compile(
                "<debugging>",
                source,
                settings,
                textifier,
                semanticPhaseVisitor,
                irPhaseVisitor,
                asmPhaseVisitor
            );
        } catch (RuntimeException e) {
            textifier.print(outputWriter);
            e.addSuppressed(new Exception("current bytecode: \n" + output));
            throw e;
        }

        textifier.print(outputWriter);
        return output.toString();
    }

    static void phases(
        final String source,
        UserTreeVisitor<ScriptScope> semanticPhaseVisitor,
        UserTreeVisitor<ScriptScope> irPhaseVisitor,
        IRTreeVisitor<WriteScope> asmPhaseVisitor
    ) {
        tree(
            PainlessTestScript.class,
            source,
            new CompilerSettings(),
            PainlessPlugin.BASE_WHITELISTS,
            semanticPhaseVisitor,
            irPhaseVisitor,
            asmPhaseVisitor
        );
    }
}
