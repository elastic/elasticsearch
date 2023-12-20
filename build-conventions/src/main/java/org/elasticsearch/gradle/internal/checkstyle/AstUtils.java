/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.checkstyle;

import com.puppycrawl.tools.checkstyle.api.DetailAST;

class AstUtils {

    /**
     * Dumps a tree of the provided AST to the console. Numeric types can get checked
     * against {@link com.puppycrawl.tools.checkstyle.grammar.java.JavaLanguageLexer}
     * @param ast the AST to dump
     */
    public static void dumpAst(DetailAST ast) {
        dumpAst(0, ast);
    }

    private static void dumpAst(int depth, DetailAST ast) {
        System.err.println("AST: " + ("  ".repeat(depth)) + ast.getType() + "[" + ast.getText() + "]");
        if (ast.hasChildren()) {
            for (DetailAST child = ast.getFirstChild(); child != null; child = child.getNextSibling()) {
                dumpAst(depth + 1, child);
            }
        }
    }
}
