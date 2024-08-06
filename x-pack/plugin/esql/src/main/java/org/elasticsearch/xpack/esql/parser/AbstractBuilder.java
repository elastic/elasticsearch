/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.xpack.esql.core.tree.Source;

abstract class AbstractBuilder extends EsqlBaseParserBaseVisitor<Object> {

    @Override
    public Object visit(ParseTree tree) {
        return ParserUtils.visit(super::visit, tree);
    }

    @Override
    public Source visitTerminal(TerminalNode node) {
        return ParserUtils.source(node);
    }

    static String unquote(Source source) {
        return unquote(source.text());
    }

    static String unquote(String string) {
        if (string == null) {
            return null;
        }

        // unescaped strings can be interpreted directly
        if (string.startsWith("\"\"\"")) {
            return string.substring(3, string.length() - 3);
        }

        string = string.substring(1, string.length() - 1);
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < string.length();) {
            if (string.charAt(i) == '\\') {
                // ANTLR4 Grammar guarantees there is always a character after the `\`
                switch (string.charAt(++i)) {
                    case 't' -> sb.append('\t');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case '"' -> sb.append('\"');
                    case '\\' -> sb.append('\\');

                    // will be interpreted as regex, so we have to escape it
                    default ->
                        // unknown escape sequence, pass through as-is, e.g: `...\w...`
                        sb.append('\\').append(string.charAt(i));
                }
                i++;
            } else {
                sb.append(string.charAt(i++));
            }
        }
        return sb.toString();
    }
}
