/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.StringUtils;

public class LiteralBuilder extends IdentifierBuilder {

    static String unquoteString(String text) {
        // remove leading and trailing ' for strings and also eliminate escaped single quotes
        if (text == null) {
            return null;
        }

        // unescaped strings can be interpreted directly
        if (text.startsWith("?")) {
            return text.substring(2, text.length() - 1);
        }

        text = text.substring(1, text.length() - 1);
        Pattern regex = Pattern.compile("\\\\.");
        StringBuffer resultString = new StringBuffer();
        Matcher regexMatcher = regex.matcher(text);

        while (regexMatcher.find()) {
            String source = regexMatcher.group();
            String replacement;

            switch (source) {
                case "\\t":
                    replacement = "\t";
                    break;
                case "\\b":
                    replacement = "\b";
                    break;
                case "\\f":
                    replacement = "\f";
                    break;
                case "\\n":
                    replacement = "\n";
                    break;
                case "\\r":
                    replacement = "\r";
                    break;
                case "\\\"":
                    replacement = "\"";
                    break;
                case "\\'":
                    replacement = "'";
                    break;
                case "\\\\":
                    // will be interpreted as regex, so we have to escape it
                    replacement = "\\\\";
                    break;
                default:
                    // unknown escape sequence, pass through as-is
                    replacement = source;
            }

            regexMatcher.appendReplacement(resultString, replacement);

        }
        regexMatcher.appendTail(resultString);

        return resultString.toString();
    }

    @Override
    public Literal visitNullLiteral(EqlBaseParser.NullLiteralContext ctx) {
        Source source = source(ctx);
        return new Literal(source, null, DataTypes.NULL);
    }

    @Override
    public Literal visitBooleanValue(EqlBaseParser.BooleanValueContext ctx) {
        Source source = source(ctx);
        return new Literal(source, ctx.TRUE() != null, DataTypes.BOOLEAN);
    }

    @Override
    public Literal visitDecimalLiteral(EqlBaseParser.DecimalLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        try {
            return new Literal(source, Double.valueOf(StringUtils.parseDouble(text)), DataTypes.DOUBLE);
        } catch (QlIllegalArgumentException siae) {
            throw new ParsingException(source, siae.getMessage());
        }
    }

    @Override
    public Literal visitIntegerLiteral(EqlBaseParser.IntegerLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        long value;

        try {
            value = Long.valueOf(StringUtils.parseLong(text));
        } catch (QlIllegalArgumentException siae) {
            // if it's too large, then quietly try to parse as a float instead
            try {
                return new Literal(source, Double.valueOf(StringUtils.parseDouble(text)), DataTypes.DOUBLE);
            } catch (QlIllegalArgumentException ignored) {}

            throw new ParsingException(source, siae.getMessage());
        }

        Object val = Long.valueOf(value);
        DataType type = DataTypes.LONG;

        // try to downsize to int if possible (since that's the most common type)
        if ((int) value == value) {
            type = DataTypes.INTEGER;
            val = Integer.valueOf((int) value);
        }
        return new Literal(source, val, type);
    }

    @Override
    public Literal visitString(EqlBaseParser.StringContext ctx) {
        return new Literal(source(ctx), unquoteString(ctx.getText()), DataTypes.KEYWORD);
    }
}
