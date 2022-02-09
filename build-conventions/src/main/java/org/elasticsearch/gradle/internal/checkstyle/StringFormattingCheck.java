/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.checkstyle;

import com.puppycrawl.tools.checkstyle.StatelessCheck;
import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Checks for calls to {@link String#formatted(Object...)} that include format specifiers that
 * are not locale-safe. This method always uses the default {@link Locale}, and so for our
 * purposes it is safer to use {@link String#format(Locale, String, Object...)}.
 * <p>
 * Note that this rule can currently only detect violations when calling <code>formatted()</code>
 * on a string literal or text block. In theory, it could be extended to detect violations in
 * local variables or statics.
 */
@StatelessCheck
public class StringFormattingCheck extends AbstractCheck {

    public static final String FORMATTED_MSG_KEY = "forbidden.formatted";

    @Override
    public int[] getDefaultTokens() {
        return getRequiredTokens();
    }

    @Override
    public int[] getAcceptableTokens() {
        return getRequiredTokens();
    }

    @Override
    public int[] getRequiredTokens() {
        return new int[] { TokenTypes.METHOD_CALL };
    }

    @Override
    public void visitToken(DetailAST ast) {
        checkFormattedMethod(ast);
    }

    // Originally pinched from java/util/Formatter.java but then modified.
    // %[argument_index$][flags][width][.precision][t]conversion
    private static final Pattern formatSpecifier = Pattern.compile("%(?:\\d+\\$)?(?:[-#+ 0,\\(<]*)?(?:\\d+)?(?:\\.\\d+)?([tT]?[a-zA-Z%])");

    private void checkFormattedMethod(DetailAST ast) {
        final DetailAST dotAst = ast.findFirstToken(TokenTypes.DOT);
        if (dotAst == null) {
            return;
        }

        final String methodName = dotAst.findFirstToken(TokenTypes.IDENT).getText();
        if (methodName.equals("formatted") == false) {
            return;
        }

        final DetailAST subjectAst = dotAst.getFirstChild();

        String stringContent = null;
        if (subjectAst.getType() == TokenTypes.TEXT_BLOCK_LITERAL_BEGIN) {
            stringContent = subjectAst.findFirstToken(TokenTypes.TEXT_BLOCK_CONTENT).getText();
        } else if (subjectAst.getType() == TokenTypes.STRING_LITERAL) {
            stringContent = subjectAst.getText();
        }

        if (stringContent != null) {
            final Matcher m = formatSpecifier.matcher(stringContent);
            while (m.find()) {
                char specifier = m.group(1).toLowerCase(Locale.ROOT).charAt(0);

                if (specifier == 'd' || specifier == 'e' || specifier == 'f' || specifier == 'g' || specifier == 't') {
                    log(ast, FORMATTED_MSG_KEY, m.group());
                }
            }
        }
    }
}
