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

/**
 * This rule checks for switch statements that use a {@code between(x, y)} to generate some kind
 * of random behaviour in tests and which have a case that falls outside the random range.
 * <p>
 * Consider this example:
 * <pre>{@code
 * switch (between(0, 3)) {
 *   case 0 -> name += randomAlphaOfLength(5);
 *   case 1 -> requiredSize += between(1, 100);
 *   case 2 -> minDocCount += between(1, 100);
 *   case 3 -> subsetSize += between(1, 100);
 *   case 4 -> supersetSize += between(1, 100);
 * }
 * }</pre>
 * <p>The "4" case is an error, because it can never execute.
 */
@StatelessCheck
public class SwitchBetweenCheck extends AbstractCheck {

    public static final String SWITCH_RANDOM_INT_MSG_KEY = "forbidden.switch.randomInt";
    public static final String SWITCH_BETWEEN_MSG_KEY = "forbidden.switch.between";

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
        return new int[] { TokenTypes.LITERAL_SWITCH };
    }

    @Override
    public void visitToken(DetailAST ast) {
        checkSwitchBetween(ast);
    }

    private void checkSwitchBetween(DetailAST ast) {
        // First dig out the switch expression
        final DetailAST switchExprAst = ast.findFirstToken(TokenTypes.EXPR);
        if (switchExprAst == null) {
            return;
        }

        // Check if it's a method call
        final DetailAST methodCallAst = switchExprAst.getFirstChild();
        if (methodCallAst.getType() != TokenTypes.METHOD_CALL) {
            return;
        }

        // And check if the method call is a `between` or `randomIntBetween` call
        final DetailAST methodIdentAst = methodCallAst.findFirstToken(TokenTypes.IDENT);
        if (methodIdentAst == null) {
            return;
        }
        final String switchMethodName = methodIdentAst.getText();
        switch (switchMethodName) {
            case "between":
            case "randomIntBetween":
            case "randomInt":
                // these are ok
                break;
            default:
                return;
        }

        // The method name is good, so dig out the arguments to the method. We only handle simple,
        // integer literal arguments
        final DetailAST argListAst = methodCallAst.findFirstToken(TokenTypes.ELIST);
        int min;
        int max;
        if (switchMethodName.equals("randomInt")) {
            if (argListAst.getChildCount() != 1) { // 1 arg
                return;
            }

            try {
                // Get first or last child, which is an EXPR, then get the argument itself
                final String maxStr = argListAst.getLastChild().getFirstChild().getText();
                min = 0;
                max = Integer.parseInt(maxStr);
            } catch (NumberFormatException e) {
                return;
            }
        } else {
            if (argListAst.getChildCount() != 3) { // 2 args + COMMA
                return;
            }

            try {
                // Get first or last child, which is an EXPR, then get the argument itself
                final String minStr = argListAst.getFirstChild().getFirstChild().getText();
                final String maxStr = argListAst.getLastChild().getFirstChild().getText();
                min = Integer.parseInt(minStr);
                max = Integer.parseInt(maxStr);
            } catch (NumberFormatException e) {
                return;
            }
        }

        // Now check all the cases of the switch and look for values outside the possible range.
        // We ignore anything that doesn't parse as an integer, so it's possible we could miss
        // some cases.
        for (DetailAST caseAst = ast.getFirstChild(); caseAst != null; caseAst = caseAst.getNextSibling()) {
            if (caseAst.getType() != TokenTypes.CASE_GROUP && caseAst.getType() != TokenTypes.SWITCH_RULE) {
                continue;
            }

            final DetailAST literalCaseAst = caseAst.getFirstChild();
            if (literalCaseAst.getType() == TokenTypes.LITERAL_DEFAULT) {
                continue;
            }

            final DetailAST exprAst = literalCaseAst.getFirstChild();
            if (exprAst.getType() != TokenTypes.EXPR) {
                continue;
            }

            try {
                int value = Integer.parseInt(exprAst.getFirstChild().getText());
                if (value < min || value > max) {
                    if (switchMethodName.equals("randomInt")) {
                        log(caseAst, SWITCH_RANDOM_INT_MSG_KEY, value, switchMethodName, max);
                    } else {
                        log(caseAst, SWITCH_BETWEEN_MSG_KEY, value, switchMethodName, min, max);
                    }
                }
            } catch (NumberFormatException e) {
                // Ignore
            }
        }
    }
}
