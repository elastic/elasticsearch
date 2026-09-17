/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.checkstyle;

import com.puppycrawl.tools.checkstyle.FileStatefulCheck;
import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.FullIdent;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;

import java.util.HashSet;
import java.util.Set;

/**
 * Checks for fully qualified class literals whose type is already imported.
 *
 * Checkstyle's {@code UnnecessaryFullyQualifiedType} does not inspect class literals,
 * so this check covers that type-reference context.
 */
@FileStatefulCheck
public class UnnecessaryFullyQualifiedClassLiteralCheck extends AbstractCheck {

    public static final String MSG_KEY = "unnecessary.fully.qualified.class.literal";

    private final Set<String> importedTypes = new HashSet<>();

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
        return new int[] { TokenTypes.IMPORT, TokenTypes.DOT };
    }

    @Override
    public void beginTree(DetailAST rootAST) {
        importedTypes.clear();
    }

    @Override
    public void visitToken(DetailAST ast) {
        if (ast.getType() == TokenTypes.IMPORT) {
            importedTypes.add(FullIdent.createFullIdentBelow(ast).getText());
        } else if (ast.getLastChild().getType() == TokenTypes.LITERAL_CLASS && ast.getFirstChild().getType() == TokenTypes.DOT) {
            DetailAST type = ast.getFirstChild();
            String qualifiedType = FullIdent.createFullIdent(type).getText();
            if (importedTypes.contains(qualifiedType)) {
                log(type, MSG_KEY, qualifiedType, type.getLastChild().getText());
            }
        }
    }
}
