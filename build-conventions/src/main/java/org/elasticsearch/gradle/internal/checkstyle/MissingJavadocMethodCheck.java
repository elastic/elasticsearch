/*
 * @notice
 * checkstyle: Checks Java source code for adherence to a set of rules.
 * Copyright (C) 2001-2021 the original author or authors.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.elasticsearch.gradle.internal.checkstyle;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.puppycrawl.tools.checkstyle.FileStatefulCheck;
import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.FileContents;
import com.puppycrawl.tools.checkstyle.api.Scope;
import com.puppycrawl.tools.checkstyle.api.TextBlock;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;
import com.puppycrawl.tools.checkstyle.utils.AnnotationUtil;
import com.puppycrawl.tools.checkstyle.utils.CheckUtil;
import com.puppycrawl.tools.checkstyle.utils.CommonUtil;
import com.puppycrawl.tools.checkstyle.utils.ScopeUtil;

import org.gradle.api.logging.LogLevel;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

/**
 * This is a copy of Checkstyle's {@link com.puppycrawl.tools.checkstyle.checks.javadoc.MissingJavadocMethodCheck},
 * modified to accept a regex to exclude classes by name. See the original class for full documentation.
 */
@FileStatefulCheck
public class MissingJavadocMethodCheck extends AbstractCheck {

    /**
     * A key is pointing to the warning message text in "messages.properties"
     * file.
     */
    public static final String MSG_JAVADOC_MISSING = "javadoc.missing";

    /** Default value of minimal amount of lines in method to allow no documentation.*/
    private static final int DEFAULT_MIN_LINE_COUNT = -1;

    /** Specify the visibility scope where Javadoc comments are checked. */
    private Scope scope = Scope.PUBLIC;

    /** Specify the visibility scope where Javadoc comments are not checked. */
    private Scope excludeScope;

    /** Control the minimal amount of lines in method to allow no documentation.*/
    private int minLineCount = DEFAULT_MIN_LINE_COUNT;

    /** Specify pattern for types to ignore. */
    private Pattern ignorePattern = Pattern.compile("^$");

    /** Specify pattern for types to include. */
    private Pattern includePattern = Pattern.compile("^.*$");

    /**
     * Control whether to allow missing Javadoc on accessor methods for
     * properties (setters and getters).
     */
    private boolean allowMissingPropertyJavadoc;

    /** Ignore method whose names are matching specified regex. */
    private Pattern ignoreMethodNamesRegex;

    /** Configure the list of annotations that allow missed documentation. */
    private List<String> allowedAnnotations = Collections.singletonList("Override");

    /**
     * Setter to configure the list of annotations that allow missed documentation.
     *
     * @param userAnnotations user's value.
     */
    public void setAllowedAnnotations(String... userAnnotations) {
        allowedAnnotations = Arrays.asList(userAnnotations);
    }

    /**
     * Setter to ignore method whose names are matching specified regex.
     *
     * @param pattern a pattern.
     */
    public void setIgnoreMethodNamesRegex(Pattern pattern) {
        ignoreMethodNamesRegex = pattern;
    }

    /**
     * Setter to control the minimal amount of lines in method to allow no documentation.
     *
     * @param value user's value.
     */
    public void setMinLineCount(int value) {
        minLineCount = value;
    }

    /**
     * Setter to control whether to allow missing Javadoc on accessor methods for properties
     * (setters and getters).
     *
     * @param flag a {@code Boolean} value
     */
    public void setAllowMissingPropertyJavadoc(final boolean flag) {
        allowMissingPropertyJavadoc = flag;
    }

    /**
     * Setter to specify the visibility scope where Javadoc comments are checked.
     *
     * @param scope a scope.
     */
    public void setScope(Scope scope) {
        this.scope = scope;
    }

    /**
     * Setter to specify the visibility scope where Javadoc comments are not checked.
     *
     * @param excludeScope a scope.
     */
    public void setExcludeScope(Scope excludeScope) {
        this.excludeScope = excludeScope;
    }

    /**
     * Setter to specify pattern for types to ignore.
     *
     * @param pattern a pattern.
     */
    public final void setIgnorePattern(Pattern pattern) {
        ignorePattern = pattern;
    }

    /**
     * Setter to specify pattern for types to include.
     *
     * @param pattern a pattern.
     */
    public final void setIncludePattern(Pattern pattern) {
        includePattern = pattern;
    }

    @Override
    public final int[] getRequiredTokens() {
        return CommonUtil.EMPTY_INT_ARRAY;
    }

    @Override
    public int[] getDefaultTokens() {
        return getAcceptableTokens();
    }

    @Override
    public int[] getAcceptableTokens() {
        return new int[] {
            TokenTypes.METHOD_DEF,
            TokenTypes.CTOR_DEF,
            TokenTypes.ANNOTATION_FIELD_DEF,
            TokenTypes.COMPACT_CTOR_DEF,
        };
    }

    @Override
    public final void visitToken(DetailAST ast) {
        final Scope theScope = ScopeUtil.getScope(ast);
        if (shouldCheck(ast, theScope)) {
            final FileContents contents = getFileContents();
            final TextBlock textBlock = contents.getJavadocBefore(ast.getLineNo());

            if (textBlock == null && isMissingJavadocAllowed(ast) == false) {
                log(ast, MSG_JAVADOC_MISSING);
            }
        }
    }

    /**
     * Some javadoc.
     *
     * @param methodDef Some javadoc.
     * @return Some javadoc.
     */
    private static int getMethodsNumberOfLine(DetailAST methodDef) {
        final int numberOfLines;
        final DetailAST lcurly = methodDef.getLastChild();
        final DetailAST rcurly = lcurly.getLastChild();

        if (lcurly.getFirstChild() == rcurly) {
            numberOfLines = 1;
        }
        else {
            numberOfLines = rcurly.getLineNo() - lcurly.getLineNo() - 1;
        }
        return numberOfLines;
    }

    /**
     * Checks if a missing Javadoc is allowed by the check's configuration.
     *
     * @param ast the tree node for the method or constructor.
     * @return True if this method or constructor doesn't need Javadoc.
     */
    private boolean isMissingJavadocAllowed(final DetailAST ast) {
        return allowMissingPropertyJavadoc
            && (CheckUtil.isSetterMethod(ast) || CheckUtil.isGetterMethod(ast))
            || matchesSkipRegex(ast)
            || isContentsAllowMissingJavadoc(ast);
    }

    /**
     * Checks if the Javadoc can be missing if the method or constructor is
     * below the minimum line count or has a special annotation.
     *
     * @param ast the tree node for the method or constructor.
     * @return True if this method or constructor doesn't need Javadoc.
     */
    private boolean isContentsAllowMissingJavadoc(DetailAST ast) {
        return (ast.getType() == TokenTypes.METHOD_DEF
            || ast.getType() == TokenTypes.CTOR_DEF
            || ast.getType() == TokenTypes.COMPACT_CTOR_DEF)
            && (getMethodsNumberOfLine(ast) <= minLineCount
            || AnnotationUtil.containsAnnotation(ast, allowedAnnotations));
    }

    /**
     * Checks if the given method name matches the regex. In that case
     * we skip enforcement of javadoc for this method
     *
     * @param methodDef {@link TokenTypes#METHOD_DEF METHOD_DEF}
     * @return true if given method name matches the regex.
     */
    private boolean matchesSkipRegex(DetailAST methodDef) {
        boolean result = false;
        if (ignoreMethodNamesRegex != null) {
            final DetailAST ident = methodDef.findFirstToken(TokenTypes.IDENT);
            final String methodName = ident.getText();

            final Matcher matcher = ignoreMethodNamesRegex.matcher(methodName);
            if (matcher.matches()) {
                result = true;
            }
        }
        return result;
    }

    /**
     * Whether we should check this node.
     *
     * @param ast a given node.
     * @param nodeScope the scope of the node.
     * @return whether we should check a given node.
     */
    private boolean shouldCheck(final DetailAST ast, final Scope nodeScope) {
        final Scope surroundingScope = ScopeUtil.getSurroundingScope(ast);

        DetailAST parent = ast;
        while (parent != null && parent.getType() != TokenTypes.CLASS_DEF) {
            parent = parent.getParent();
        }
        final String outerTypeName = parent == null ? "<unknown>" : parent.findFirstToken(TokenTypes.IDENT).getText();

        return (excludeScope == null
            || nodeScope != excludeScope
            && surroundingScope != excludeScope)
            && nodeScope.isIn(scope)
            && surroundingScope.isIn(scope)
            && includePattern.matcher(outerTypeName).find()
            && ignorePattern.matcher(outerTypeName).find() == false;
    }

}
