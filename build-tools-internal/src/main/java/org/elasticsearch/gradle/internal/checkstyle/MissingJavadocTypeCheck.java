/*
 * @notice
 * checkstyle: Checks Java source code for adherence to a set of rules.
 * Copyright (C) 2001-2020 the original author or authors.
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

import com.puppycrawl.tools.checkstyle.StatelessCheck;
import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.FileContents;
import com.puppycrawl.tools.checkstyle.api.Scope;
import com.puppycrawl.tools.checkstyle.api.TextBlock;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;
import com.puppycrawl.tools.checkstyle.utils.AnnotationUtil;
import com.puppycrawl.tools.checkstyle.utils.CommonUtil;
import com.puppycrawl.tools.checkstyle.utils.ScopeUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This is a copy of Checkstyle's {@link com.puppycrawl.tools.checkstyle.checks.javadoc.MissingJavadocTypeCheck},
 * modified to accept a regex to exclude classes by name. See the original class for full documentation.
 */
@StatelessCheck
public class MissingJavadocTypeCheck extends AbstractCheck {

    /**
     * A key is pointing to the warning message text in "messages.properties"
     * file.
     */
    public static final String MSG_JAVADOC_MISSING = "javadoc.missing";

    /** Specify the visibility scope where Javadoc comments are checked. */
    private Scope scope = Scope.PUBLIC;

    /** Specify the visibility scope where Javadoc comments are not checked. */
    private Scope excludeScope;

    /** Specify pattern for types to ignore. */
    private Pattern ignorePattern = Pattern.compile("^$");

    /**
     * Specify the list of annotations that allow missed documentation.
     * Only short names are allowed, e.g. {@code Generated}.
     */
    private List<String> skipAnnotations = Collections.singletonList("Generated");

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
     * Setter to specify the list of annotations that allow missed documentation.
     * Only short names are allowed, e.g. {@code Generated}.
     *
     * @param userAnnotations user's value.
     */
    public void setSkipAnnotations(String... userAnnotations) {
        skipAnnotations = Arrays.asList(userAnnotations);
    }

    /**
     * Setter to specify pattern for types to ignore.
     *
     * @param pattern a pattern.
     */
    public final void setIgnorePattern(Pattern pattern) {
        ignorePattern = pattern;
    }

    @Override
    public int[] getDefaultTokens() {
        return getAcceptableTokens();
    }

    @Override
    public int[] getAcceptableTokens() {
        return new int[] {
            TokenTypes.INTERFACE_DEF,
            TokenTypes.CLASS_DEF,
            TokenTypes.ENUM_DEF,
            TokenTypes.ANNOTATION_DEF,
            TokenTypes.RECORD_DEF };
    }

    @Override
    public int[] getRequiredTokens() {
        return CommonUtil.EMPTY_INT_ARRAY;
    }

    @Override
    public void visitToken(DetailAST ast) {
        if (shouldCheck(ast)) {
            final FileContents contents = getFileContents();
            final int lineNo = ast.getLineNo();
            final TextBlock textBlock = contents.getJavadocBefore(lineNo);
            if (textBlock == null) {
                log(ast, MSG_JAVADOC_MISSING);
            }
        }
    }

    /**
     * Whether we should check this node.
     *
     * @param ast a given node.
     * @return whether we should check a given node.
     */
    private boolean shouldCheck(final DetailAST ast) {
        final Scope customScope;

        if (ScopeUtil.isInInterfaceOrAnnotationBlock(ast)) {
            customScope = Scope.PUBLIC;
        } else {
            final DetailAST mods = ast.findFirstToken(TokenTypes.MODIFIERS);
            customScope = ScopeUtil.getScopeFromMods(mods);
        }
        final Scope surroundingScope = ScopeUtil.getSurroundingScope(ast);

        final String outerTypeName = ast.findFirstToken(TokenTypes.IDENT).getText();

        return customScope.isIn(scope)
            && (surroundingScope == null || surroundingScope.isIn(scope))
            && (excludeScope == null
                || customScope.isIn(excludeScope) == false
                || surroundingScope != null && surroundingScope.isIn(excludeScope) == false)
            && AnnotationUtil.containsAnnotation(ast, skipAnnotations) == false
            && ignorePattern.matcher(outerTypeName).find() == false;
    }

}
