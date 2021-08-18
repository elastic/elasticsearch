/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rewrite.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Option;
import org.openrewrite.Recipe;
import org.openrewrite.internal.lang.NonNull;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.gradle.internal.rewrite.rules.FullQualifiedChangeMethodOwnerRecipe.METHOD_CHANGE_PREFIX;

public class FixFullQualifiedReferenceRecipe extends Recipe {

    @Option(
            displayName = "Fully-qualified target type name",
            description = "A fully-qualified class name we want to fix.",
            example = "org.elasticsearch.core.List"
    )
    private String fullQualifiedClassname;

    @JsonCreator
    public FixFullQualifiedReferenceRecipe(
            @NonNull @JsonProperty("fullQualifiedClassname") String fullQualifiedClassname
    ) {
        this.fullQualifiedClassname = fullQualifiedClassname;
    }

    @Override
    public String getDisplayName() {
        return "FixFullQualifiedReferenceRecipe";
    }

    @Override
    public String getDescription() {
        return "Converts full qualified method calls to simple calls and an import if no clashing imports are found.";
    }

    @Override
    protected JavaVisitor<ExecutionContext> getVisitor() {
        String unqualifiedIdentifier = fullQualifiedClassname.substring(fullQualifiedClassname.lastIndexOf('.') + 1);
        return new Visitor(fullQualifiedClassname, unqualifiedIdentifier);
    }

    public static class Visitor extends JavaIsoVisitor<ExecutionContext> {

        private String fullQualifiedClassname;
        private String unqualifiedIdentifier;
        private boolean hasOriginImport;

        public Visitor(String fullQualifiedClassname, String unqualifiedIdentifier) {
            this.fullQualifiedClassname = fullQualifiedClassname;
            this.unqualifiedIdentifier = unqualifiedIdentifier;
        }

        @Override
        public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext executionContext) {
            Set<Object> processed = executionContext.getMessage(METHOD_CHANGE_PREFIX + fullQualifiedClassname, Collections.emptySet());
            boolean changedMadeBefore = processed.contains(method.getId());
            J.MethodInvocation m = super.visitMethodInvocation(method, executionContext);
            if (changedMadeBefore
                && hasOriginImport == false
                && m.getSelect() instanceof J.FieldAccess
                && ((J.FieldAccess) m.getSelect()).isFullyQualifiedClassReference(fullQualifiedClassname)) {
                Expression select = m.getSelect();
                JavaType javaType = JavaType.buildType(fullQualifiedClassname);
                J.Identifier list = J.Identifier.build(
                    select.getId(),
                    select.getPrefix(),
                    select.getMarkers(),
                    unqualifiedIdentifier,
                    javaType
                );
                m = m.withSelect(list);
                maybeAddImport(fullQualifiedClassname);
            }
            return m;
        }

        @Override
        public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext executionContext) {
            hasOriginImport = cu.getImports().stream().anyMatch(i -> i.getClassName().equals(unqualifiedIdentifier));
            return super.visitCompilationUnit(cu, executionContext);
        }
    }

}
