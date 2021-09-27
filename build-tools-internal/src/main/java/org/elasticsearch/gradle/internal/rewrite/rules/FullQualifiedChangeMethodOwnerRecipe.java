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
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;

import java.util.Objects;
import java.util.UUID;

import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.rangeClosed;

public class FullQualifiedChangeMethodOwnerRecipe extends Recipe {

    public static final String METHOD_CHANGE_PREFIX = FullQualifiedChangeMethodOwnerRecipe.class.getSimpleName() + "-CHANGED";
    @Option(
        displayName = "Fully-qualified origin class name",
        description = "A fully-qualified class name of the type upon which the method is defined.",
        example = "java.util.List"
    )
    private String originFullQualifiedClassname;

    @Option(
        displayName = "method name",
        description = "The name of the method we want to change the origin.",
        example = "of"
    )
    private String originMethod;

    @Option(
        displayName = "Fully-qualified target type name",
        description = "A fully-qualified class name of the type we want to change the method call to.",
        example = "org.elasticsearch.core.List"
    )
    private String targetFullQualifiedClassname;

    @JsonCreator
    public FullQualifiedChangeMethodOwnerRecipe(
        @NonNull @JsonProperty("originFullQualifiedClassname") String originFullQualifiedClassname,
        @NonNull @JsonProperty("originMethod") String originMethod,
        @NonNull @JsonProperty("targetFullQualifiedClassname") String targetFullQualifiedClassname
    ) {
        this.originFullQualifiedClassname = originFullQualifiedClassname;
        this.originMethod = originMethod;
        this.targetFullQualifiedClassname = targetFullQualifiedClassname;
    }

    @Override
    public String getDisplayName() {
        return "FullQualifiedListOfBackportRecipe";
    }

    @Override
    public String getDescription() {
        return "Converts owner of a method call and uses full qualified type to avoid import conflicts.";
    }

    @Override
    protected JavaVisitor<ExecutionContext> getVisitor() {
        return new Visitor(originFullQualifiedClassname, originMethod, targetFullQualifiedClassname);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        FullQualifiedChangeMethodOwnerRecipe that = (FullQualifiedChangeMethodOwnerRecipe) o;
        return Objects.equals(originFullQualifiedClassname, that.originFullQualifiedClassname)
            && Objects.equals(originMethod, that.originMethod)
            && Objects.equals(targetFullQualifiedClassname, that.targetFullQualifiedClassname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), originFullQualifiedClassname, originMethod, targetFullQualifiedClassname);
    }

    public static class Visitor extends JavaIsoVisitor<ExecutionContext> {
        private final MethodMatcher methodMatcher;
        private String originFullQualifiedClassname;
        private String originMethod;
        private String targetFullQualifiedClassname;

        public Visitor(MethodMatcher methodMatcher) {
            this.methodMatcher = methodMatcher;
        }

        public Visitor(String originFullQualifiedClassname,
                       String originMethod,
                       String targetFullQualifiedClassname) {
            this.originFullQualifiedClassname = originFullQualifiedClassname;
            this.originMethod = originMethod;
            this.targetFullQualifiedClassname = targetFullQualifiedClassname;
            methodMatcher = new MethodMatcher(originFullQualifiedClassname + " " + originMethod + "(..)");
        }

        @Override
        public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext executionContext) {
            J.MethodInvocation mi = super.visitMethodInvocation(method, executionContext);
            JavaType.Method type = method.getType();
            if (type != null && methodMatcher.matches(method)) {
                int paramCount = method.getArguments().size();
                String code = targetFullQualifiedClassname
                    + "."
                    + originMethod
                    + "("
                    + rangeClosed(1, paramCount).mapToObj(i -> "#{any()}").collect(joining(", "))
                    + ")";
                JavaTemplate listOfUsage = JavaTemplate.builder(this::getCursor, code).build();
                mi = method.withTemplate(listOfUsage, method.getCoordinates().replace(), method.getArguments().toArray());
                maybeRemoveImport(originFullQualifiedClassname);
                trackChange(executionContext, mi.getId());
            }
            return mi;
        }

        private void trackChange(ExecutionContext executionContext, UUID id) {
            executionContext.putMessageInSet(METHOD_CHANGE_PREFIX + targetFullQualifiedClassname, id);
        }

    }
}
