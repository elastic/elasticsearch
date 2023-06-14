/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;

record Parameter(TypeName type, String name) {
    static Parameter from(VariableElement e) {
        return new Parameter(ClassName.get(e.asType()), e.getSimpleName().toString());
    }

    void declareField(TypeSpec.Builder builder) {
        builder.addField(type(), name(), Modifier.PRIVATE, Modifier.FINAL);
    }

    void buildCtor(MethodSpec.Builder builder) {
        builder.addParameter(type(), name());
        builder.addStatement("this.$N = $N", name(), name());
    }
}
