/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.MethodSpec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class TryWithResources {
    private final List<Object> tryArgs = new ArrayList<>();
    private final MethodSpec.Builder builder;
    private final String eachLine;
    private int count = 0;

    TryWithResources(MethodSpec.Builder builder, String eachLine) {
        this.builder = builder;
        this.eachLine = eachLine;
    }

    void addLine(Object... params) {
        Collections.addAll(tryArgs, params);
        count++;
    }

    void build() {
        StringBuilder tryFmt = new StringBuilder("try (");
        if (count == 1) {
            tryFmt.append(eachLine).append(")");
        } else {
            for (int i = 0; i < count; i++) {
                tryFmt.append("\n  ").append(eachLine).append(";");
            }
            tryFmt.append("\n)");
        }
        builder.beginControlFlow(tryFmt.toString(), tryArgs.toArray());
    }
}
