/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle;

/**
 * TODO We need to clean that up when other (binary) components have aligned on architecture namings
 * I put all different names and nomenclatures here to have a central place to check when we adopt further
 * ones ml and beats have been updated and bwc branches have been updated too.
 * */
public enum Architecture {

    AMD64("amd64", "x86_64", "x64", "X86_64", "x86_64", "amd64", "linux/amd64"),
    AARCH64("aarch64", "aarch64", "aarch64", "aarch64", "aarch64", "arm64", "linux/arm64");

    public final String classifier;
    public final String dockerPlatform;
    public final String bwcClassifier;
    public final String jdkClassifier;
    public final String mlClassifier;
    public final String debianClassifier;
    public final String rpmClassifier;

    Architecture(
        String classifier,
        String bwcClassifier,
        String jdkClassifier,
        String rpmClassifier,
        String mlClassifier,
        String debianClassifier,
        String dockerPlatform
    ) {
        this.classifier = classifier;
        this.bwcClassifier = bwcClassifier;
        this.jdkClassifier = jdkClassifier;
        this.rpmClassifier = rpmClassifier;
        this.mlClassifier = mlClassifier;
        this.debianClassifier = debianClassifier;
        this.dockerPlatform = dockerPlatform;
    }

    public static Architecture current() {
        final String architecture = System.getProperty("os.arch", "");
        return switch (architecture) {
            case "amd64", "x86_64" -> AMD64;
            case "aarch64", "arm64" -> AARCH64;
            default -> throw new IllegalArgumentException("can not determine architecture from [" + architecture + "]");
        };
    }

}
