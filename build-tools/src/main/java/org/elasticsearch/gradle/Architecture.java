/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle;

public enum Architecture {

    AMD64("amd64", "x64", "x86_64", "amd64", "linux/amd64"),
    AARCH64("aarch64", "aarch64", "aarch64", "arm64", "linux/arm64");

    public final String classifier;
    public final String dockerPlatform;
    public final String jdkClassifier;
    public final String mlClassifier;
    public final String debianClassifier;

    Architecture(String classifier, String jdkClassifier, String mlClassifier, String debianClassifier, String dockerPlatform) {
        this.classifier = classifier;
        this.jdkClassifier = jdkClassifier;
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
