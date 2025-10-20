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

    X64("x86_64", "linux/amd64", "amd64", "x64"),
    AARCH64("aarch64", "linux/arm64", "arm64", "aarch64");

    public final String classifier;
    public final String dockerPlatform;
    public final String dockerClassifier;
    public final String javaClassifier;

    Architecture(String classifier, String dockerPlatform, String dockerClassifier, String javaClassifier) {
        this.classifier = classifier;
        this.dockerPlatform = dockerPlatform;
        this.dockerClassifier = dockerClassifier;
        this.javaClassifier = javaClassifier;
    }

    public static Architecture current() {
        final String architecture = System.getProperty("os.arch", "");
        return switch (architecture) {
            case "amd64", "x86_64" -> X64;
            case "aarch64" -> AARCH64;
            default -> throw new IllegalArgumentException("can not determine architecture from [" + architecture + "]");
        };
    }

}
