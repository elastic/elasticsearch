/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

public enum Architecture {

    X64("x86_64", "linux/amd64"),
    AARCH64("aarch64", "linux/arm64");

    public final String classifier;
    public final String dockerPlatform;

    Architecture(String classifier, String dockerPlatform) {
        this.classifier = classifier;
        this.dockerPlatform = dockerPlatform;
    }

    public static Architecture current() {
        final String architecture = System.getProperty("os.arch", "");
        switch (architecture) {
            case "amd64":
            case "x86_64":
                return X64;
            case "aarch64":
                return AARCH64;
            default:
                throw new IllegalArgumentException("can not determine architecture from [" + architecture + "]");
        }
    }

}
