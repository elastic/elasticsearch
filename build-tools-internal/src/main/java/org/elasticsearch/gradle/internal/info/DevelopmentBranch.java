/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.info;

import org.elasticsearch.gradle.Version;

import java.io.Serializable;
import java.util.Objects;

/**
 * Information about a development branch used in branches.json file
 *
 * @param name Name of the development branch
 * @param version Elasticsearch version on the development branch
 */
public record DevelopmentBranch(String name, Version version) implements Serializable {
    public DevelopmentBranch {
        Objects.requireNonNull(name);
        Objects.requireNonNull(version);
    }
}
