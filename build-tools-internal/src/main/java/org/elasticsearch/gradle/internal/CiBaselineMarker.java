/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

/**
 * DO NOT MERGE. Exists only on the CI time reduction baseline branch: adding it changes the build-tools-internal
 * classpath, so tasks configured by these plugins miss the remote build cache the same way they do on the branch
 * being benchmarked, and every test runs. It is not referenced anywhere.
 */
final class CiBaselineMarker {
    static final String PURPOSE = "ci-time-reduction baseline benchmark";

    private CiBaselineMarker() {}
}
