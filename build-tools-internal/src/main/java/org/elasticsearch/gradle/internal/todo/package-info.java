/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * Reports to-do and fix-me comments in Java sources, with the git author of each line and the
 * Gradle module that owns the file, as JSON and CSV a dashboard can group by date, author, and area.
 */
package org.elasticsearch.gradle.internal.todo;
