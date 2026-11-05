/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * One to-do or fix-me comment, flat so a dashboard can group the rows without parsing source.
 *
 * @param marker       upper-case marker text
 * @param message      text after the marker on the same line
 * @param assignee     name in parentheses immediately after the marker, or null when the comment names nobody
 * @param file         path relative to the repository root, with forward slashes
 * @param module       Gradle project path of the file
 * @param area         first segment of {@code module}, such as {@code server} or {@code x-pack}
 * @param sourceSet    source set that contains the file, such as {@code main} or {@code test}
 * @param line         1-based line of the marker
 * @param authorName   git author of that line, or null when blame could not name one
 * @param authorEmail  git author email, or null
 * @param authoredAt   ISO-8601 author time with its offset, or null
 * @param authoredDate author calendar date in that offset ({@code YYYY-MM-DD}), or null
 * @param commit       blame commit, or null when the line is uncommitted or untracked
 * @param blameStatus  {@code ok}, {@code uncommitted}, or {@code untracked}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TodoEntry(
    String marker,
    String message,
    String assignee,
    String file,
    String module,
    String area,
    String sourceSet,
    int line,
    String authorName,
    String authorEmail,
    String authoredAt,
    String authoredDate,
    String commit,
    String blameStatus
) {}
