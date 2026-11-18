/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import org.gradle.process.ExecOperations;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads {@code git blame --line-porcelain} for one file. One invocation carries the author of
 * every line, so a file with several to-do comments is blamed once.
 */
public final class GitBlame {

    private static final Pattern HEADER = Pattern.compile("^([0-9a-f]{40}) (\\d+) (\\d+)");
    private static final String UNCOMMITTED = "0000000000000000000000000000000000000000";

    /**
     * Blame of a single final line.
     *
     * @param commit       commit id, or null when the line is not committed
     * @param authorName   author name from blame
     * @param authorEmail  author email without angle brackets
     * @param authoredAt   ISO-8601 author time with the author offset
     * @param authoredDate calendar date of {@code authoredAt} in that offset
     */
    public record Line(String commit, String authorName, String authorEmail, String authoredAt, String authoredDate) {}

    /** The file is not in HEAD, so every marker in it is untracked. */
    public static final class UntrackedFileException extends RuntimeException {
        UntrackedFileException(Path file) {
            super(file.toString());
        }
    }

    private GitBlame() {}

    /**
     * Blames {@code file} from {@code workingDirectory}. {@code git ls-files} prints the path when the
     * file is tracked and nothing when it is not; an empty listing throws {@link UntrackedFileException}
     * and blame is not run. A missing git binary or a directory that is not a repository fails the
     * Gradle exec.
     */
    public static Map<Integer, Line> blame(ExecOperations execOperations, Path workingDirectory, Path file) {
        String tracked = runGit(execOperations, workingDirectory, "git", "ls-files", "--", file.toString());
        if (tracked.isBlank()) {
            throw new UntrackedFileException(file);
        }
        String blamed = runGit(execOperations, workingDirectory, "git", "blame", "-w", "--line-porcelain", "--", file.toString());
        return parsePorcelain(blamed);
    }

    private static String runGit(ExecOperations execOperations, Path workingDirectory, String... args) {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        execOperations.exec(spec -> {
            spec.setCommandLine((Object[]) args);
            spec.setWorkingDir(workingDirectory.toFile());
            spec.setStandardOutput(stdout);
            spec.setErrorOutput(OutputStream.nullOutputStream());
        });
        return stdout.toString(StandardCharsets.UTF_8);
    }

    /**
     * Parses {@code git blame --line-porcelain} output into final line numbers. A zero commit id
     * is {@code uncommitted}; every other line is {@code ok}.
     */
    public static Map<Integer, Line> parsePorcelain(String porcelain) {
        String[] rows = porcelain.split("\n", -1);
        Map<Integer, Line> lines = new HashMap<>();
        int i = 0;
        while (i < rows.length) {
            String row = stripCarriageReturn(rows[i]);
            Matcher header = HEADER.matcher(row);
            if (header.find() == false) {
                i++;
                continue;
            }
            String sha = header.group(1);
            int finalLine = Integer.parseInt(header.group(3));
            i++;
            String author = null;
            String email = null;
            Long authorTime = null;
            String authorTz = null;
            while (i < rows.length && stripCarriageReturn(rows[i]).startsWith("\t") == false) {
                String meta = stripCarriageReturn(rows[i]);
                if (meta.startsWith("author ")) {
                    author = meta.substring("author ".length());
                } else if (meta.startsWith("author-mail ")) {
                    email = stripMail(meta.substring("author-mail ".length()));
                } else if (meta.startsWith("author-time ")) {
                    authorTime = Long.parseLong(meta.substring("author-time ".length()).trim());
                } else if (meta.startsWith("author-tz ")) {
                    authorTz = meta.substring("author-tz ".length()).trim();
                }
                i++;
            }
            if (i < rows.length && stripCarriageReturn(rows[i]).startsWith("\t")) {
                i++;
            }
            if (author != null && authorTime != null && authorTz != null) {
                lines.put(finalLine, toLine(sha, author, email, authorTime, authorTz));
            }
        }
        return lines;
    }

    private static Line toLine(String sha, String author, String email, long authorTime, String authorTz) {
        ZoneOffset offset = zoneOffset(authorTz);
        OffsetDateTime authored = Instant.ofEpochSecond(authorTime).atOffset(offset);
        boolean uncommitted = UNCOMMITTED.equals(sha);
        return new Line(
            uncommitted ? null : sha,
            author,
            email,
            authored.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
            authored.toLocalDate().toString()
        );
    }

    private static ZoneOffset zoneOffset(String authorTz) {
        int sign = authorTz.charAt(0) == '-' ? -1 : 1;
        int hours = Integer.parseInt(authorTz.substring(1, 3));
        int minutes = Integer.parseInt(authorTz.substring(3, 5));
        return ZoneOffset.ofHoursMinutes(sign * hours, sign * minutes);
    }

    private static String stripMail(String mail) {
        String trimmed = mail.trim();
        if (trimmed.startsWith("<") && trimmed.endsWith(">") && trimmed.length() >= 2) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static String stripCarriageReturn(String line) {
        if (line.endsWith("\r")) {
            return line.substring(0, line.length() - 1);
        }
        return line;
    }
}
