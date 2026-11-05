/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
     * @param blameStatus  {@code ok} or {@code uncommitted}
     */
    public record Line(String commit, String authorName, String authorEmail, String authoredAt, String authoredDate, String blameStatus) {}

    /** {@code git} is not on the path. */
    public static final class GitRequiredException extends RuntimeException {
        GitRequiredException(Throwable cause) {
            super("git is required to attribute TODO and FIXME comments", cause);
        }
    }

    /** The working tree is not a git repository, so the report cannot attribute comments. */
    public static final class NotARepositoryException extends RuntimeException {
        NotARepositoryException(String stderr) {
            super("todoReport requires a git checkout: " + stderr.trim());
        }
    }

    /** {@code git blame} failed for a reason other than an untracked file. */
    public static final class BlameFailedException extends RuntimeException {
        BlameFailedException(Path file, String detail) {
            super("git blame failed for " + file + ": " + detail.trim());
        }
    }

    /** The file is not in HEAD, so every marker in it is untracked. */
    public static final class UntrackedFileException extends RuntimeException {
        UntrackedFileException(Path file) {
            super(file.toString());
        }
    }

    private GitBlame() {}

    /**
     * Blames {@code file} from {@code workingDirectory}. Throws {@link UntrackedFileException} when
     * git reports that the path is not in HEAD. Throws when git is missing or the directory is not a repository.
     */
    public static Map<Integer, Line> blame(Path workingDirectory, Path file) throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder("git", "blame", "-w", "--line-porcelain", "--", file.toString());
        builder.directory(workingDirectory.toFile());
        Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            if (e.getMessage() != null && e.getMessage().contains("Cannot run program")) {
                throw new GitRequiredException(e);
            }
            throw e;
        }
        AtomicReference<IOException> drainFailure = new AtomicReference<>();
        byte[][] errBytes = new byte[1][];
        Thread drain = new Thread(() -> {
            try (InputStream stderr = process.getErrorStream()) {
                errBytes[0] = stderr.readAllBytes();
            } catch (IOException e) {
                drainFailure.set(e);
            }
        }, "git-blame-stderr");
        drain.setDaemon(true);
        drain.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode = process.waitFor();
        drain.join();
        if (drainFailure.get() != null) {
            throw drainFailure.get();
        }
        String stderr = new String(errBytes[0] == null ? new byte[0] : errBytes[0], StandardCharsets.UTF_8);
        if (exitCode != 0) {
            if (stderr.contains("not a git repository")) {
                throw new NotARepositoryException(stderr);
            }
            if (stderr.contains("no such path")) {
                throw new UntrackedFileException(file);
            }
            throw new BlameFailedException(file, stderr.isBlank() ? "exit " + exitCode : stderr);
        }
        return parsePorcelain(output);
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
            authored.toLocalDate().toString(),
            uncommitted ? "uncommitted" : "ok"
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
