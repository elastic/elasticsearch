/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import org.junit.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class GitBlameTests {

    private static final String PORCELAIN = """
        0000000000000000000000000000000000000000 1 1 1
        author Not Committed Yet
        author-mail <not.committed.yet>
        author-time 1710000000
        author-tz -0700
        committer Not Committed Yet
        committer-mail <not.committed.yet>
        committer-time 1710000000
        committer-tz -0700
        summary
        filename Foo.java
        \t// TODO: uncommitted
        abcdefabcdefabcdefabcdefabcdefabcdefabcd 2 2 1
        author Ada Lovelace
        author-mail <ada@example.com>
        author-time 1710000000
        author-tz +0000
        committer Ada Lovelace
        committer-mail <ada@example.com>
        committer-time 1710000000
        committer-tz +0000
        summary add todo
        previous abcdefabcdefabcdefabcdefabcdefabcdefabcd Foo.java
        filename Foo.java
        \t// FIXME: later
        """;

    @Test
    public void testParsesAuthorEmailTimeAndUncommitted() {
        Map<Integer, GitBlame.Line> lines = GitBlame.parsePorcelain(PORCELAIN);

        GitBlame.Line uncommitted = lines.get(1);
        assertThat(uncommitted.commit(), nullValue());
        assertThat(uncommitted.authorName(), equalTo("Not Committed Yet"));
        assertThat(uncommitted.authorEmail(), equalTo("not.committed.yet"));
        OffsetDateTime pacific = Instant.ofEpochSecond(1710000000L).atOffset(ZoneOffset.ofHoursMinutes(-7, 0));
        assertThat(uncommitted.authoredAt(), equalTo(pacific.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
        assertThat(uncommitted.authoredDate(), equalTo(pacific.toLocalDate().toString()));

        GitBlame.Line committed = lines.get(2);
        assertThat(committed.commit(), equalTo("abcdefabcdefabcdefabcdefabcdefabcdefabcd"));
        assertThat(committed.authorName(), equalTo("Ada Lovelace"));
        assertThat(committed.authorEmail(), equalTo("ada@example.com"));
        OffsetDateTime utc = Instant.ofEpochSecond(1710000000L).atOffset(ZoneOffset.UTC);
        assertThat(committed.authoredAt(), equalTo(utc.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
        assertThat(committed.authoredDate(), equalTo(utc.toLocalDate().toString()));
    }
}
