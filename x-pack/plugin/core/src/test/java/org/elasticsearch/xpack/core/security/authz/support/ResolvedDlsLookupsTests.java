/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ResolvedDlsLookupsTests extends ESTestCase {

    private final DlsLookup jobs = new DlsLookup("jobs", "ml_job_ids", Map.of("spaces", List.of("marketing")));
    private final DlsLookup owner = new DlsLookup("owner", "profile_uid", Map.of());

    public void testEncodeDecodeRoundTrip() {
        final ResolvedDlsLookups original = new ResolvedDlsLookups(
            Map.of(jobs.key(), List.of("j1", "j2"), owner.key(), Map.of("uid", "u_123", "active", true, "count", 3))
        );
        final ResolvedDlsLookups decoded = ResolvedDlsLookups.decode(original.encode());
        assertThat(decoded, equalTo(original));
        assertThat(decoded.contains(jobs), is(true));
        assertThat(decoded.get(jobs), equalTo(List.of("j1", "j2")));
        assertThat(decoded.get(owner), equalTo(Map.of("uid", "u_123", "active", true, "count", 3)));
    }

    public void testMergeKeepsExistingValues() {
        final ResolvedDlsLookups first = new ResolvedDlsLookups(Map.of(jobs.key(), List.of("j1")));
        assertThat(first.merge(Map.of()), sameInstance(first));

        final ResolvedDlsLookups merged = first.merge(Map.of(jobs.key(), List.of("changed"), owner.key(), "u_1"));
        assertThat(merged.get(jobs), equalTo(List.of("j1")));
        assertThat(merged.get(owner), equalTo("u_1"));
        // the original is untouched
        assertThat(first.contains(owner), is(false));
    }

    public void testReadFromContextWithoutHeaderIsEmpty() {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        assertThat(ResolvedDlsLookups.readFromContext(threadContext), sameInstance(ResolvedDlsLookups.EMPTY));
        assertThat(ResolvedDlsLookups.EMPTY.contains(jobs), is(false));
        assertThat(ResolvedDlsLookups.EMPTY.get(jobs), is(nullValue()));
    }

    public void testWriteAndReadFromContext() {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final ResolvedDlsLookups resolved = new ResolvedDlsLookups(Map.of(jobs.key(), List.of("j1")));
        resolved.writeToContext(threadContext);
        assertThat(ResolvedDlsLookups.readFromContext(threadContext), equalTo(resolved));
        // the header is a request header, so it is carried to downstream actions
        assertThat(threadContext.getRequestHeadersOnly().containsKey(AuthorizationServiceField.DLS_LOOKUPS_KEY), is(true));
        // writing again without clearing is a programming error
        expectThrows(IllegalArgumentException.class, () -> resolved.writeToContext(threadContext));
    }

    public void testExecuteWithResolvedDlsLookupsReplacesAndRestoresHeader() {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        threadContext.putHeader("unrelated", "kept");
        threadContext.putTransient("transient", "kept");

        final ResolvedDlsLookups initial = new ResolvedDlsLookups(Map.of(jobs.key(), List.of("j1")));
        initial.writeToContext(threadContext);
        final ResolvedDlsLookups replacement = initial.merge(Map.of(owner.key(), "u_1"));

        final boolean[] ran = new boolean[1];
        securityContext.executeWithResolvedDlsLookups(replacement, () -> {
            ran[0] = true;
            assertThat(securityContext.getResolvedDlsLookups(), equalTo(replacement));
            assertThat(threadContext.getHeader("unrelated"), equalTo("kept"));
            assertThat(threadContext.getTransient("transient"), equalTo("kept"));
        });
        assertThat(ran[0], is(true));
        assertThat(securityContext.getResolvedDlsLookups(), equalTo(initial));
    }

    public void testExecuteWithResolvedDlsLookupsWhenNonePresent() {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final SecurityContext securityContext = new SecurityContext(Settings.EMPTY, threadContext);
        final ResolvedDlsLookups resolved = new ResolvedDlsLookups(Map.of(jobs.key(), List.of("j1")));
        securityContext.executeWithResolvedDlsLookups(
            resolved,
            () -> assertThat(securityContext.getResolvedDlsLookups(), equalTo(resolved))
        );
        assertThat(securityContext.getResolvedDlsLookups(), sameInstance(ResolvedDlsLookups.EMPTY));
    }
}
