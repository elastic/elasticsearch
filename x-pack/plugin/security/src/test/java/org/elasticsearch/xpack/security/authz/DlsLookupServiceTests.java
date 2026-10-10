/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.support.DlsLookup;
import org.elasticsearch.xpack.core.security.authz.support.DlsLookupResolver;
import org.elasticsearch.xpack.core.security.authz.support.ResolvedDlsLookups;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class DlsLookupServiceTests extends ESTestCase {

    private final Subject subject = new Subject(new User("alice", "analyst"), new RealmRef("native", "native", "node"));
    private final DlsLookup marketingJobs = new DlsLookup("jobs", "ml_job_ids", Map.of("spaces", List.of("marketing")));
    private final DlsLookup salesJobs = new DlsLookup("other_jobs", "ml_job_ids", Map.of("spaces", List.of("sales")));
    private final DlsLookup owner = new DlsLookup("owner", "profile_uid", Map.of());

    public void testNothingToResolveCompletesWithSameInstance() throws Exception {
        final AtomicInteger invocations = new AtomicInteger();
        final DlsLookupService service = new DlsLookupService(Map.of("ml_job_ids", countingResolver(invocations, List.of("j1"))));

        final ResolvedDlsLookups empty = randomBoolean() ? ResolvedDlsLookups.EMPTY : new ResolvedDlsLookups(Map.of("k", "v"));
        assertThat(resolve(service, List.of(), empty), sameInstance(empty));

        // already carried by the request: not resolved again
        final ResolvedDlsLookups already = new ResolvedDlsLookups(Map.of(marketingJobs.key(), List.of("j1")));
        assertThat(resolve(service, List.of(marketingJobs), already), sameInstance(already));
        assertThat(invocations.get(), equalTo(0));
    }

    public void testResolvesEachDistinctKeyOnce() throws Exception {
        final List<Map<String, Object>> seenParams = new ArrayList<>();
        final DlsLookupResolver jobsResolver = (params, effectiveSubject, listener) -> {
            assertThat(effectiveSubject, sameInstance(subject));
            seenParams.add(params);
            listener.onResponse(List.of("job-for-" + params.get("spaces")));
        };
        final AtomicInteger ownerInvocations = new AtomicInteger();
        final DlsLookupService service = new DlsLookupService(
            Map.of("ml_job_ids", jobsResolver, "profile_uid", countingResolver(ownerInvocations, "u_alice"))
        );

        // same type and params under a different name share a resolution
        final DlsLookup marketingJobsAlias = new DlsLookup("alias", marketingJobs.type(), marketingJobs.params());
        final ResolvedDlsLookups resolved = resolve(
            service,
            List.of(marketingJobs, marketingJobsAlias, salesJobs, owner),
            ResolvedDlsLookups.EMPTY
        );

        assertThat(seenParams, containsInAnyOrder(marketingJobs.params(), salesJobs.params()));
        assertThat(ownerInvocations.get(), equalTo(1));
        assertThat(resolved.get(marketingJobs), equalTo(List.of("job-for-[marketing]")));
        assertThat(resolved.get(marketingJobsAlias), equalTo(List.of("job-for-[marketing]")));
        assertThat(resolved.get(salesJobs), equalTo(List.of("job-for-[sales]")));
        assertThat(resolved.get(owner), equalTo("u_alice"));
    }

    public void testMergesWithAlreadyResolvedValues() throws Exception {
        final AtomicInteger invocations = new AtomicInteger();
        final DlsLookupService service = new DlsLookupService(Map.of("ml_job_ids", countingResolver(invocations, List.of("j2"))));
        final ResolvedDlsLookups already = new ResolvedDlsLookups(Map.of(marketingJobs.key(), List.of("j1")));

        final ResolvedDlsLookups resolved = resolve(service, List.of(marketingJobs, salesJobs), already);
        assertThat(invocations.get(), equalTo(1));
        assertThat(resolved.get(marketingJobs), equalTo(List.of("j1")));
        assertThat(resolved.get(salesJobs), equalTo(List.of("j2")));
    }

    public void testUnknownTypeFailsBeforeInvokingAnyResolver() {
        final AtomicInteger invocations = new AtomicInteger();
        final DlsLookupService service = new DlsLookupService(Map.of("ml_job_ids", countingResolver(invocations, List.of("j1"))));
        assertThat(service.hasResolver("ml_job_ids"), is(true));
        assertThat(service.hasResolver("profile_uid"), is(false));

        final ExecutionException e = expectThrows(
            ExecutionException.class,
            () -> resolve(service, List.of(marketingJobs, owner), ResolvedDlsLookups.EMPTY)
        );
        assertThat(e.getCause(), instanceOf(IllegalStateException.class));
        assertThat(
            e.getCause().getMessage(),
            equalTo("no DLS lookup resolver is registered for type [profile_uid] declared by lookup [owner]")
        );
        assertThat(invocations.get(), equalTo(0));
    }

    public void testNullValueFails() {
        final DlsLookupService service = new DlsLookupService(
            Map.of("ml_job_ids", (params, effectiveSubject, listener) -> listener.onResponse(null))
        );
        final ExecutionException e = expectThrows(
            ExecutionException.class,
            () -> resolve(service, List.of(marketingJobs), ResolvedDlsLookups.EMPTY)
        );
        assertThat(e.getCause(), instanceOf(IllegalStateException.class));
        assertThat(e.getCause().getMessage(), equalTo("DLS lookup resolver for type [ml_job_ids] returned null for lookup [jobs]"));
    }

    public void testResolverFailurePropagates() {
        final ElasticsearchException failure = new ElasticsearchException("lookup index unavailable");
        final DlsLookupResolver failing = (params, effectiveSubject, listener) -> {
            if (randomBoolean()) {
                listener.onFailure(failure);
            } else {
                throw failure;
            }
        };
        final DlsLookupService service = new DlsLookupService(
            Map.of("ml_job_ids", failing, "profile_uid", countingResolver(new AtomicInteger(), "u"))
        );
        final ExecutionException e = expectThrows(
            ExecutionException.class,
            () -> resolve(service, List.of(marketingJobs, owner), ResolvedDlsLookups.EMPTY)
        );
        assertThat(e.getCause(), sameInstance(failure));
    }

    public void testOrderOfDeclarationDoesNotAffectResult() throws Exception {
        final DlsLookupService service = new DlsLookupService(
            Map.of(
                "ml_job_ids",
                (params, effectiveSubject, listener) -> listener.onResponse(List.of(params.get("spaces").toString())),
                "profile_uid",
                (params, effectiveSubject, listener) -> listener.onResponse("u")
            )
        );
        final List<DlsLookup> lookups = new ArrayList<>(List.of(marketingJobs, salesJobs, owner));
        final ResolvedDlsLookups first = resolve(service, lookups, ResolvedDlsLookups.EMPTY);
        java.util.Collections.shuffle(lookups, random());
        final ResolvedDlsLookups second = resolve(service, lookups, ResolvedDlsLookups.EMPTY);
        assertThat(first, equalTo(second));
        assertThat(ResolvedDlsLookups.decode(first.encode()), equalTo(first));
        assertThat(first.get(marketingJobs), equalTo(List.of("[marketing]")));
        assertThat(List.of(first.get(salesJobs)), contains(List.of("[sales]")));
    }

    private ResolvedDlsLookups resolve(DlsLookupService service, List<DlsLookup> lookups, ResolvedDlsLookups already) throws Exception {
        final PlainActionFuture<ResolvedDlsLookups> future = new PlainActionFuture<>();
        service.resolve(lookups, already, subject, future);
        return future.get();
    }

    private static DlsLookupResolver countingResolver(AtomicInteger invocations, Object value) {
        return (params, effectiveSubject, listener) -> {
            invocations.incrementAndGet();
            ActionListener.completeWith(listener, () -> value);
        };
    }
}
