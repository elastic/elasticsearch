/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class ThreadContextTests extends ESTestCase {

    public void testStashContext() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader("foo", "bar");
        threadContext.putTransient("ctx.foo", 1);
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            assertNull(threadContext.getHeader("foo"));
            assertNull(threadContext.getTransient("ctx.foo"));
            assertEquals("1", threadContext.getHeader("default"));
        }

        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
    }

    public void testNewContextWithClearedTransients() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient("foo", "bar");
        threadContext.putTransient("bar", "baz");
        threadContext.putHeader("foo", "bar");
        threadContext.putHeader("baz", "bar");
        threadContext.addResponseHeader("foo", "bar");
        threadContext.addResponseHeader("bar", "qux");

        // this is missing or null
        if (randomBoolean()) {
            threadContext.putTransient("acme", null);
        }

        // foo is the only existing transient header that is cleared
        try (
            ThreadContext.StoredContext stashed = threadContext.newStoredContext(
                randomFrom(List.of("foo", "foo"), List.of("foo"), List.of("foo", "acme"))
            )
        ) {
            // only the requested transient header is cleared
            assertNull(threadContext.getTransient("foo"));
            // missing header is still missing
            assertNull(threadContext.getTransient("acme"));
            // other headers are preserved
            assertEquals("baz", threadContext.getTransient("bar"));
            assertEquals("bar", threadContext.getHeader("foo"));
            assertEquals("bar", threadContext.getHeader("baz"));
            assertEquals("bar", threadContext.getResponseHeaders().get("foo").get(0));
            assertEquals("qux", threadContext.getResponseHeaders().get("bar").get(0));

            // try override stashed header
            threadContext.putTransient("foo", "acme");
            assertEquals("acme", threadContext.getTransient("foo"));
            // add new headers
            threadContext.putTransient("baz", "bar");
            threadContext.putHeader("bar", "baz");
            threadContext.addResponseHeader("baz", "bar");
            threadContext.addResponseHeader("foo", "baz");
        }

        // original is restored (it is not overridden)
        assertEquals("bar", threadContext.getTransient("foo"));
        // headers added inside the stash are NOT preserved
        assertNull(threadContext.getTransient("baz"));
        assertNull(threadContext.getHeader("bar"));
        assertNull(threadContext.getResponseHeaders().get("baz"));
        // original headers are restored
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals("bar", threadContext.getHeader("baz"));
        assertEquals("bar", threadContext.getResponseHeaders().get("foo").get(0));
        assertEquals(1, threadContext.getResponseHeaders().get("foo").size());
        assertEquals("qux", threadContext.getResponseHeaders().get("bar").get(0));

        // test stashed missing header stays missing
        try (
            ThreadContext.StoredContext stashed = threadContext.newStoredContext(
                randomFrom(Arrays.asList("acme", "acme"), Arrays.asList("acme"))
            )
        ) {
            assertNull(threadContext.getTransient("acme"));
            threadContext.putTransient("acme", "foo");
        }
        assertNull(threadContext.getTransient("acme"));
    }

    public void testStashWithOrigin() {
        final String origin = randomAlphaOfLengthBetween(4, 16);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        final boolean setOtherValues = randomBoolean();
        if (setOtherValues) {
            threadContext.putTransient("foo", "bar");
            threadContext.putHeader("foo", "bar");
        }

        assertNull(threadContext.getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME));
        try (ThreadContext.StoredContext storedContext = threadContext.stashWithOrigin(origin)) {
            assertEquals(origin, threadContext.getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME));
            assertNull(threadContext.getTransient("foo"));
            assertNull(threadContext.getTransient("bar"));
        }

        assertNull(threadContext.getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME));

        if (setOtherValues) {
            assertEquals("bar", threadContext.getTransient("foo"));
            assertEquals("bar", threadContext.getHeader("foo"));
        }
    }

    public void testStashAndMerge() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader("foo", "bar");
        threadContext.putTransient("ctx.foo", 1);
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
        HashMap<String, String> toMerge = new HashMap<>();
        toMerge.put("foo", "baz");
        toMerge.put("simon", "says");
        try (ThreadContext.StoredContext ctx = threadContext.stashAndMergeHeaders(toMerge)) {
            assertEquals("bar", threadContext.getHeader("foo"));
            assertEquals("says", threadContext.getHeader("simon"));
            assertNull(threadContext.getTransient("ctx.foo"));
            assertEquals("1", threadContext.getHeader("default"));
        }

        assertNull(threadContext.getHeader("simon"));
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
    }

    public void testStoreContext() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader("foo", "bar");
        threadContext.putTransient("ctx.foo", 1);
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
        ThreadContext.StoredContext storedContext = threadContext.newStoredContext();
        threadContext.putHeader("foo.bar", "baz");
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            assertNull(threadContext.getHeader("foo"));
            assertNull(threadContext.getTransient("ctx.foo"));
            assertEquals("1", threadContext.getHeader("default"));
        }

        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
        assertEquals("baz", threadContext.getHeader("foo.bar"));
        if (randomBoolean()) {
            storedContext.restore();
        } else {
            storedContext.close();
        }
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
        assertNull(threadContext.getHeader("foo.bar"));
    }

    public void testRestorableContext() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader("foo", "bar");
        threadContext.putTransient("ctx.foo", 1);
        threadContext.addResponseHeader("resp.header", "baaaam");
        Supplier<ThreadContext.StoredContext> contextSupplier = threadContext.newRestorableContext(true);

        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            assertNull(threadContext.getHeader("foo"));
            assertEquals("1", threadContext.getHeader("default"));
            threadContext.addResponseHeader("resp.header", "boom");
            try (ThreadContext.StoredContext tmp = contextSupplier.get()) {
                assertEquals("bar", threadContext.getHeader("foo"));
                assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
                assertEquals("1", threadContext.getHeader("default"));
                assertEquals(2, threadContext.getResponseHeaders().get("resp.header").size());
                assertEquals("boom", threadContext.getResponseHeaders().get("resp.header").get(0));
                assertEquals("baaaam", threadContext.getResponseHeaders().get("resp.header").get(1));
            }
            assertNull(threadContext.getHeader("foo"));
            assertNull(threadContext.getTransient("ctx.foo"));
            assertEquals(1, threadContext.getResponseHeaders().get("resp.header").size());
            assertEquals("boom", threadContext.getResponseHeaders().get("resp.header").get(0));
        }
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
        assertEquals(1, threadContext.getResponseHeaders().get("resp.header").size());
        assertEquals("baaaam", threadContext.getResponseHeaders().get("resp.header").get(0));

        contextSupplier = threadContext.newRestorableContext(false);

        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            assertNull(threadContext.getHeader("foo"));
            assertEquals("1", threadContext.getHeader("default"));
            threadContext.addResponseHeader("resp.header", "boom");
            try (ThreadContext.StoredContext tmp = contextSupplier.get()) {
                assertEquals("bar", threadContext.getHeader("foo"));
                assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
                assertEquals("1", threadContext.getHeader("default"));
                assertEquals(1, threadContext.getResponseHeaders().get("resp.header").size());
                assertEquals("baaaam", threadContext.getResponseHeaders().get("resp.header").get(0));
            }
            assertNull(threadContext.getHeader("foo"));
            assertNull(threadContext.getTransient("ctx.foo"));
            assertEquals(1, threadContext.getResponseHeaders().get("resp.header").size());
            assertEquals("boom", threadContext.getResponseHeaders().get("resp.header").get(0));
        }

        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
        assertEquals(1, threadContext.getResponseHeaders().get("resp.header").size());
        assertEquals("baaaam", threadContext.getResponseHeaders().get("resp.header").get(0));
    }

    public void testResponseHeaders() {
        final boolean expectThird = randomBoolean();

        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        threadContext.addResponseHeader("foo", "bar");
        // pretend that another thread created the same response
        if (randomBoolean()) {
            threadContext.addResponseHeader("foo", "bar");
        }

        final String value = HeaderWarning.formatWarning("qux");
        threadContext.addResponseHeader("baz", value, s -> HeaderWarning.extractWarningValueFromWarningHeader(s, false));
        // pretend that another thread created the same response at a different time
        if (randomBoolean()) {
            final String duplicateValue = HeaderWarning.formatWarning("qux");
            threadContext.addResponseHeader("baz", duplicateValue, s -> HeaderWarning.extractWarningValueFromWarningHeader(s, false));
        }

        threadContext.addResponseHeader("Warning", "One is the loneliest number");
        threadContext.addResponseHeader("Warning", "Two can be as bad as one");
        if (expectThird) {
            threadContext.addResponseHeader("Warning", "No is the saddest experience");
        }

        final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
        final List<String> foo = responseHeaders.get("foo");
        final List<String> baz = responseHeaders.get("baz");
        final List<String> warnings = responseHeaders.get("Warning");
        final int expectedWarnings = expectThird ? 3 : 2;

        assertThat(foo, hasSize(1));
        assertThat(baz, hasSize(1));
        assertEquals("bar", foo.get(0));
        assertEquals(value, baz.get(0));
        assertThat(warnings, hasSize(expectedWarnings));
        assertThat(warnings, hasItem(equalTo("One is the loneliest number")));
        assertThat(warnings, hasItem(equalTo("Two can be as bad as one")));

        if (expectThird) {
            assertThat(warnings, hasItem(equalTo("No is the saddest experience")));
        }
    }

    public void testCopyHeaders() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.copyHeaders(Collections.<String, String>emptyMap().entrySet());
        threadContext.copyHeaders(Collections.<String, String>singletonMap("foo", "bar").entrySet());
        assertEquals("bar", threadContext.getHeader("foo"));
    }

    public void testSerialize() throws IOException {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader("foo", "bar");
        threadContext.putTransient("ctx.foo", 1);
        threadContext.addResponseHeader("Warning", "123456");
        if (rarely()) {
            threadContext.addResponseHeader("Warning", "123456");
        }
        threadContext.addResponseHeader("Warning", "234567");

        BytesStreamOutput out = new BytesStreamOutput();
        threadContext.writeTo(out);
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            assertNull(threadContext.getHeader("foo"));
            assertNull(threadContext.getTransient("ctx.foo"));
            assertTrue(threadContext.getResponseHeaders().isEmpty());
            assertEquals("1", threadContext.getHeader("default"));

            threadContext.readHeaders(out.bytes().streamInput());
            assertEquals("bar", threadContext.getHeader("foo"));
            assertNull(threadContext.getTransient("ctx.foo"));

            final Map<String, List<String>> responseHeaders = threadContext.getResponseHeaders();
            final List<String> warnings = responseHeaders.get("Warning");

            assertThat(responseHeaders.keySet(), hasSize(1));
            assertThat(warnings, hasSize(2));
            assertThat(warnings, hasItem(equalTo("123456")));
            assertThat(warnings, hasItem(equalTo("234567")));
        }
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(Integer.valueOf(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
    }

    public void testSerializeInDifferentContext() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        {
            Settings build = Settings.builder().put("request.headers.default", "1").build();
            ThreadContext threadContext = new ThreadContext(build);
            threadContext.putHeader("foo", "bar");
            threadContext.putTransient("ctx.foo", 1);
            threadContext.addResponseHeader("Warning", "123456");
            if (rarely()) {
                threadContext.addResponseHeader("Warning", "123456");
            }
            threadContext.addResponseHeader("Warning", "234567");

            assertEquals("bar", threadContext.getHeader("foo"));
            assertNotNull(threadContext.getTransient("ctx.foo"));
            assertEquals("1", threadContext.getHeader("default"));
            assertThat(threadContext.getResponseHeaders().keySet(), hasSize(1));
            threadContext.writeTo(out);
        }
        {
            Settings otherSettings = Settings.builder().put("request.headers.default", "5").build();
            ThreadContext otherThreadContext = new ThreadContext(otherSettings);
            otherThreadContext.readHeaders(out.bytes().streamInput());

            assertEquals("bar", otherThreadContext.getHeader("foo"));
            assertNull(otherThreadContext.getTransient("ctx.foo"));
            assertEquals("1", otherThreadContext.getHeader("default"));

            final Map<String, List<String>> responseHeaders = otherThreadContext.getResponseHeaders();
            final List<String> warnings = responseHeaders.get("Warning");

            assertThat(responseHeaders.keySet(), hasSize(1));
            assertThat(warnings, hasSize(2));
            assertThat(warnings, hasItem(equalTo("123456")));
            assertThat(warnings, hasItem(equalTo("234567")));
        }
    }

    public void testSerializeInDifferentContextNoDefaults() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        {
            ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            threadContext.putHeader("foo", "bar");
            threadContext.putTransient("ctx.foo", 1);

            assertEquals("bar", threadContext.getHeader("foo"));
            assertNotNull(threadContext.getTransient("ctx.foo"));
            assertNull(threadContext.getHeader("default"));
            threadContext.writeTo(out);
        }
        {
            Settings otherSettings = Settings.builder().put("request.headers.default", "5").build();
            ThreadContext otherhreadContext = new ThreadContext(otherSettings);
            otherhreadContext.readHeaders(out.bytes().streamInput());

            assertEquals("bar", otherhreadContext.getHeader("foo"));
            assertNull(otherhreadContext.getTransient("ctx.foo"));
            assertEquals("5", otherhreadContext.getHeader("default"));
        }
    }

    public void testCanResetDefault() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader("default", "2");
        assertEquals("2", threadContext.getHeader("default"));
    }

    public void testStashAndMergeWithModifiedDefaults() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        HashMap<String, String> toMerge = new HashMap<>();
        toMerge.put("default", "2");
        try (ThreadContext.StoredContext ctx = threadContext.stashAndMergeHeaders(toMerge)) {
            assertEquals("2", threadContext.getHeader("default"));
        }

        build = Settings.builder().put("request.headers.default", "1").build();
        threadContext = new ThreadContext(build);
        threadContext.putHeader("default", "4");
        toMerge = new HashMap<>();
        toMerge.put("default", "2");
        try (ThreadContext.StoredContext ctx = threadContext.stashAndMergeHeaders(toMerge)) {
            assertEquals("4", threadContext.getHeader("default"));
        }
    }

    public void testPreserveContext() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Runnable withContext;

        // Create a runnable that should run with some header
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            withContext = threadContext.preserveContext(
                sometimesAbstractRunnable(() -> { assertEquals("bar", threadContext.getHeader("foo")); })
            );
        }

        // We don't see the header outside of the runnable
        assertNull(threadContext.getHeader("foo"));

        // But we do inside of it
        withContext.run();

        // but not after
        assertNull(threadContext.getHeader("foo"));
    }

    public void testPreserveContextKeepsOriginalContextWhenCalledTwice() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Runnable originalWithContext;
        Runnable withContext;

        // Create a runnable that should run with some header
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            withContext = threadContext.preserveContext(
                sometimesAbstractRunnable(() -> { assertEquals("bar", threadContext.getHeader("foo")); })
            );
        }

        // Now attempt to rewrap it
        originalWithContext = withContext;
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("foo", "zot");
            withContext = threadContext.preserveContext(withContext);
        }

        // We get the original context inside the runnable
        withContext.run();

        // In fact the second wrapping didn't even change it
        assertThat(withContext, sameInstance(originalWithContext));
    }

    public void testPreservesThreadsOriginalContextOnRunException() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Runnable withContext;

        // create a abstract runnable, add headers and transient objects and verify in the methods
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            boolean systemContext = randomBoolean();
            if (systemContext) {
                threadContext.markAsSystemContext();
            }
            threadContext.putTransient("foo", "bar_transient");
            withContext = threadContext.preserveContext(new AbstractRunnable() {

                @Override
                public void onAfter() {
                    assertEquals(systemContext, threadContext.isSystemContext());
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertEquals("bar_transient", threadContext.getTransient("foo"));
                    assertNotNull(threadContext.getTransient("failure"));
                    assertEquals("exception from doRun", ((RuntimeException) threadContext.getTransient("failure")).getMessage());
                    assertFalse(threadContext.isDefaultContext());
                    threadContext.putTransient("after", "after");
                }

                @Override
                public void onFailure(Exception e) {
                    assertEquals(systemContext, threadContext.isSystemContext());
                    assertEquals("exception from doRun", e.getMessage());
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertEquals("bar_transient", threadContext.getTransient("foo"));
                    assertFalse(threadContext.isDefaultContext());
                    threadContext.putTransient("failure", e);
                }

                @Override
                protected void doRun() throws Exception {
                    assertEquals(systemContext, threadContext.isSystemContext());
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertEquals("bar_transient", threadContext.getTransient("foo"));
                    assertFalse(threadContext.isDefaultContext());
                    throw new RuntimeException("exception from doRun");
                }
            });
        }

        // We don't see the header outside of the runnable
        assertNull(threadContext.getHeader("foo"));
        assertNull(threadContext.getTransient("foo"));
        assertNull(threadContext.getTransient("failure"));
        assertNull(threadContext.getTransient("after"));
        assertTrue(threadContext.isDefaultContext());

        // But we do inside of it
        withContext.run();

        // verify not seen after
        assertNull(threadContext.getHeader("foo"));
        assertNull(threadContext.getTransient("foo"));
        assertNull(threadContext.getTransient("failure"));
        assertNull(threadContext.getTransient("after"));
        assertTrue(threadContext.isDefaultContext());

        // repeat with regular runnable
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            threadContext.putTransient("foo", "bar_transient");
            withContext = threadContext.preserveContext(() -> {
                assertEquals("bar", threadContext.getHeader("foo"));
                assertEquals("bar_transient", threadContext.getTransient("foo"));
                assertFalse(threadContext.isDefaultContext());
                threadContext.putTransient("run", true);
                throw new RuntimeException("exception from run");
            });
        }

        assertNull(threadContext.getHeader("foo"));
        assertNull(threadContext.getTransient("foo"));
        assertNull(threadContext.getTransient("run"));
        assertTrue(threadContext.isDefaultContext());

        final Runnable runnable = withContext;
        RuntimeException e = expectThrows(RuntimeException.class, runnable::run);
        assertEquals("exception from run", e.getMessage());
        assertNull(threadContext.getHeader("foo"));
        assertNull(threadContext.getTransient("foo"));
        assertNull(threadContext.getTransient("run"));
        assertTrue(threadContext.isDefaultContext());
    }

    public void testPreservesThreadsOriginalContextOnFailureException() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Runnable withContext;

        // a runnable that throws from onFailure
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            threadContext.putTransient("foo", "bar_transient");
            withContext = threadContext.preserveContext(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException("from onFailure", e);
                }

                @Override
                protected void doRun() throws Exception {
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertEquals("bar_transient", threadContext.getTransient("foo"));
                    assertFalse(threadContext.isDefaultContext());
                    throw new RuntimeException("from doRun");
                }
            });
        }

        // We don't see the header outside of the runnable
        assertNull(threadContext.getHeader("foo"));
        assertNull(threadContext.getTransient("foo"));
        assertTrue(threadContext.isDefaultContext());

        // But we do inside of it
        RuntimeException e = expectThrows(RuntimeException.class, withContext::run);
        assertEquals("from onFailure", e.getMessage());
        assertEquals("from doRun", e.getCause().getMessage());

        // but not after
        assertNull(threadContext.getHeader("foo"));
        assertNull(threadContext.getTransient("foo"));
        assertTrue(threadContext.isDefaultContext());
    }

    public void testPreservesThreadsOriginalContextOnAfterException() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Runnable withContext;

        // a runnable that throws from onAfter
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("foo", "bar");
            threadContext.putTransient("foo", "bar_transient");
            withContext = threadContext.preserveContext(new AbstractRunnable() {

                @Override
                public void onAfter() {
                    throw new RuntimeException("from onAfter");
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException("from onFailure", e);
                }

                @Override
                protected void doRun() throws Exception {
                    assertEquals("bar", threadContext.getHeader("foo"));
                    assertEquals("bar_transient", threadContext.getTransient("foo"));
                    assertFalse(threadContext.isDefaultContext());
                }
            });
        }

        // We don't see the header outside of the runnable
        assertNull(threadContext.getHeader("foo"));
        assertNull(threadContext.getTransient("foo"));
        assertTrue(threadContext.isDefaultContext());

        // But we do inside of it
        RuntimeException e = expectThrows(RuntimeException.class, withContext::run);
        assertEquals("from onAfter", e.getMessage());
        assertNull(e.getCause());

        // but not after
        assertNull(threadContext.getHeader("foo"));
        assertNull(threadContext.getTransient("foo"));
        assertTrue(threadContext.isDefaultContext());
    }

    public void testMarkAsSystemContext() throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        assertFalse(threadContext.isSystemContext());
        try (ThreadContext.StoredContext context = threadContext.stashContext()) {
            assertFalse(threadContext.isSystemContext());
            threadContext.markAsSystemContext();
            assertTrue(threadContext.isSystemContext());
        }
        assertFalse(threadContext.isSystemContext());
    }

    public void testPutHeaders() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader(Collections.<String, String>emptyMap());
        threadContext.putHeader(Collections.<String, String>singletonMap("foo", "bar"));
        assertEquals("bar", threadContext.getHeader("foo"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> threadContext.putHeader(Collections.<String, String>singletonMap("foo", "boom"))
        );
        assertEquals("value for key [foo] already present", e.getMessage());
    }

    public void testHeadersCopiedOnStash() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, "x-opaque-id");
            threadContext.putHeader(Task.TRACE_ID, "0af7651916cd43dd8448eb211c80319c");
            threadContext.putHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "kibana");

            try (ThreadContext.StoredContext ignored2 = threadContext.stashContext()) {
                assertEquals("x-opaque-id", threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER));
                assertEquals("0af7651916cd43dd8448eb211c80319c", threadContext.getHeader(Task.TRACE_ID));
                assertEquals("kibana", threadContext.getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER));
            }

            assertEquals("x-opaque-id", threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER));
            assertEquals("0af7651916cd43dd8448eb211c80319c", threadContext.getHeader(Task.TRACE_ID));
            assertEquals("kibana", threadContext.getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER));
        }
    }

    /**
     * Sometimes wraps a Runnable in an AbstractRunnable.
     */
    private Runnable sometimesAbstractRunnable(Runnable r) {
        if (random().nextBoolean()) {
            return r;
        }
        return new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }

            @Override
            protected void doRun() throws Exception {
                r.run();
            }
        };
    }
}
