/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.Matchers.sameInstance;

public class ThreadContextTests extends ESTestCase {

    public void testStashContext() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader("foo", "bar");
        threadContext.putTransient("ctx.foo", new Integer(1));
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(new Integer(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            assertNull(threadContext.getHeader("foo"));
            assertNull(threadContext.getTransient("ctx.foo"));
            assertEquals("1", threadContext.getHeader("default"));
        }

        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(new Integer(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
    }

    public void testStashAndMerge() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader("foo", "bar");
        threadContext.putTransient("ctx.foo", new Integer(1));
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(new Integer(1), threadContext.getTransient("ctx.foo"));
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
        assertEquals(new Integer(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
    }

    public void testStoreContext() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader("foo", "bar");
        threadContext.putTransient("ctx.foo", new Integer(1));
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(new Integer(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
        ThreadContext.StoredContext storedContext = threadContext.newStoredContext();
        threadContext.putHeader("foo.bar", "baz");
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            assertNull(threadContext.getHeader("foo"));
            assertNull(threadContext.getTransient("ctx.foo"));
            assertEquals("1", threadContext.getHeader("default"));
        }

        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(new Integer(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
        assertEquals("baz", threadContext.getHeader("foo.bar"));
        if (randomBoolean()) {
            storedContext.restore();
        } else {
            storedContext.close();
        }
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(new Integer(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
        assertNull(threadContext.getHeader("foo.bar"));
    }

    public void testCopyHeaders() {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.copyHeaders(Collections.<String,String>emptyMap().entrySet());
        threadContext.copyHeaders(Collections.<String,String>singletonMap("foo", "bar").entrySet());
        assertEquals("bar", threadContext.getHeader("foo"));
    }

    public void testAccessClosed() throws IOException {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader("foo", "bar");
        threadContext.putTransient("ctx.foo", new Integer(1));

        threadContext.close();
        try {
            threadContext.getHeader("foo");
            fail();
        } catch (IllegalStateException ise) {
            assertEquals("threadcontext is already closed", ise.getMessage());
        }

        try {
            threadContext.putTransient("foo", new Object());
            fail();
        } catch (IllegalStateException ise) {
            assertEquals("threadcontext is already closed", ise.getMessage());
        }

        try {
            threadContext.putHeader("boom", "boom");
            fail();
        } catch (IllegalStateException ise) {
            assertEquals("threadcontext is already closed", ise.getMessage());
        }
    }

    public void testSerialize() throws IOException {
        Settings build = Settings.builder().put("request.headers.default", "1").build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putHeader("foo", "bar");
        threadContext.putTransient("ctx.foo", new Integer(1));
        BytesStreamOutput out = new BytesStreamOutput();
        threadContext.writeTo(out);
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            assertNull(threadContext.getHeader("foo"));
            assertNull(threadContext.getTransient("ctx.foo"));
            assertEquals("1", threadContext.getHeader("default"));

            threadContext.readHeaders(StreamInput.wrap(out.bytes()));
            assertEquals("bar", threadContext.getHeader("foo"));
            assertNull(threadContext.getTransient("ctx.foo"));
        }
        assertEquals("bar", threadContext.getHeader("foo"));
        assertEquals(new Integer(1), threadContext.getTransient("ctx.foo"));
        assertEquals("1", threadContext.getHeader("default"));
    }

    public void testSerializeInDifferentContext() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        {
            Settings build = Settings.builder().put("request.headers.default", "1").build();
            ThreadContext threadContext = new ThreadContext(build);
            threadContext.putHeader("foo", "bar");
            threadContext.putTransient("ctx.foo", new Integer(1));

            assertEquals("bar", threadContext.getHeader("foo"));
            assertNotNull(threadContext.getTransient("ctx.foo"));
            assertEquals("1", threadContext.getHeader("default"));
            threadContext.writeTo(out);
        }
        {
            Settings otherSettings = Settings.builder().put("request.headers.default", "5").build();
            ThreadContext otherhreadContext = new ThreadContext(otherSettings);
            otherhreadContext.readHeaders(StreamInput.wrap(out.bytes()));

            assertEquals("bar", otherhreadContext.getHeader("foo"));
            assertNull(otherhreadContext.getTransient("ctx.foo"));
            assertEquals("1", otherhreadContext.getHeader("default"));
        }
    }
    
    public void testSerializeInDifferentContextNoDefaults() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        {
            ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
            threadContext.putHeader("foo", "bar");
            threadContext.putTransient("ctx.foo", new Integer(1));

            assertEquals("bar", threadContext.getHeader("foo"));
            assertNotNull(threadContext.getTransient("ctx.foo"));
            assertNull(threadContext.getHeader("default"));
            threadContext.writeTo(out);
        }
        {
            Settings otherSettings = Settings.builder().put("request.headers.default", "5").build();
            ThreadContext otherhreadContext = new ThreadContext(otherSettings);
            otherhreadContext.readHeaders(StreamInput.wrap(out.bytes()));

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
        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            Runnable withContext;

            // Create a runnable that should run with some header
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.putHeader("foo", "bar");
                withContext = threadContext.preserveContext(sometimesAbstractRunnable(() -> {
                    assertEquals("bar", threadContext.getHeader("foo"));
                }));
            }

            // We don't see the header outside of the runnable
            assertNull(threadContext.getHeader("foo"));

            // But we do inside of it
            withContext.run();
        }
    }

    public void testPreserveContextKeepsOriginalContextWhenCalledTwice() throws IOException {
        try (ThreadContext threadContext = new ThreadContext(Settings.EMPTY)) {
            Runnable originalWithContext;
            Runnable withContext;

            // Create a runnable that should run with some header
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.putHeader("foo", "bar");
                withContext = threadContext.preserveContext(sometimesAbstractRunnable(() -> {
                    assertEquals("bar", threadContext.getHeader("foo"));
                }));
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
            public void onFailure(Throwable t) {
                throw new RuntimeException(t);
            }

            @Override
            protected void doRun() throws Exception {
                r.run();
            }
        };
    }
}
