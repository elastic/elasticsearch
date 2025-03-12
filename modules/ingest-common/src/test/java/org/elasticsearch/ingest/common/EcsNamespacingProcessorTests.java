/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("unchecked")
public class EcsNamespacingProcessorTests extends ESTestCase {

    /**
     * Test that the special key values are extracted correctly.
     * When ingesting a document as follows:
     * <pre>
     *  {
     *      "error": {
     *          "exception": {
     *              "type": "value1",
     *              "foo": "bar"
     *          },
     *          "exception.type": "value2",
     *          "foo": "bar"
     *      },
     *      "error.exception": {
     *          "type": "value3",
     *          "foo": "bar"
     *      },
     *      "error.exception.type": "value4"
     *  }
     *  </pre>
     *  The processor should properly resolve all paths that match the {@code error.exception.type} key and extract the values:
     *  <ul>
     *      <li>value1</li>
     *      <li>value2</li>
     *      <li>value3</li>
     *      <li>value4</li>
     *  </ul>
     * The processor should also remove all keys that have paths that are resolved to {@code error.exception.type}
     */
    public void testExtractAndRemoveAllKeyValues() {
        Map<String, Object> document = createErrorExceptionTypeDocument();

        List<Object> values = new ArrayList<>();
        EcsNamespacingProcessor processor = new EcsNamespacingProcessor(null, null);
        processor.extractAndRemoveAllKeyValues(document, "error.exception.type", 0, values);

        assertEquals(4, values.size());
        assertTrue(values.contains("value1"));
        assertTrue(values.contains("value2"));
        assertTrue(values.contains("value3"));
        assertTrue(values.contains("value4"));

        Object errorMap = document.get("error");
        assertTrue(errorMap instanceof Map);
        assertEquals("bar", ((Map<String, Object>) errorMap).get("foo"));

        Object exceptionMap = ((Map<String, Object>) errorMap).get("exception");
        assertTrue(exceptionMap instanceof Map);
        assertEquals("bar", ((Map<String, Object>) exceptionMap).get("foo"));

        Object errorExceptionMap = document.get("error.exception");
        assertTrue(errorExceptionMap instanceof Map);
        assertEquals("bar", ((Map<String, Object>) errorExceptionMap).get("foo"));

        assertFalse(((Map<String, Object>) errorMap).containsKey("exception.type"));
        assertFalse(((Map<String, Object>) exceptionMap).containsKey("type"));
        assertFalse(((Map<String, Object>) errorExceptionMap).containsKey("type"));
    }

    public void testNormalizeSpecialKeysThreadSafety() throws InterruptedException {
        int numThreads = 10;
        CountDownLatch latch = new CountDownLatch(numThreads);
        List<Throwable> errors = new ArrayList<>();
        EcsNamespacingProcessor processor = new EcsNamespacingProcessor(null, null);

        try (ExecutorService executorService = Executors.newFixedThreadPool(numThreads)) {
            for (int i = 0; i < numThreads; i++) {
                executorService.submit(() -> {
                    try {
                        Map<String, Object> document = createErrorExceptionTypeDocument();
                        processor.normalizeSpecialKeys(document);

                        // Verify the document structure and values
                        Object errorMap = document.get("error");
                        assertTrue("'error' object is null or not a map", errorMap instanceof Map);
                        assertEquals("'error.foo' is not set to 'bar'", "bar", ((Map<String, Object>) errorMap).get("foo"));

                        Object exceptionMap = ((Map<String, Object>) errorMap).get("exception");
                        assertTrue("'error.exception' object is null or not a map", exceptionMap instanceof Map);
                        assertEquals("'error.exception.foo' is not set to 'bar'", "bar", ((Map<String, Object>) exceptionMap).get("foo"));

                        Object errorExceptionMap = document.get("error.exception");
                        assertTrue("'error.exception' object is null or not a map", errorExceptionMap instanceof Map);
                        assertEquals(
                            "'error.exception.foo' is not set to 'bar'",
                            "bar",
                            ((Map<String, Object>) errorExceptionMap).get("foo")
                        );

                        Object values = document.get("error.exception.type");
                        assertNotNull("document doesn't contain 'error.exception.type' key", values);
                        assertTrue("'error.exception.type' is not a list", values instanceof List);
                        List<String> valuesList = (List<String>) values;
                        assertEquals("value list expected to have 4 values but had " + valuesList.size(), 4, valuesList.size());
                        assertTrue("values list doesn't contain 'value1'", valuesList.contains("value1"));
                        assertTrue("values list doesn't contain 'value2'", valuesList.contains("value2"));
                        assertTrue("values list doesn't contain 'value3'", valuesList.contains("value3"));
                        assertTrue("values list doesn't contain 'value4'", valuesList.contains("value4"));

                        assertFalse(
                            "'error' object contains 'exception.type' key",
                            ((Map<String, Object>) errorMap).containsKey("exception.type")
                        );
                        assertFalse(
                            "'error.exception' object contains 'type' key",
                            ((Map<String, Object>) exceptionMap).containsKey("type")
                        );
                        assertFalse(
                            "'error.exception' object contains 'type' key",
                            ((Map<String, Object>) errorExceptionMap).containsKey("type")
                        );
                    } catch (Throwable t) {
                        synchronized (errors) {
                            errors.add(t);
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await(1, TimeUnit.MINUTES);
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(1, TimeUnit.MINUTES));
        }

        if (errors.isEmpty() == false) {
            fail("Thread safety test failed with errors: " + errors.getFirst().getMessage());
        }
    }

    private static Map<String, Object> createErrorExceptionTypeDocument() {
        Map<String, Object> document = new HashMap<>();
        Map<String, Object> error = new HashMap<>();
        Map<String, Object> exception = new HashMap<>();
        exception.put("type", "value1");
        exception.put("foo", "bar");
        error.put("exception", exception);
        error.put("exception.type", "value2");
        error.put("foo", "bar");
        document.put("error", error);
        Map<String, Object> errorException = new HashMap<>();
        errorException.put("type", "value3");
        errorException.put("foo", "bar");
        document.put("error.exception", errorException);
        document.put("error.exception.type", "value4");
        return document;
    }
}
