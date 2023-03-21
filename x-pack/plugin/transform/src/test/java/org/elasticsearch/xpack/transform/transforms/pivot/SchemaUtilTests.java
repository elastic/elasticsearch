/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class SchemaUtilTests extends ESTestCase {

    public void testInsertNestedObjectMappings() {
        Map<String, String> fieldMappings = new HashMap<>() {
            {
                // creates: a.b, a
                put("a.b.c", "long");
                put("a.b.d", "double");
                // creates: c.b, c
                put("c.b.a", "double");
                // creates: c.d
                put("c.d.e", "object");
                put("d", "long");
                put("e.f.g", "long");
                // cc: already there
                put("e.f", "object");
                // cc: already there but different type (should not be possible)
                put("e", "long");
                // cc: start with . (should not be possible)
                put(".x", "long");
                // cc: start and ends with . (should not be possible), creates: .y
                put(".y.", "long");
                // cc: ends with . (should not be possible), creates: .z
                put(".z.", "long");
            }
        };

        SchemaUtil.insertNestedObjectMappings(fieldMappings);

        assertEquals(18, fieldMappings.size());
        assertEquals("long", fieldMappings.get("a.b.c"));
        assertEquals("object", fieldMappings.get("a.b"));
        assertEquals("double", fieldMappings.get("a.b.d"));
        assertEquals("object", fieldMappings.get("a"));
        assertEquals("object", fieldMappings.get("c.d"));
        assertEquals("object", fieldMappings.get("e.f"));
        assertEquals("long", fieldMappings.get("e"));
        assertEquals("object", fieldMappings.get(".y"));
        assertEquals("object", fieldMappings.get(".z"));
        assertFalse(fieldMappings.containsKey("."));
        assertFalse(fieldMappings.containsKey(""));
    }

    public void testConvertToIntegerTypeIfNeeded() {
        assertEquals(33L, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("unsigned_long", 33.0));
        assertEquals(33L, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("long", 33.0));
        assertEquals(33.0, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("double", 33.0));
        assertEquals(33.0, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("half_float", 33.0));
        assertEquals(33.0, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("unknown", 33.0));
        assertEquals(33.0, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt(null, 33.0));

        Object value = SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("unsigned_long", 1.8446744073709551615E19);
        assertThat(value, instanceOf(BigInteger.class));

        assertEquals(new BigInteger("18446744073709551615").doubleValue(), ((BigInteger) value).doubleValue(), 0.0);
    }

    public void testGetSourceFieldMappings() throws InterruptedException {
        try (Client client = new FieldCapsMockClient(getTestName())) {
            // fields is null
            this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.getSourceFieldMappings(
                    client,
                    emptyMap(),
                    new String[] { "index-1", "index-2" },
                    null,
                    emptyMap(),
                    listener
                ),
                mappings -> {
                    assertNotNull(mappings);
                    assertTrue(mappings.isEmpty());
                }
            );

            // fields is empty
            this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.getSourceFieldMappings(
                    client,
                    emptyMap(),
                    new String[] { "index-1", "index-2" },
                    new String[] {},
                    emptyMap(),
                    listener
                ),
                mappings -> {
                    assertNotNull(mappings);
                    assertTrue(mappings.isEmpty());
                }
            );

            // indices is null
            this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.getSourceFieldMappings(
                    client,
                    emptyMap(),
                    null,
                    new String[] { "field-1", "field-2" },
                    emptyMap(),
                    listener
                ),
                mappings -> {
                    assertNotNull(mappings);
                    assertTrue(mappings.isEmpty());
                }
            );

            // indices is empty
            this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.getSourceFieldMappings(
                    client,
                    emptyMap(),
                    new String[] {},
                    new String[] { "field-1", "field-2" },
                    emptyMap(),
                    listener
                ),
                mappings -> {
                    assertNotNull(mappings);
                    assertTrue(mappings.isEmpty());
                }
            );

            // good use
            this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.getSourceFieldMappings(
                    client,
                    emptyMap(),
                    new String[] { "index-1", "index-2" },
                    new String[] { "field-1", "field-2" },
                    emptyMap(),
                    listener
                ),
                mappings -> {
                    assertNotNull(mappings);
                    assertEquals(2, mappings.size());
                    assertEquals("long", mappings.get("field-1"));
                    assertEquals("long", mappings.get("field-2"));
                }
            );
        }
    }

    public void testGetSourceFieldMappingsWithRuntimeMappings() throws InterruptedException {
        Map<String, Object> runtimeMappings = new HashMap<>() {
            {
                put("field-2", singletonMap("type", "keyword"));
                put("field-3", singletonMap("type", "boolean"));
            }
        };
        try (Client client = new FieldCapsMockClient(getTestName())) {
            this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.getSourceFieldMappings(
                    client,
                    emptyMap(),
                    new String[] { "index-1", "index-2" },
                    new String[] { "field-1", "field-2" },
                    runtimeMappings,
                    listener
                ),
                mappings -> {
                    assertThat(mappings, is(aMapWithSize(3)));
                    assertThat(
                        mappings,
                        allOf(hasEntry("field-1", "long"), hasEntry("field-2", "keyword"), hasEntry("field-3", "boolean"))
                    );
                }
            );
        }
    }

    private static class FieldCapsMockClient extends NoOpClient {
        FieldCapsMockClient(String testName) {
            super(testName);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof FieldCapabilitiesRequest fieldCapsRequest) {
                Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
                for (String field : fieldCapsRequest.fields()) {
                    responseMap.put(field, singletonMap(field, createFieldCapabilities(field, "long")));
                }
                for (Map.Entry<String, Object> runtimeField : fieldCapsRequest.runtimeFields().entrySet()) {
                    String field = runtimeField.getKey();
                    String type = (String) ((Map) runtimeField.getValue()).get("type");
                    responseMap.put(field, singletonMap(field, createFieldCapabilities(field, type)));
                }

                final FieldCapabilitiesResponse response = new FieldCapabilitiesResponse(fieldCapsRequest.indices(), responseMap);
                listener.onResponse((Response) response);
                return;
            }

            super.doExecute(action, request, listener);
        }
    }

    private static FieldCapabilities createFieldCapabilities(String name, String type) {
        return new FieldCapabilities(
            name,
            type,
            false,
            true,
            true,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Collections.emptyMap()
        );
    }

    private <T> void assertAsync(Consumer<ActionListener<T>> function, Consumer<T> furtherTests) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            furtherTests.accept(r);
        }, e -> { fail("got unexpected exception: " + e); }), latch);

        function.accept(listener);
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }

}
