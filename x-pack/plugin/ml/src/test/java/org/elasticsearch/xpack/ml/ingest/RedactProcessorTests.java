/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.ingest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.results.NerResults;
import org.elasticsearch.xpack.core.ml.inference.results.NlpClassificationInferenceResults;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class RedactProcessorTests extends ESTestCase {

    public void testRedactEntities() {
        {
            String input = "My name is Clare and I work at Blackbird Bakery";
            var entities = List.of(
                new NerResults.EntityGroup("Clare", "PERSON", 0.9, 0, 0),
                new NerResults.EntityGroup("Blackbird Bakery", "ORG", 0.9, 0, 0)
            );

            var redacted = RedactProcessor.redactEntities(input, entities);
            assertEquals(redacted, "My name is <PERSON> and I work at <ORG>");
        }
        {
            String input = "Nothing here";
            var redacted = RedactProcessor.redactEntities(input, List.of());
            assertThat(redacted, sameInstance(input));
        }
    }

    public void testRedactGroks() throws Exception {
        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}", "%{IP:IP_ADDRESS}", "%{CREDIT_CARD:CREDIT_CARD}"));
        config.put("pattern_definitions", Map.of("CREDIT_CARD", "\\d{4}[ -]\\d{4}[ -]\\d{4}[ -]\\d{4}"));
        var processor = new RedactProcessor.Factory(mock(Client.class), MatcherWatchdog.noop()).create(null, "t", "d", config);
        var groks = processor.getGroks();

        {
            String input = "This is ok nothing to redact";
            var redacted = RedactProcessor.redactGroks(input, groks);
            assertThat(redacted, sameInstance(input));
        }
        {
            String input = "thisisanemail@address.com will be redacted";
            var redacted = RedactProcessor.redactGroks(input, groks);
            assertEquals("<EMAIL> will be redacted", redacted);
        }
        {
            String input = "here is something that looks like a cred card number: 0001-0002-0003-0004";
            var redacted = RedactProcessor.redactGroks(input, groks);
            assertEquals("here is something that looks like a cred card number: <CREDIT_CARD>", redacted);
        }
    }

    public void testNoNerModel() throws Exception {
        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("foo"));

        Client client = mock(Client.class);
        var processor = new RedactProcessor.Factory(client, MatcherWatchdog.noop()).create(null, "t", "d", config);

        var handlerCalled = new AtomicBoolean();
        // verify no calls to the infer action
        processor.execute(createIngestDoc(Map.of("to_redact", "fieldValue")), (doc, e) -> {
            verifyNoInteractions(client);
            handlerCalled.set(true);
        });
        assertTrue(handlerCalled.get());
    }

    public void testIgnoreMissing() throws Exception {
        Client client = mock(Client.class);
        {
            var handlerCalled = new AtomicBoolean();
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("foo"));
            var processor = new RedactProcessor.Factory(client, MatcherWatchdog.noop()).create(null, "t", "d", config);
            processor.execute(createIngestDoc(Map.of("not_the_field", "fieldValue")), (doc, e) -> { handlerCalled.set(true); });
            assertTrue(handlerCalled.get());
        }
        {
            var handlerCalled = new AtomicBoolean();
            var config = new HashMap<String, Object>();
            config.put("field", "to_redact");
            config.put("patterns", List.of("foo"));
            config.put("ignore_missing", false);   // this time the missing field should error

            var processor = new RedactProcessor.Factory(client, MatcherWatchdog.noop()).create(null, "t", "d", config);
            processor.execute(createIngestDoc(Map.of("not_the_field", "fieldValue")), (doc, e) -> {
                assertThat(e, instanceOf(IllegalArgumentException.class));
                assertThat(e.getMessage(), containsString("field [to_redact] is null or missing"));
                handlerCalled.set(true);
            });
            assertTrue(handlerCalled.get());
        }
    }

    @SuppressWarnings("unchecked")
    public void testWithNerModel() throws Exception {
        String modelId = "ner-model";
        String fieldName = "to_redact";
        Client client = mock(Client.class);
        var threadPool = new TestThreadPool("redact-processor-test");
        when(client.threadPool()).thenReturn(threadPool);

        String input = "My name is Clare and I work at Blackbird Bakery with the obvious email clare@blackbirdbakery.com";
        var entities = List.of(
            new NerResults.EntityGroup("Clare", "PERSON", 0.9, 0, 0),
            new NerResults.EntityGroup("Blackbird Bakery", "ORG", 0.9, 0, 0)
        );

        var response = new InferModelAction.Response(List.of(new NerResults("foo", "bar", entities, false)), "ner-model", true);

        doAnswer(invocationOnMock -> {
            ActionListener<InferModelAction.Response> listener = (ActionListener<InferModelAction.Response>) invocationOnMock
                .getArguments()[2];
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).execute(same(InferModelAction.INSTANCE), any(), any());

        var config = new HashMap<String, Object>();
        config.put("field", fieldName);
        config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}"));
        config.put("model_id", modelId);

        var processor = new RedactProcessor.Factory(client, MatcherWatchdog.noop()).create(null, "t", "d", config);

        var handlerCalled = new AtomicBoolean();
        processor.execute(createIngestDoc(Map.of(fieldName, input)), (doc, e) -> {
            assertNull(e);
            String redacted = doc.getFieldValue(fieldName, String.class, false);
            assertEquals("My name is <PERSON> and I work at <ORG> with the obvious email <EMAIL>", redacted);
            handlerCalled.set(true);
        });
        assertTrue(handlerCalled.get());

        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testNerModelOnly() throws Exception {
        String modelId = "ner-model";
        String fieldName = "to_redact";
        Client client = mock(Client.class);
        var threadPool = new TestThreadPool("redact-processor-test");
        when(client.threadPool()).thenReturn(threadPool);

        String input = "My name is Clare and I work at Blackbird Bakery with the obvious email clare@blackbirdbakery.com";
        var entities = List.of(
            new NerResults.EntityGroup("Clare", "PERSON", 0.9, 0, 0),
            new NerResults.EntityGroup("Blackbird Bakery", "ORG", 0.9, 0, 0)
        );

        var response = new InferModelAction.Response(List.of(new NerResults("foo", "bar", entities, false)), "ner-model", true);

        doAnswer(invocationOnMock -> {
            ActionListener<InferModelAction.Response> listener = (ActionListener<InferModelAction.Response>) invocationOnMock
                .getArguments()[2];
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).execute(same(InferModelAction.INSTANCE), any(), any());

        var config = new HashMap<String, Object>();
        config.put("field", fieldName);
        config.put("model_id", modelId);

        var processor = new RedactProcessor.Factory(client, MatcherWatchdog.noop()).create(null, "t", "d", config);

        var handlerCalled = new AtomicBoolean();
        processor.execute(createIngestDoc(Map.of(fieldName, input)), (doc, e) -> {
            assertNull(e);
            String redacted = doc.getFieldValue(fieldName, String.class, false);
            assertEquals("My name is <PERSON> and I work at <ORG> with the obvious email clare@blackbirdbakery.com", redacted);
            handlerCalled.set(true);
        });
        assertTrue(handlerCalled.get());

        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testWithNerModelFailure() throws Exception {
        Client client = mock(Client.class);
        var threadPool = new TestThreadPool("redact-processor-test");
        when(client.threadPool()).thenReturn(threadPool);

        var error = new ElasticsearchException("Some failure");
        doAnswer(invocationOnMock -> {
            ActionListener<InferModelAction.Response> listener = (ActionListener<InferModelAction.Response>) invocationOnMock
                .getArguments()[2];
            listener.onFailure(error);
            return Void.TYPE;
        }).when(client).execute(same(InferModelAction.INSTANCE), any(), any());

        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("model_id", "foo");
        config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}"));

        var processor = new RedactProcessor.Factory(client, MatcherWatchdog.noop()).create(null, "t", "d", config);

        String input = "Something blah";
        var handlerCalled = new AtomicBoolean();
        processor.execute(createIngestDoc(Map.of("to_redact", input)), (doc, e) -> {
            assertNotNull(e);
            handlerCalled.set(true);
        });
        assertTrue(handlerCalled.get());

        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testWithNotAnNerModel() throws Exception {
        Client client = mock(Client.class);
        var threadPool = new TestThreadPool("redact-processor-test");
        when(client.threadPool()).thenReturn(threadPool);

        doAnswer(invocationOnMock -> {
            ActionListener<InferModelAction.Response> listener = (ActionListener<InferModelAction.Response>) invocationOnMock
                .getArguments()[2];
            listener.onResponse(
                new InferModelAction.Response(
                    List.of(new NlpClassificationInferenceResults("class", List.of(), "f", 0.1, false)),
                    "foo",
                    true
                )
            );
            return Void.TYPE;
        }).when(client).execute(same(InferModelAction.INSTANCE), any(), any());

        var config = new HashMap<String, Object>();
        config.put("field", "to_redact");
        config.put("patterns", List.of("%{EMAILADDRESS:EMAIL}"));
        config.put("model_id", "foo");

        var processor = new RedactProcessor.Factory(client, MatcherWatchdog.noop()).create(null, "t", "d", config);

        String input = "Something blah";
        var handlerCalled = new AtomicBoolean();
        processor.execute(createIngestDoc(Map.of("to_redact", input)), (doc, e) -> {
            assertNotNull(e);
            handlerCalled.set(true);
        });
        assertTrue(handlerCalled.get());

        terminate(threadPool);
    }

    private IngestDocument createIngestDoc(Map<String, Object> source) {
        return new IngestDocument("index", "id", 0L, "routing", VersionType.INTERNAL, source);
    }
}
