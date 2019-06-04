/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class DeleteJobWithDataTests extends ESTestCase {
    public void testDeleteOnlyJob() throws IOException, InterruptedException {
        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME,
                Collections.singletonMap("_meta",
                    Collections.singletonMap(RollupField.ROLLUP_META,
                        Collections.singletonMap(jobName, job)))));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Mockito.when(meta.mapping()).thenReturn(mappingMeta);

        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.builder(1);
        indices.put("foo", meta);

        ThreadPool threadpool = new TestThreadPool(getTestName());
        Client client = new NoOpClient(threadpool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
                PutMappingRequest putMappingRequest = (PutMappingRequest) request;
                assertThat(putMappingRequest.source(), equalTo("{\"_meta\":{\"_rollup\":{}}}"));
                assertThat(((PutMappingRequest) request).indices(), equalTo(new String[]{job.getRollupIndex()}));

                listener.onResponse((Response) new AcknowledgedResponse(true));
            }
        };

        try {
            CountDownLatch latch = new CountDownLatch(1);
            TransportDeleteRollupJobAction.updateMetadata(job.getId(), indices.build(), client, new ActionListener<>() {
                @Override
                public void onResponse(DeleteRollupJobAction.Response response) {
                    assertTrue(response.isDeleted());
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e.getMessage());
                }
            });
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } finally {
            client.close();
            terminate(threadpool);
        }
    }

    public void testDeleteOneOfTwoJobs() throws IOException, InterruptedException {
        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);
        String jobName2 = randomAlphaOfLength(5);
        RollupJobConfig job2 = ConfigTestHelpers.randomRollupJobConfig(random(), jobName2);

        Map<String, RollupJobConfig> jobs = new HashMap<>(2);
        jobs.put(jobName, job);
        jobs.put(jobName2, job2);

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME,
                Collections.singletonMap("_meta",
                    Collections.singletonMap(RollupField.ROLLUP_META, jobs))));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(2);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Mockito.when(meta.mapping()).thenReturn(mappingMeta);

        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.builder(1);
        indices.put("foo", meta);

        ThreadPool threadpool = new TestThreadPool(getTestName());
        Client client = new NoOpClient(threadpool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
                PutMappingRequest putMappingRequest = (PutMappingRequest) request;
                Map<String, Object> source = entityAsMap(putMappingRequest.source());

                //This job should be removed
                assertNull(ObjectPath.eval("_meta._rollup." + jobName, source));

                //This job should be present
                Map<String, Object> job2Values = ObjectPath.eval("_meta._rollup." + jobName2, source);
                assertNotNull(job2Values);
                assertThat(job2Values.get("rollup_index"), equalTo(job2.getRollupIndex()));
                assertThat(job2Values.get("index_pattern"), equalTo(job2.getIndexPattern()));

                assertThat(((PutMappingRequest) request).indices(), equalTo(new String[]{job.getRollupIndex()}));
                listener.onResponse((Response) new AcknowledgedResponse(true));
            }
        };

        try {
            CountDownLatch latch = new CountDownLatch(1);
            TransportDeleteRollupJobAction.updateMetadata(job.getId(), indices.build(), client, new ActionListener<>() {
                @Override
                public void onResponse(DeleteRollupJobAction.Response response) {
                    assertTrue(response.isDeleted());
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e.getMessage());
                }
            });
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } finally {
            client.close();
            terminate(threadpool);
        }
    }

    /**
     * This test deletes one of two jobs, and checks to make sure unrelated _meta is preserved, as well
     * as extra mappings
     */
    public void testDeleteWithMappingAndUnrelatedMeta() throws IOException, InterruptedException {
        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);
        String jobName2 = randomAlphaOfLength(5);
        RollupJobConfig job2 = ConfigTestHelpers.randomRollupJobConfig(random(), jobName2);

        Map<String, RollupJobConfig> jobs = new HashMap<>(2);
        jobs.put(jobName, job);
        jobs.put(jobName2, job2);

        Map<String, Object> metaMap = new HashMap<>(2);
        metaMap.put(RollupField.ROLLUP_META, jobs);
        metaMap.put("foo", Collections.singletonMap("bar", "bazz"));

        Map<String, Object> mappingMap = new HashMap<>(2);
        mappingMap.put("_meta", metaMap);
        mappingMap.put("properties", Collections.singletonMap("foo", Collections.singletonMap("type", "long")));

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME, mappingMap));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(2);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Mockito.when(meta.mapping()).thenReturn(mappingMeta);

        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.builder(1);
        indices.put("foo", meta);

        ThreadPool threadpool = new TestThreadPool(getTestName());
        Client client = new NoOpClient(threadpool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
                PutMappingRequest putMappingRequest = (PutMappingRequest) request;
                Map<String, Object> source = entityAsMap(putMappingRequest.source());

                //This job should be removed
                assertNull(ObjectPath.eval("_meta._rollup." + jobName, source));

                //This job should be present
                Map<String, Object> job2Values = ObjectPath.eval("_meta._rollup." + jobName2, source);
                assertNotNull(job2Values);
                assertThat(job2Values.get("rollup_index"), equalTo(job2.getRollupIndex()));
                assertThat(job2Values.get("index_pattern"), equalTo(job2.getIndexPattern()));

                // And this unrelated _meta should be preserved
                Map<String, Object> fooMap = ObjectPath.eval("_meta.foo", source);
                assertNotNull(fooMap);
                assertThat(fooMap.get("bar"), equalTo("bazz"));

                // And the mappings should be untouched
                String fooType = ObjectPath.eval("properties.foo.type", source);
                assertNotNull(fooType);
                assertThat(fooType, equalTo("long"));

                assertThat(((PutMappingRequest) request).indices(), equalTo(new String[]{job.getRollupIndex()}));
                listener.onResponse((Response) new AcknowledgedResponse(true));
            }
        };

        try {
            CountDownLatch latch = new CountDownLatch(1);
            TransportDeleteRollupJobAction.updateMetadata(job.getId(), indices.build(), client, new ActionListener<>() {
                @Override
                public void onResponse(DeleteRollupJobAction.Response response) {
                    assertTrue(response.isDeleted());
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e.getMessage());
                }
            });
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } finally {
            client.close();
            terminate(threadpool);
        }
    }

    public void testPutMappingNotAcknowledged() throws IOException, InterruptedException {
        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME,
                Collections.singletonMap("_meta",
                    Collections.singletonMap(RollupField.ROLLUP_META,
                        Collections.singletonMap(jobName, job)))));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Mockito.when(meta.mapping()).thenReturn(mappingMeta);

        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.builder(1);
        indices.put("foo", meta);

        ThreadPool threadpool = new TestThreadPool(getTestName());
        Client client = new NoOpClient(threadpool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
                listener.onResponse((Response) new AcknowledgedResponse(false));
            }
        };

        try {
            CountDownLatch latch = new CountDownLatch(1);
            TransportDeleteRollupJobAction.updateMetadata(job.getId(), indices.build(), client, new ActionListener<>() {
                @Override
                public void onResponse(DeleteRollupJobAction.Response response) {
                    fail("test should have triggered onFailure()");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e.getMessage(), equalTo("Attempt to remove job from _meta config was not acknowledged"));
                    latch.countDown();
                }
            });
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } finally {
            client.close();
            terminate(threadpool);
        }
    }

    public void testPutMappingException() throws IOException, InterruptedException {
        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME,
                Collections.singletonMap("_meta",
                    Collections.singletonMap(RollupField.ROLLUP_META,
                        Collections.singletonMap(jobName, job)))));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Mockito.when(meta.mapping()).thenReturn(mappingMeta);

        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.builder(1);
        indices.put("foo", meta);

        ThreadPool threadpool = new TestThreadPool(getTestName());
        Client client = new NoOpClient(threadpool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
                listener.onFailure(new RuntimeException("foo"));
            }
        };

        try {
            CountDownLatch latch = new CountDownLatch(1);
            TransportDeleteRollupJobAction.updateMetadata(job.getId(), indices.build(), client, new ActionListener<>() {
                @Override
                public void onResponse(DeleteRollupJobAction.Response response) {
                    fail("test should have triggered onFailure()");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e.getMessage(), equalTo("foo"));
                    latch.countDown();
                }
            });
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } finally {
            client.close();
            terminate(threadpool);
        }
    }

    public void testNoMatchingJob() throws IOException, InterruptedException {
        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME,
                Collections.singletonMap("_meta",
                    Collections.singletonMap(RollupField.ROLLUP_META,
                        Collections.singletonMap(jobName, job)))));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Mockito.when(meta.mapping()).thenReturn(mappingMeta);

        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.builder(1);
        indices.put("foo", meta);

        ThreadPool threadpool = new TestThreadPool(getTestName());
        Client client = new NoOpClient(threadpool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
                fail("Test should not reach client execution");
            }
        };

        try {
            IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> TransportDeleteRollupJobAction.updateMetadata(job.getId() + "_does_not_match",
                    indices.build(),
                    client,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(DeleteRollupJobAction.Response response) {
                            fail("test should not have triggered onResponse()");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            fail("test should not have triggered onFailure()");
                        }
                    }));
            assertThat(e.getMessage(),
                equalTo("Could not find config for job [" + job.getId() + "_does_not_match] in metadata when deleting."));

        } finally {
            client.close();
            terminate(threadpool);
        }
    }

    public void testWrapWithDelete() throws IOException, InterruptedException {
        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        MappingMetaData mappingMeta = new MappingMetaData(RollupField.TYPE_NAME,
            Collections.singletonMap(RollupField.TYPE_NAME,
                Collections.singletonMap("_meta",
                    Collections.singletonMap(RollupField.ROLLUP_META,
                        Collections.singletonMap(jobName, job)))));

        ImmutableOpenMap.Builder<String, MappingMetaData> mappings = ImmutableOpenMap.builder(1);
        mappings.put(RollupField.TYPE_NAME, mappingMeta);
        IndexMetaData meta = Mockito.mock(IndexMetaData.class);
        Mockito.when(meta.getMappings()).thenReturn(mappings.build());
        Mockito.when(meta.mapping()).thenReturn(mappingMeta);

        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.builder(1);
        indices.put("foo", meta);
        ImmutableOpenMap<String, IndexMetaData> indicesMap = indices.build();

        MetaData md = Mockito.mock(MetaData.class);
        Mockito.when(md.getIndices()).thenReturn(indicesMap);

        ClusterState cs = Mockito.mock(ClusterState.class);
        Mockito.when(cs.getMetaData()).thenReturn(md);

        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(cs);

        ThreadPool threadpool = new TestThreadPool(getTestName());
        Client client = new NoOpClient(threadpool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
                PutMappingRequest putMappingRequest = (PutMappingRequest) request;
                assertThat(putMappingRequest.source(), equalTo("{\"_meta\":{\"_rollup\":{}}}"));
                assertThat(((PutMappingRequest) request).indices(), equalTo(new String[]{job.getRollupIndex()}));

                listener.onResponse((Response) new AcknowledgedResponse(true));
            }
        };
        DeleteRollupJobAction.Request request = new DeleteRollupJobAction.Request(job.getId(), true);

        try {
            CountDownLatch latch = new CountDownLatch(1);
            ActionListener<DeleteRollupJobAction.Response> listener
                = TransportDeleteRollupJobAction.wrapListener(request, new ActionListener<>() {
                    @Override
                    public void onResponse(DeleteRollupJobAction.Response response) {
                        assertTrue(response.isDeleted());
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail(e.getMessage());
                    }
            }, clusterService, client);

            listener.onResponse(new DeleteRollupJobAction.Response(true));
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } finally {
            client.close();
            terminate(threadpool);
        }
    }

    public void testDBQ() throws InterruptedException {
        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        ThreadPool threadpool = new TestThreadPool(getTestName());
        Client client = new NoOpClient(threadpool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
                DeleteByQueryRequest dbq = (DeleteByQueryRequest) request;
                assertThat(dbq.getSearchRequest().source().toString(),
                    equalTo("{\"size\":1000,\"query\":{\"bool\":{\"filter\":[{\"term\":{\"_rollup.id\":{\"value\":\"" + job.getId()
                        + "\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"_source\":false}"));
                assertThat(dbq.indices(), equalTo(new String[]{job.getRollupIndex()}));

                listener.onResponse((Response) new BulkByScrollResponse(new TimeValue(1000),
                    new BulkByScrollTask.Status(1, 1, 1, 1, 1, 1, 0, 0, 0, 0, new TimeValue(0), 1, null, new TimeValue(0)), // phew!
                    Collections.emptyList(),
                    Collections.emptyList(),
                    false)
                );
            }
        };

        try {
            CountDownLatch latch = new CountDownLatch(1);
            ActionListener<DeleteRollupJobAction.Response> listener = new ActionListener<>() {
                @Override
                public void onResponse(DeleteRollupJobAction.Response response) {
                    assertTrue(response.isDeleted());
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e.getMessage());
                }
            };

            TransportDeleteRollupJobAction.deleteDataFromIndex(job, listener, client);
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } finally {
            client.close();
            terminate(threadpool);
        }
    }

    public void testDBQTimedOut() throws InterruptedException {
        String jobName = randomAlphaOfLength(5);
        RollupJobConfig job = ConfigTestHelpers.randomRollupJobConfig(random(), jobName);

        ThreadPool threadpool = new TestThreadPool(getTestName());
        Client client = new NoOpClient(threadpool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(Action<Response> action, Request request, ActionListener<Response> listener) {
                DeleteByQueryRequest dbq = (DeleteByQueryRequest) request;
                assertThat(dbq.getSearchRequest().source().toString(),
                    equalTo("{\"size\":1000,\"query\":{\"bool\":{\"filter\":[{\"term\":{\"_rollup.id\":{\"value\":\"" + job.getId()
                        + "\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}},\"_source\":false}"));
                assertThat(dbq.indices(), equalTo(new String[]{job.getRollupIndex()}));

                listener.onResponse((Response) new BulkByScrollResponse(new TimeValue(1000),
                    new BulkByScrollTask.Status(1, 1, 1, 1, 1, 1, 0, 0, 0, 0, new TimeValue(0), 1, null, new TimeValue(0)),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    true) // <-- timed out
                );
            }
        };

        try {
            CountDownLatch latch = new CountDownLatch(1);
            ActionListener<DeleteRollupJobAction.Response> listener = new ActionListener<>() {
                @Override
                public void onResponse(DeleteRollupJobAction.Response response) {
                    fail("Should not receive response in this test");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e.getMessage(), equalTo("DeleteByQuery on index [" + job.getRollupIndex()
                        + "] for job [" + job.getId() + "] timed out after 1000ms"));
                    latch.countDown();
                }
            };

            TransportDeleteRollupJobAction.deleteDataFromIndex(job, listener, client);
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } finally {
            client.close();
            terminate(threadpool);
        }
    }

    public static Map<String, Object> entityAsMap(String response) {
        XContentType xContentType = XContentType.JSON;
        // EMPTY and THROW are fine here because `.map` doesn't use named x content or deprecation
        try (XContentParser parser = xContentType.xContent().createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, response)) {
            return parser.map();
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }
}
