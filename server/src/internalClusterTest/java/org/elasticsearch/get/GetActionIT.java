/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.get;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static java.util.Collections.singleton;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class GetActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class, SearcherWrapperPlugin.class);
    }

    public static class SearcherWrapperPlugin extends Plugin {
        public static boolean enabled = false;
        public static final AtomicInteger calls = new AtomicInteger();

        @Override
        public void onIndexModule(IndexModule indexModule) {
            super.onIndexModule(indexModule);
            if (enabled) {
                indexModule.setReaderWrapper(indexService -> {
                    CheckedFunction<DirectoryReader, DirectoryReader, IOException> wrapper = EngineTestCase.randomReaderWrapper();
                    return reader -> {
                        calls.incrementAndGet();
                        return wrapper.apply(reader);
                    };
                });
            }
        }
    }

    @Before
    public void maybeEnableSearcherWrapper() {
        SearcherWrapperPlugin.enabled = randomBoolean();
        SearcherWrapperPlugin.calls.set(0);
    }

    public void testSimpleGet() {
        assertAcked(
            prepareCreate("test").setMapping("field1", "type=keyword,store=true", "field2", "type=keyword,store=true")
                .setSettings(Settings.builder().put("index.refresh_interval", -1))
                .addAlias(new Alias("alias").writeIndex(randomFrom(true, false, null)))
        );
        ensureGreen();

        final Function<UnaryOperator<GetRequestBuilder>, GetResponse> docGetter = op -> getDocument(indexOrAlias(), "1", op);

        GetResponse response = docGetter.apply(UnaryOperator.identity());
        assertThat(response.isExists(), equalTo(false));

        logger.info("--> index doc 1");
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").get();

        logger.info("--> non realtime get 1");
        response = docGetter.apply(r -> r.setRealtime(false));
        assertThat(response.isExists(), equalTo(false));

        logger.info("--> realtime get 1");
        response = docGetter.apply(UnaryOperator.identity());
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime get 1 (no source, implicit)");
        response = docGetter.apply(r -> r.setStoredFields(Strings.EMPTY_ARRAY));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        Set<String> fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(Collections.<String>emptySet()));
        assertThat(response.getSourceAsBytesRef(), nullValue());

        logger.info("--> realtime get 1 (no source, explicit)");
        response = docGetter.apply(r -> r.setFetchSource(false));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(Collections.<String>emptySet()));
        assertThat(response.getSourceAsBytesRef(), nullValue());

        logger.info("--> realtime get 1 (no type)");
        response = docGetter.apply(UnaryOperator.identity());
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime fetch of field");
        response = docGetter.apply(r -> r.setStoredFields("field1"));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsBytesRef(), nullValue());
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> realtime fetch of field & source");
        response = docGetter.apply(r -> r.setStoredFields("field1").setFetchSource("field1", null));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap(), hasKey("field1"));
        assertThat(response.getSourceAsMap(), not(hasKey("field2")));
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> realtime get 1");
        response = docGetter.apply(UnaryOperator.identity());
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> refresh the index, so we load it from it");
        refresh();

        logger.info("--> non realtime get 1 (loaded from index)");
        response = docGetter.apply(r -> r.setRealtime(false));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime fetch of field (loaded from index)");
        response = docGetter.apply(r -> r.setStoredFields("field1"));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsBytesRef(), nullValue());
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> realtime fetch of field & source (loaded from index)");
        response = docGetter.apply(r -> r.setStoredFields("field1").setFetchSource(true));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsBytesRef(), not(nullValue()));
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> update doc 1");
        prepareIndex("test").setId("1").setSource("field1", "value1_1", "field2", "value2_1").get();

        logger.info("--> realtime get 1");
        response = docGetter.apply(UnaryOperator.identity());
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1_1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2_1"));

        logger.info("--> update doc 1 again");
        prepareIndex("test").setId("1").setSource("field1", "value1_2", "field2", "value2_2").get();

        response = docGetter.apply(UnaryOperator.identity());
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1_2"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2_2"));

        DeleteResponse deleteResponse = client().prepareDelete("test", "1").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());

        response = docGetter.apply(UnaryOperator.identity());
        assertThat(response.isExists(), equalTo(false));
    }

    public void testGetWithAliasPointingToMultipleIndices() {
        indicesAdmin().prepareCreate("index1").addAlias(new Alias("alias1").indexRouting("0")).get();
        if (randomBoolean()) {
            indicesAdmin().prepareCreate("index2")
                .addAlias(new Alias("alias1").indexRouting("0").writeIndex(randomFrom(false, null)))
                .get();
        } else {
            indicesAdmin().prepareCreate("index3").addAlias(new Alias("alias1").indexRouting("1").writeIndex(true)).get();
        }
        DocWriteResponse indexResponse = prepareIndex("index1").setId("id").setSource(Collections.singletonMap("foo", "bar")).get();
        assertThat(indexResponse.status().getStatus(), equalTo(RestStatus.CREATED.getStatus()));

        assertThat(
            asInstanceOf(IllegalArgumentException.class, getDocumentFailure("alias1", "_alias_id", r -> r)).getMessage(),
            endsWith("can't execute a single index op")
        );
    }

    static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }

    private static GetResponse getDocument(String index, String id, UnaryOperator<GetRequestBuilder> requestOperator) {
        return safeAwait(l -> getDocumentAsync(index, id, requestOperator, l));
    }

    private static Throwable getDocumentFailure(String index, String id, UnaryOperator<GetRequestBuilder> requestOperator) {
        return ExceptionsHelper.unwrapCause(safeAwaitFailure(GetResponse.class, l -> getDocumentAsync(index, id, requestOperator, l)));
    }

    private static void getDocumentAsync(
        String index,
        String id,
        UnaryOperator<GetRequestBuilder> requestOperator,
        ActionListener<GetResponse> listener
    ) {
        requestOperator.apply(client().prepareGet(index, id))
            .execute(ActionListener.runBefore(listener, () -> ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GET)));
    }

    public void testSimpleMultiGet() throws Exception {
        assertAcked(
            prepareCreate("test").addAlias(new Alias("alias").writeIndex(randomFrom(true, false, null)))
                .setMapping("field", "type=keyword,store=true")
                .setSettings(Settings.builder().put("index.refresh_interval", -1))
        );
        ensureGreen();

        MultiGetResponse response = client().prepareMultiGet().add(indexOrAlias(), "1").get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(false));

        for (int i = 0; i < 10; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }

        response = client().prepareMultiGet()
            .add(indexOrAlias(), "1")
            .add(indexOrAlias(), "15")
            .add(indexOrAlias(), "3")
            .add(indexOrAlias(), "9")
            .add(indexOrAlias(), "11")
            .get();
        assertThat(response.getResponses().length, equalTo(5));
        assertThat(response.getResponses()[0].getId(), equalTo("1"));
        assertThat(response.getResponses()[0].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[1].getId(), equalTo("15"));
        assertThat(response.getResponses()[1].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getResponse().getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getResponse().isExists(), equalTo(false));
        assertThat(response.getResponses()[2].getId(), equalTo("3"));
        assertThat(response.getResponses()[2].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[2].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[3].getId(), equalTo("9"));
        assertThat(response.getResponses()[3].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[3].getResponse().getIndex(), equalTo("test"));
        assertThat(response.getResponses()[3].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[4].getId(), equalTo("11"));
        assertThat(response.getResponses()[4].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[4].getResponse().getIndex(), equalTo("test"));
        assertThat(response.getResponses()[4].getResponse().isExists(), equalTo(false));

        // multi get with specific field
        response = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").storedFields("field"))
            .add(new MultiGetRequest.Item(indexOrAlias(), "3").storedFields("field"))
            .get();

        assertThat(response.getResponses().length, equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getSourceAsBytesRef(), nullValue());
        assertThat(response.getResponses()[0].getResponse().getField("field").getValues().get(0).toString(), equalTo("value1"));
    }

    public void testGetDocWithMultivaluedFields() throws Exception {
        String mapping1 = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("field")
                .field("type", "text")
                .field("store", true)
                .endObject()
                .endObject()
                .endObject()
        );
        assertAcked(prepareCreate("test").setMapping(mapping1));
        ensureGreen();

        final Function<UnaryOperator<GetRequestBuilder>, GetResponse> docGetter = op -> getDocument("test", "1", op);

        GetResponse response = docGetter.apply(UnaryOperator.identity());
        assertThat(response.isExists(), equalTo(false));

        prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().array("field", "1", "2").endObject()).get();

        response = docGetter.apply(r -> r.setStoredFields("field"));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        Set<String> fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(singleton("field")));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));

        // Now test values being fetched from stored fields.
        refresh();
        response = docGetter.apply(r -> r.setStoredFields("field"));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(singleton("field")));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));
    }

    public void testGetWithVersion() {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSettings(Settings.builder().put("index.refresh_interval", -1)));
        ensureGreen();

        final Function<UnaryOperator<GetRequestBuilder>, GetResponse> docGetter = op -> getDocument(indexOrAlias(), "1", op);

        GetResponse response = docGetter.apply(UnaryOperator.identity());
        assertThat(response.isExists(), equalTo(false));

        logger.info("--> index doc 1");
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").get();

        // From translog:

        response = docGetter.apply(r -> r.setVersion(Versions.MATCH_ANY));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1L));

        response = docGetter.apply(r -> r.setVersion(1));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1L));

        assertThat(getDocumentFailure(indexOrAlias(), "1", r -> r.setVersion(2)), instanceOf(VersionConflictEngineException.class));

        // From Lucene index:
        refresh();

        response = docGetter.apply(r -> r.setVersion(Versions.MATCH_ANY).setRealtime(false));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(1L));

        response = docGetter.apply(r -> r.setVersion(1).setRealtime(false));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(1L));

        assertThat(
            getDocumentFailure(indexOrAlias(), "1", r -> r.setVersion(2).setRealtime(false)),
            instanceOf(VersionConflictEngineException.class)
        );

        logger.info("--> index doc 1 again, so increasing the version");
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").get();

        // From translog:

        response = docGetter.apply(r -> r.setVersion(Versions.MATCH_ANY));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));

        assertThat(getDocumentFailure(indexOrAlias(), "1", r -> r.setVersion(1)), instanceOf(VersionConflictEngineException.class));

        response = docGetter.apply(r -> r.setVersion(2));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));

        // From Lucene index:
        refresh();

        response = docGetter.apply(r -> r.setVersion(Versions.MATCH_ANY).setRealtime(false));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));

        assertThat(
            getDocumentFailure(indexOrAlias(), "1", r -> r.setVersion(1).setRealtime(false)),
            instanceOf(VersionConflictEngineException.class)
        );

        response = docGetter.apply(r -> r.setVersion(2).setRealtime(false));
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));
    }

    public void testMultiGetWithVersion() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSettings(Settings.builder().put("index.refresh_interval", -1)));
        ensureGreen();

        MultiGetResponse response = client().prepareMultiGet().add(indexOrAlias(), "1").get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(false));

        for (int i = 0; i < 3; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }

        // Version from translog
        response = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(Versions.MATCH_ANY))
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(1))
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(2))
            .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("1"));
        assertThat(response.getResponses()[0].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[1].getId(), equalTo("1"));
        assertThat(response.getResponses()[1].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getFailure(), nullValue());
        assertThat(response.getResponses()[1].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[1].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[2].getFailure(), notNullValue());
        assertThat(response.getResponses()[2].getFailure().getId(), equalTo("1"));
        assertThat(response.getResponses()[2].getFailure().getMessage(), startsWith("[1]: version conflict"));
        assertThat(response.getResponses()[2].getFailure().getFailure(), instanceOf(VersionConflictEngineException.class));

        // Version from Lucene index
        refresh();
        response = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(Versions.MATCH_ANY))
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(1))
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(2))
            .setRealtime(false)
            .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("1"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[1].getId(), equalTo("1"));
        assertThat(response.getResponses()[1].getFailure(), nullValue());
        assertThat(response.getResponses()[1].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[1].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[2].getFailure(), notNullValue());
        assertThat(response.getResponses()[2].getFailure().getId(), equalTo("1"));
        assertThat(response.getResponses()[2].getFailure().getMessage(), startsWith("[1]: version conflict"));
        assertThat(response.getResponses()[2].getFailure().getFailure(), instanceOf(VersionConflictEngineException.class));

        for (int i = 0; i < 3; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }

        // Version from translog
        response = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(Versions.MATCH_ANY))
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(1))
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(2))
            .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("2"));
        assertThat(response.getResponses()[0].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));
        assertThat(response.getResponses()[1].getFailure(), notNullValue());
        assertThat(response.getResponses()[1].getFailure().getId(), equalTo("2"));
        assertThat(response.getResponses()[1].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getFailure().getMessage(), startsWith("[2]: version conflict"));
        assertThat(response.getResponses()[2].getId(), equalTo("2"));
        assertThat(response.getResponses()[2].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[2].getFailure(), nullValue());
        assertThat(response.getResponses()[2].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[2].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));

        // Version from Lucene index
        refresh();
        response = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(Versions.MATCH_ANY))
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(1))
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(2))
            .setRealtime(false)
            .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("2"));
        assertThat(response.getResponses()[0].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));
        assertThat(response.getResponses()[1].getFailure(), notNullValue());
        assertThat(response.getResponses()[1].getFailure().getId(), equalTo("2"));
        assertThat(response.getResponses()[1].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getFailure().getMessage(), startsWith("[2]: version conflict"));
        assertThat(response.getResponses()[2].getId(), equalTo("2"));
        assertThat(response.getResponses()[2].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[2].getFailure(), nullValue());
        assertThat(response.getResponses()[2].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[2].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));
    }

    public void testGetFieldsNonLeafField() throws Exception {
        assertAcked(
            prepareCreate("test").addAlias(new Alias("alias"))
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject("field1")
                        .startObject("properties")
                        .startObject("field2")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .setSettings(Settings.builder().put("index.refresh_interval", -1))
        );

        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().startObject("field1").field("field2", "value1").endObject().endObject())
            .get();

        IllegalArgumentException exc = asInstanceOf(
            IllegalArgumentException.class,
            getDocumentFailure(indexOrAlias(), "1", r -> r.setStoredFields("field1"))
        );
        assertThat(exc.getMessage(), equalTo("field [field1] isn't a leaf field"));

        flush();

        exc = asInstanceOf(IllegalArgumentException.class, getDocumentFailure(indexOrAlias(), "1", r -> r.setStoredFields("field1")));
        assertThat(exc.getMessage(), equalTo("field [field1] isn't a leaf field"));
    }

    public void testGetFieldsComplexField() throws Exception {
        assertAcked(
            prepareCreate("my-index")
                // multi types in 5.6
                .setSettings(Settings.builder().put("index.refresh_interval", -1))
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("_doc")
                        .startObject("properties")
                        .startObject("field1")
                        .field("type", "object")
                        .startObject("properties")
                        .startObject("field2")
                        .field("type", "object")
                        .startObject("properties")
                        .startObject("field3")
                        .field("type", "object")
                        .startObject("properties")
                        .startObject("field4")
                        .field("type", "text")
                        .field("store", true)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("field1")
                .startObject()
                .startObject("field2")
                .startArray("field3")
                .startObject()
                .field("field4", "value1")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .startObject()
                .startObject("field2")
                .startArray("field3")
                .startObject()
                .field("field4", "value2")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );

        logger.info("indexing documents");

        prepareIndex("my-index").setId("1").setSource(source, XContentType.JSON).get();

        logger.info("checking real time retrieval");

        String field = "field1.field2.field3.field4";
        GetResponse getResponse = getDocument("my-index", "1", r -> r.setStoredFields(field));
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));

        getResponse = getDocument("my-index", "1", r -> r.setStoredFields(field));
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));

        logger.info("waiting for recoveries to complete");

        // Flush fails if shard has ongoing recoveries, make sure the cluster is settled down
        ensureGreen();

        logger.info("flushing");
        BroadcastResponse flushResponse = indicesAdmin().prepareFlush("my-index").setForce(true).get();
        if (flushResponse.getSuccessfulShards() == 0) {
            StringBuilder sb = new StringBuilder("failed to flush at least one shard. total shards [").append(
                flushResponse.getTotalShards()
            ).append("], failed shards: [").append(flushResponse.getFailedShards()).append("]");
            for (DefaultShardOperationFailedException failure : flushResponse.getShardFailures()) {
                sb.append("\nShard failure: ").append(failure);
            }
            fail(sb.toString());
        }

        logger.info("checking post-flush retrieval");

        getResponse = getDocument("my-index", "1", r -> r.setStoredFields(field));
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));
    }

    public void testUngeneratedFieldsThatAreNeverStored() throws IOException {
        String createIndexSource = """
            {
              "settings": {
                "index.translog.flush_threshold_size": "1pb",
                "refresh_interval": "-1"
              },
              "mappings": {
                "_doc": {
                  "properties": {
                    "suggest": {
                      "type": "completion"
                    }
                  }
                }
              }
            }""";
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSource(createIndexSource, XContentType.JSON));
        ensureGreen();
        String doc = """
            {
              "suggest": {
                "input": [
                  "Nevermind",
                  "Nirvana"
                ]
              }
            }""";

        index("test", "1", doc);
        String[] fieldsList = { "suggest" };
        // before refresh - document is only in translog
        assertGetFieldsAlwaysNull(indexOrAlias(), "1", fieldsList);
        refresh();
        // after refresh - document is in translog and also indexed
        assertGetFieldsAlwaysNull(indexOrAlias(), "1", fieldsList);
        flush();
        // after flush - document is in not anymore translog - only indexed
        assertGetFieldsAlwaysNull(indexOrAlias(), "1", fieldsList);
    }

    public void testUngeneratedFieldsThatAreAlwaysStored() throws IOException {
        String createIndexSource = """
            {
              "settings": {
                "index.translog.flush_threshold_size": "1pb",
                "refresh_interval": "-1"
              }
            }""";
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSource(createIndexSource, XContentType.JSON));
        ensureGreen();

        prepareIndex("test").setId("1").setRouting("routingValue").setId("1").setSource("{}", XContentType.JSON).get();

        String[] fieldsList = { "_routing" };
        // before refresh - document is only in translog
        assertGetFieldsAlwaysWorks(indexOrAlias(), "1", fieldsList, "routingValue");
        refresh();
        // after refresh - document is in translog and also indexed
        assertGetFieldsAlwaysWorks(indexOrAlias(), "1", fieldsList, "routingValue");
        flush();
        // after flush - document is in not anymore translog - only indexed
        assertGetFieldsAlwaysWorks(indexOrAlias(), "1", fieldsList, "routingValue");
    }

    public void testUngeneratedFieldsNotPartOfSourceStored() throws IOException {
        String createIndexSource = """
            {
              "settings": {
                "index.translog.flush_threshold_size": "1pb",
                "refresh_interval": "-1"
              }
            }""";

        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSource(createIndexSource, XContentType.JSON));
        ensureGreen();
        String doc = """
            {
              "text": "some text."
            }
            """;
        prepareIndex("test").setId("1").setSource(doc, XContentType.JSON).setRouting("1").get();
        String[] fieldsList = { "_routing" };
        // before refresh - document is only in translog
        assertGetFieldsAlwaysWorks(indexOrAlias(), "1", fieldsList, "1");
        refresh();
        // after refresh - document is in translog and also indexed
        assertGetFieldsAlwaysWorks(indexOrAlias(), "1", fieldsList, "1");
        flush();
        // after flush - document is in not anymore translog - only indexed
        assertGetFieldsAlwaysWorks(indexOrAlias(), "1", fieldsList, "1");
    }

    public void testGeneratedStringFieldsUnstored() throws IOException {
        indexSingleDocumentWithStringFieldsGeneratedFromText(false, randomBoolean());
        String[] fieldsList = { "_field_names" };
        // before refresh - document is only in translog
        assertGetFieldsAlwaysNull(indexOrAlias(), "1", fieldsList);
        refresh();
        // after refresh - document is in translog and also indexed
        assertGetFieldsAlwaysNull(indexOrAlias(), "1", fieldsList);
        flush();
        // after flush - document is in not anymore translog - only indexed
        assertGetFieldsAlwaysNull(indexOrAlias(), "1", fieldsList);
    }

    public void testGeneratedStringFieldsStored() throws IOException {
        indexSingleDocumentWithStringFieldsGeneratedFromText(true, randomBoolean());
        String[] fieldsList = { "text1", "text2" };
        String[] alwaysNotStoredFieldsList = { "_field_names" };
        assertGetFieldsAlwaysWorks(indexOrAlias(), "1", fieldsList);
        assertGetFieldsNull(indexOrAlias(), "1", alwaysNotStoredFieldsList);
        flush();
        // after flush - document is in not anymore translog - only indexed
        assertGetFieldsAlwaysWorks(indexOrAlias(), "1", fieldsList);
        assertGetFieldsNull(indexOrAlias(), "1", alwaysNotStoredFieldsList);
    }

    void indexSingleDocumentWithStringFieldsGeneratedFromText(boolean stored, boolean sourceEnabled) {

        String storedString = stored ? "true" : "false";
        String createIndexSource = Strings.format("""
            {
              "settings": {
                "index.translog.flush_threshold_size": "1pb",
                "refresh_interval": "-1"
              },
              "mappings": {
                "_doc": {
                  "_source": {
                    "enabled": %s
                  },
                  "properties": {
                    "text1": {
                      "type": "text",
                      "store": "%s"
                    },
                    "text2": {
                      "type": "text",
                      "store": "%s"
                    }
                  }
                }
              }
            }""", sourceEnabled, storedString, storedString);

        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSource(createIndexSource, XContentType.JSON));
        ensureGreen();
        String doc = """
            {
              "text1": "some text."
            ,  "text2": "more text."
            }
            """;
        index("test", "1", doc);
    }

    public void testAvoidWrappingSearcherInMultiGet() {
        SearcherWrapperPlugin.enabled = true;
        assertAcked(
            prepareCreate("test").setMapping("f", "type=keyword")
                .setSettings(indexSettings(1, 0).put("index.refresh_interval", "-1").put("index.routing.rebalance.enable", "none"))
        );
        // start tracking translog locations in the live version map
        {
            index("test", "0", Map.of("f", "empty"));
            getDocument("test", "0", r -> r.setRealtime(true));
            refresh("test");
        }
        Map<String, String> indexedDocs = new HashMap<>();
        indexedDocs.put("0", "empty");
        Map<String, String> visibleDocs = new HashMap<>(indexedDocs);
        Set<String> pendingIds = new HashSet<>();
        int iters = between(1, 20);
        for (int iter = 0; iter < iters; iter++) {
            int numDocs = randomIntBetween(1, 20);
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < numDocs; i++) {
                String id = Integer.toString(between(1, 50));
                String value = "v-" + between(1, 1000);
                indexedDocs.put(id, value);
                pendingIds.add(id);
                bulk.add(new IndexRequest("test").id(id).source("f", value));
            }
            assertNoFailures(bulk.get());
            SearcherWrapperPlugin.calls.set(0);
            boolean realTime = randomBoolean();
            MultiGetRequestBuilder mget = client().prepareMultiGet().setRealtime(realTime);
            List<String> ids = randomSubsetOf(between(1, indexedDocs.size()), indexedDocs.keySet());
            Randomness.shuffle(ids);
            for (String id : ids) {
                mget.add("test", id);
            }
            MultiGetResponse resp = mget.get();
            Map<String, String> expected = realTime ? indexedDocs : visibleDocs;
            int getFromTranslog = 0;
            for (int i = 0; i < ids.size(); i++) {
                String id = ids.get(i);
                MultiGetItemResponse item = resp.getResponses()[i];
                assertThat(item.getId(), equalTo(id));
                if (expected.containsKey(id)) {
                    assertTrue(item.getResponse().isExists());
                    assertThat(item.getResponse().getSource().get("f"), equalTo(expected.get(id)));
                } else {
                    assertFalse(item.getResponse().isExists());
                }
                if (realTime && pendingIds.contains(id)) {
                    getFromTranslog++;
                }
            }
            int expectedCalls = getFromTranslog == ids.size() ? getFromTranslog : getFromTranslog + 1;
            assertThat(SearcherWrapperPlugin.calls.get(), equalTo(expectedCalls));
            if (randomBoolean()) {
                refresh("test");
                visibleDocs = new HashMap<>(indexedDocs);
                pendingIds.clear();
            }
        }
    }

    public void testGetRemoteIndex() {
        IllegalArgumentException iae = asInstanceOf(IllegalArgumentException.class, getDocumentFailure("cluster:index", "id", r -> r));
        assertEquals(
            "Cross-cluster calls are not supported in this context but remote indices were requested: [cluster:index]",
            iae.getMessage()
        );
    }

    public void testRealTimeGetNestedFields() {
        String index = "test";
        SourceFieldMapper.Mode sourceMode = randomFrom(SourceFieldMapper.Mode.values());
        assertAcked(
            prepareCreate(index).setMapping("title", "type=keyword", "author", "type=nested")
                .setSettings(
                    indexSettings(1, 0).put("index.refresh_interval", -1)
                        .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), sourceMode)
                )
        );
        ensureGreen();
        String source0 = """
            {
              "title": "t0",
              "author": [
                {
                  "name": "a0"
                }
              ]
            }
            """;
        prepareIndex(index).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE).setId("0").setSource(source0, XContentType.JSON).get();
        // start tracking translog locations
        assertTrue(client().prepareGet(index, "0").setRealtime(true).get().isExists());
        String source1 = """
            {
              "title": ["t1"],
              "author": [
                {
                  "name": "a1"
                }
              ]
            }
            """;
        prepareIndex(index).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE).setId("1").setSource(source1, XContentType.JSON).get();
        String source2 = """
            {
              "title": ["t1", "t2"],
              "author": [
                {
                  "name": "a1"
                },
                {
                  "name": "a2"
                }
              ]
            }
            """;
        prepareIndex(index).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE).setId("2").setSource(source2, XContentType.JSON).get();
        String source3 = """
            {
              "title": ["t1", "t3", "t2"]
            }
            """;
        prepareIndex(index).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE).setId("3").setSource(source3, XContentType.JSON).get();
        GetResponse translog1 = client().prepareGet(index, "1").setRealtime(true).get();
        GetResponse translog2 = client().prepareGet(index, "2").setRealtime(true).get();
        GetResponse translog3 = client().prepareGet(index, "3").setRealtime(true).get();
        assertTrue(translog1.isExists());
        assertTrue(translog2.isExists());
        assertTrue(translog3.isExists());
        switch (sourceMode) {
            case STORED -> {
                assertThat(translog1.getSourceAsBytesRef().utf8ToString(), equalTo(source1));
                assertThat(translog2.getSourceAsBytesRef().utf8ToString(), equalTo(source2));
                assertThat(translog3.getSourceAsBytesRef().utf8ToString(), equalTo(source3));
            }
            case SYNTHETIC -> {
                assertThat(translog1.getSourceAsBytesRef().utf8ToString(), equalTo("""
                    {"author":{"name":"a1"},"title":"t1"}"""));
                assertThat(translog2.getSourceAsBytesRef().utf8ToString(), equalTo("""
                    {"author":[{"name":"a1"},{"name":"a2"}],"title":["t1","t2"]}"""));
                assertThat(translog3.getSourceAsBytesRef().utf8ToString(), equalTo("""
                    {"title":["t1","t2","t3"]}"""));
            }
            case DISABLED -> {
                assertNull(translog1.getSourceAsBytesRef());
                assertNull(translog2.getSourceAsBytesRef());
                assertNull(translog3.getSourceAsBytesRef());
            }
        }
        assertFalse(client().prepareGet(index, "1").setRealtime(false).get().isExists());
        assertFalse(client().prepareGet(index, "2").setRealtime(false).get().isExists());
        assertFalse(client().prepareGet(index, "3").setRealtime(false).get().isExists());
        refresh(index);
        GetResponse lucene1 = client().prepareGet(index, "1").setRealtime(randomBoolean()).get();
        GetResponse lucene2 = client().prepareGet(index, "2").setRealtime(randomBoolean()).get();
        GetResponse lucene3 = client().prepareGet(index, "3").setRealtime(randomBoolean()).get();
        assertTrue(lucene1.isExists());
        assertTrue(lucene2.isExists());
        assertTrue(lucene3.isExists());
        assertThat(translog1.getSourceAsBytesRef(), equalTo(lucene1.getSourceAsBytesRef()));
        assertThat(translog2.getSourceAsBytesRef(), equalTo(lucene2.getSourceAsBytesRef()));
        assertThat(translog3.getSourceAsBytesRef(), equalTo(lucene3.getSourceAsBytesRef()));
    }

    private void assertGetFieldsAlwaysWorks(String index, String docId, String[] fields) {
        assertGetFieldsAlwaysWorks(index, docId, fields, null);
    }

    private void assertGetFieldsAlwaysWorks(String index, String docId, String[] fields, @Nullable String routing) {
        for (String field : fields) {
            assertGetFieldWorks(index, docId, field, routing);
            assertGetFieldWorks(index, docId, field, routing);
        }
    }

    private void assertGetFieldWorks(String index, String docId, String field, @Nullable String routing) {
        GetResponse response = getDocument(index, docId, field, routing);
        assertThat(response.getId(), equalTo(docId));
        assertTrue(response.isExists());
        assertNotNull(response.getField(field));
        response = multiGetDocument(index, docId, field, routing);
        assertThat(response.getId(), equalTo(docId));
        assertTrue(response.isExists());
        assertNotNull(response.getField(field));
    }

    protected void assertGetFieldsNull(String index, String docId, String[] fields) {
        assertGetFieldsNull(index, docId, fields, null);
    }

    protected void assertGetFieldsNull(String index, String docId, String[] fields, @Nullable String routing) {
        for (String field : fields) {
            assertGetFieldNull(index, docId, field, routing);
        }
    }

    protected void assertGetFieldsAlwaysNull(String index, String docId, String[] fields) {
        assertGetFieldsAlwaysNull(index, docId, fields, null);
    }

    protected void assertGetFieldsAlwaysNull(String index, String docId, String[] fields, @Nullable String routing) {
        for (String field : fields) {
            assertGetFieldNull(index, docId, field, routing);
            assertGetFieldNull(index, docId, field, routing);
        }
    }

    protected void assertGetFieldNull(String index, String docId, String field, @Nullable String routing) {
        // for get
        GetResponse response = getDocument(index, docId, field, routing);
        assertTrue(response.isExists());
        assertNull(response.getField(field));
        assertThat(response.getId(), equalTo(docId));
        // same for multi get
        response = multiGetDocument(index, docId, field, routing);
        assertNull(response.getField(field));
        assertThat(response.getId(), equalTo(docId));
        assertTrue(response.isExists());
    }

    private GetResponse multiGetDocument(String index, String docId, String field, @Nullable String routing) {
        MultiGetRequest.Item getItem = new MultiGetRequest.Item(index, docId).storedFields(field);
        if (routing != null) {
            getItem.routing(routing);
        }
        MultiGetRequestBuilder multiGetRequestBuilder = client().prepareMultiGet().add(getItem);
        MultiGetResponse multiGetResponse = multiGetRequestBuilder.get();
        assertThat(multiGetResponse.getResponses().length, equalTo(1));
        return multiGetResponse.getResponses()[0].getResponse();
    }

    private GetResponse getDocument(String index, String docId, String field, @Nullable String routing) {
        return getDocument(index, docId, r -> {
            r.setStoredFields(field);
            if (routing != null) {
                r.setRouting(routing);
            }
            return r;
        });
    }
}
