/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.transport.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherXContentParser;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.QueryWatchesAction;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.actions.email.EmailActionTests;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateService;
import org.elasticsearch.xpack.watcher.test.WatcherTestUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class QueryWatchesResponseTests extends AbstractSerializingTestCase<QueryWatchesAction.Response> {

    private static final ConstructingObjectParser<QueryWatchesAction.Response.Item, Void> TEST_ITEM_PARSER = new ConstructingObjectParser<>(
        "query_watches_response_item",
        false,
        (args, c) -> new QueryWatchesAction.Response.Item((String) args[0], (XContentSource) args[1],
            (WatchStatus) args[2], (long) args[3], (long) args[4])
    );

    static {
        TEST_ITEM_PARSER.declareString(constructorArg(), new ParseField("_id"));
        TEST_ITEM_PARSER.declareObject(
            constructorArg(),
            (p, c) -> new XContentSource(XContentBuilder.builder(p.contentType().xContent()).copyCurrentStructure(p)),
            new ParseField("watch")
        );
        TEST_ITEM_PARSER.declareObject(
            constructorArg(),
            (p, c) -> WatchStatus.parse("_not_used", new WatcherXContentParser(p, ZonedDateTime.now(ZoneOffset.UTC), null, false)),
            new ParseField("status")
        );
        TEST_ITEM_PARSER.declareLong(constructorArg(), new ParseField("_seq_no"));
        TEST_ITEM_PARSER.declareLong(constructorArg(), new ParseField("_primary_term"));
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<QueryWatchesAction.Response, Void> TEST_PARSER = new ConstructingObjectParser<>(
        "query_watches_response",
        false,
        (args, c) -> new QueryWatchesAction.Response((long) args[0], (List<QueryWatchesAction.Response.Item>) args[1])
    );

    static {
        TEST_PARSER.declareLong(constructorArg(), new ParseField("count"));
        TEST_PARSER.declareObjectArray(constructorArg(), (ContextParser<Void, Object>) TEST_ITEM_PARSER::parse, new ParseField("watches"));
    }

    @Override
    protected QueryWatchesAction.Response doParseInstance(XContentParser parser) throws IOException {
        return TEST_PARSER.parse(parser, null);
    }

    @Override
    protected Writeable.Reader<QueryWatchesAction.Response> instanceReader() {
        return QueryWatchesAction.Response::new;
    }

    @Override
    protected QueryWatchesAction.Response createTestInstance() {
        int numWatches = randomIntBetween(0, 10);
        List<QueryWatchesAction.Response.Item> items = new ArrayList<>(numWatches);
        for (int i = 0; i < numWatches; i++) {
            Watch watch = createWatch("_id + " + i);
            try (XContentBuilder builder = jsonBuilder()) {
                watch.toXContent(builder, WatcherParams.builder()
                    .hideSecrets(true)
                    .includeStatus(false)
                    .build());
                items.add(new QueryWatchesAction.Response.Item(randomAlphaOfLength(4),
                    new XContentSource(builder), watch.status(), 1, 0));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return new QueryWatchesAction.Response(numWatches + randomIntBetween(0, 100), items);
    }

    private Watch createWatch(String watchId)  {
        return WatcherTestUtils.createTestWatch(watchId,
            mock(Client.class),
            mock(HttpClient.class),
            new EmailActionTests.NoopEmailService(),
            mock(WatcherSearchTemplateService.class),
            logger);
    }

    @Override
    protected void assertEqualInstances(QueryWatchesAction.Response expectedInstance, QueryWatchesAction.Response newInstance) {
        assertThat(expectedInstance.getWatchTotalCount(), equalTo(newInstance.getWatchTotalCount()));
        assertThat(expectedInstance.getWatches().size(), equalTo(newInstance.getWatches().size()));
        for (int i = 0; i < expectedInstance.getWatches().size(); i++) {
            QueryWatchesAction.Response.Item expected = expectedInstance.getWatches().get(i);
            QueryWatchesAction.Response.Item actual = newInstance.getWatches().get(i);
            assertThat(expected.getId(), equalTo(actual.getId()));
            assertThat(expected.getSource(), equalTo(actual.getSource()));
            assertThat(expected.getSeqNo(), equalTo(actual.getSeqNo()));
            assertThat(expected.getPrimaryTerm(), equalTo(actual.getPrimaryTerm()));
        }
    }
}
