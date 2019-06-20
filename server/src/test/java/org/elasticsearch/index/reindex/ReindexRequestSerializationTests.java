package org.elasticsearch.index.reindex;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class ReindexRequestSerializationTests extends AbstractXContentTestCase<ReindexRequest> {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected ReindexRequest createTestInstance() {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices("source");
        reindexRequest.setDestIndex("destination");

        if (randomBoolean()) {
            try (XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint()) {
                BytesReference query = BytesReference.bytes(matchAllQuery().toXContent(builder, ToXContent.EMPTY_PARAMS));
                reindexRequest.setRemoteInfo(new RemoteInfo(randomAlphaOfLength(5), randomAlphaOfLength(5), between(1, Integer.MAX_VALUE),
                    null, query, "user", "pass", emptyMap(), RemoteInfo.DEFAULT_SOCKET_TIMEOUT, RemoteInfo.DEFAULT_CONNECT_TIMEOUT));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        if (randomBoolean()) {
            reindexRequest.setSourceBatchSize(randomInt(100));
        }
        if (randomBoolean()) {
            reindexRequest.setDestDocType("type");
        }
        if (randomBoolean()) {
            reindexRequest.setDestOpType("create");
        }
        if (randomBoolean()) {
            reindexRequest.setDestPipeline("my_pipeline");
        }
        if (randomBoolean()) {
            reindexRequest.setDestRouting("=cat");
        }
        if (randomBoolean()) {
            reindexRequest.setMaxDocs(randomIntBetween(100, 1000));
        }
        if (randomBoolean()) {
            reindexRequest.setAbortOnVersionConflict(false);
        }

        if (reindexRequest.getRemoteInfo() == null && randomBoolean()) {
            reindexRequest.setSourceQuery(new TermQueryBuilder("foo", "fooval"));
        }

        return reindexRequest;
    }

    @Override
    protected ReindexRequest doParseInstance(XContentParser parser) throws IOException {
        return ReindexRequest.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(ReindexRequest expectedInstance, ReindexRequest newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertArrayEquals(expectedInstance.getSearchRequest().indices(), newInstance.getSearchRequest().indices());
        assertEquals(expectedInstance.getSearchRequest(), newInstance.getSearchRequest());
        assertEquals(expectedInstance.getMaxDocs(), newInstance.getMaxDocs());
        assertEquals(expectedInstance.getSlices(), newInstance.getSlices());
        assertEquals(expectedInstance.isAbortOnVersionConflict(), newInstance.isAbortOnVersionConflict());
        assertEquals(expectedInstance.getRemoteInfo(), newInstance.getRemoteInfo());
    }

    @Override
    protected boolean enableWarningsCheck() {
        // There sometimes will be a warning about specifying types in reindex requests being deprecated.
        return false;
    }
}
