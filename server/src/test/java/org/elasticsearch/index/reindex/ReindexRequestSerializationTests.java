package org.elasticsearch.index.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.nio.charset.Charset;

public class ReindexRequestSerializationTests extends AbstractXContentTestCase<ReindexRequest> {

    @Override
    protected ReindexRequest createTestInstance() {
        ReindexRequest reindex = new ReindexRequest();
        reindex.setSourceIndices("source");
        reindex.setSourceQuery(QueryBuilders.matchAllQuery());
        reindex.setDestIndex("dest");
        return reindex;
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
    }

    public void testThing() throws IOException {
        String thing = "{\n" +
            "  \"source\": {\n" +
            "    \"index\": [\n" +
            "      \"test\"\n" +
            "    ],\n" +
            "    \"size\": 1000,\n" +
            "    \"query\": {\n" +
            "      \"match_all\": {\n" +
            "        \"boost\": 1.0\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"dest\": {\n" +
            "    \"index\": \"dest\"\n" +
            "  }\n" +
            "}";

        byte[] bytes = thing.getBytes();

        try (XContentParser p = createParser(JsonXContent.jsonXContent, new BytesArray(bytes))) {
            ReindexRequest reindexRequest = ReindexRequest.fromXContent(p);
        }
    }
}
