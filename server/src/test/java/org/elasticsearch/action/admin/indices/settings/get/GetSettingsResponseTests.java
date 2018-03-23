package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.RandomCreateIndexGenerator;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.HashMap;

public class GetSettingsResponseTests extends AbstractStreamableXContentTestCase<GetSettingsResponse> {
    @Override
    protected GetSettingsResponse createBlankInstance() {
        return new GetSettingsResponse();
    }

    @Override
    protected GetSettingsResponse createTestInstance() {
        HashMap<String, Settings> indexToSettings = new HashMap<>();
        int numIndices = randomIntBetween(1, 5);
        for (int x=0;x<numIndices;x++) {
            String indexName = randomAlphaOfLength(5);
            indexToSettings.put(indexName, RandomCreateIndexGenerator.randomIndexSettings());
        }
        ImmutableOpenMap<String, Settings> immutableIndexToSettings = ImmutableOpenMap.<String, Settings>builder().putAll(indexToSettings).build();
        return new GetSettingsResponse(immutableIndexToSettings);
    }

    @Override
    protected GetSettingsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetSettingsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
