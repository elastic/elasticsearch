package org.elasticsearch.search.builder;


import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.search.builder.SearchSourceBuilderTests.createSearchSourceBuilder;

public class SearchSourceBuilderTest extends ESTestCase {
    private static NamedWriteableRegistry namedWriteableRegistry;

    @BeforeClass
    public static void beforeClass() {
        IndicesModule indicesModule = new IndicesModule(emptyList()) {
            @Override
            protected void configure() {
                bindMapperExtension();
            }
        };
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList()) {
            @Override
            protected void configureSearch() {
                // Skip me
            }
        };
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(indicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    public void testSearchRequestBuilderSerializationWithIndexBoost() throws Exception {
        SearchSourceBuilder searchSourceBuilder = createSearchSourceBuilder();
        createIndexBoost(searchSourceBuilder);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            searchSourceBuilder.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                SearchSourceBuilder deserializedSearchSourceBuilder = new SearchSourceBuilder(in);
                BytesStreamOutput deserializedOutput = new BytesStreamOutput();
                deserializedSearchSourceBuilder.writeTo(deserializedOutput);
                assertEquals(output.bytes(), deserializedOutput.bytes());
            }
        }
    }

    private void createIndexBoost(SearchSourceBuilder searchSourceBuilder) {
        int indexBoostSize = randomIntBetween(1, 10);
        for (int i = 0; i < indexBoostSize; i++) {
            searchSourceBuilder.indexBoost(randomAsciiOfLengthBetween(5, 20), randomFloat() * 10);
        }
    }
}
