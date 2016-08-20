package org.elasticsearch.search.builder;


import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.elasticsearch.search.builder.SearchSourceBuilderTests.createSearchSourceBuilder;
import static org.elasticsearch.search.suggest.AbstractSuggestionBuilderTestCase.setCommonPropertiesOnRandomBuilder;

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

    public void testSearchRequestBuilderSerialization() throws Exception {
        SearchSourceBuilder searchSourceBuilder = createSearchSourceBuilderWithPhraseSuggestionBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            searchSourceBuilder.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                SearchSourceBuilder deserializedSearchSourceBuilder = new SearchSourceBuilder(in);
                BytesStreamOutput deserializedOutput = new BytesStreamOutput();
                deserializedSearchSourceBuilder.writeTo(deserializedOutput);
                Iterator<Map.Entry<String, Object>> streamInMap =
                    ((PhraseSuggestionBuilder) searchSourceBuilder.suggest().getSuggestions().get("suggestion1")).collateParams().entrySet().iterator();
                Iterator<Map.Entry<String, Object>> streamOutMap =
                    ((PhraseSuggestionBuilder) deserializedSearchSourceBuilder.suggest().getSuggestions().get("suggestion1")).collateParams().entrySet().iterator();
                assertMapsOrder(streamInMap, streamOutMap);
                assertEquals(output.bytes(), deserializedOutput.bytes());
            }
        }
    }

    private void assertMapsOrder(Iterator<Map.Entry<String, Object>> streamInMap, Iterator<Map.Entry<String, Object>> streamOutMap) {
        while(streamInMap.hasNext()) {
            assertEquals(streamInMap.next().getKey(), streamOutMap.next().getKey());
        }
    }

    private static SearchSourceBuilder createSearchSourceBuilderWithPhraseSuggestionBuilder() throws Exception {
        PhraseSuggestionBuilder testBuilder = new PhraseSuggestionBuilder(randomAsciiOfLengthBetween(2, 20));
        setCommonPropertiesOnRandomBuilder(testBuilder);
        Map<String, Object> collateParams = new HashMap<>();
        collateParams.put("SMzeI", "ouOrp");
        collateParams.put("VWsIh", "FPpbt");
        collateParams.put("QmpIc", "WfrMs");
        testBuilder.collateParams(collateParams);

        SearchSourceBuilder searchSourceBuilder = createSearchSourceBuilder();
        SuggestBuilder builder = new SuggestBuilder();
        searchSourceBuilder.suggest(builder);
        builder.addSuggestion("suggestion1", testBuilder);
        return searchSourceBuilder;
    }

}
