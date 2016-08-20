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
import java.util.Arrays;
import java.util.HashMap;
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
        Map<String, Object> resizeMap = buildCollateParams();
        Map<String, Object> subResizeMap = new HashMap<>();
        subResizeMap.put(randomAsciiOfLength(5), resizeMap);

        List<Map<String, Object>> collateParams = Arrays.asList(resizeMap, subResizeMap);

        for (Map<String, Object> collateParam : collateParams) {
            SearchSourceBuilder searchSourceBuilder = createSearchSourceBuilderWithPhraseSuggestionBuilder(collateParam);
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                searchSourceBuilder.writeTo(output);
                try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                    SearchSourceBuilder deserializedSearchSourceBuilder = new SearchSourceBuilder(in);
                    BytesStreamOutput deserializedOutput = new BytesStreamOutput();
                    deserializedSearchSourceBuilder.writeTo(deserializedOutput);
                    Map<String, Object> streamInCollateParams =
                        ((PhraseSuggestionBuilder) searchSourceBuilder.suggest().getSuggestions().get("suggestion1")).collateParams();
                    Map<String, Object> streamOutCollateParams =
                        ((PhraseSuggestionBuilder) deserializedSearchSourceBuilder.suggest().getSuggestions().get("suggestion1")).collateParams();
                    assertEquals(streamInCollateParams, streamOutCollateParams);
                    assertEquals(output.bytes(), deserializedOutput.bytes());
                }
            }
        }
    }

    private static Map<String, Object> buildCollateParams() {
        Map<String, Object> collateParams = new HashMap<>();
        collateParams.put("SMzeI", randomAsciiOfLength(5));
        collateParams.put("VWsIh", randomAsciiOfLength(5));
        collateParams.put("QmpIc", randomAsciiOfLength(5));
        return collateParams;
    }

    private static SearchSourceBuilder createSearchSourceBuilderWithPhraseSuggestionBuilder(Map<String, Object> collateParams) throws Exception {
        PhraseSuggestionBuilder testBuilder = new PhraseSuggestionBuilder(randomAsciiOfLengthBetween(2, 20));
        setCommonPropertiesOnRandomBuilder(testBuilder);
        testBuilder.collateParams(collateParams);

        SearchSourceBuilder searchSourceBuilder = createSearchSourceBuilder();
        SuggestBuilder builder = new SuggestBuilder();
        searchSourceBuilder.suggest(builder);
        builder.addSuggestion("suggestion1", testBuilder);
        return searchSourceBuilder;
    }

}
