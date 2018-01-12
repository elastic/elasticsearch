/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.suggest;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Integration test for registering a custom suggester.
 */
@ClusterScope(scope= Scope.SUITE, numDataNodes =1)
public class CustomSuggesterSearchIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomSuggesterPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(CustomSuggesterPlugin.class);
    }

    public static class CustomSuggesterPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<SuggesterSpec<?>> getSuggesters() {
            return singletonList(new SuggesterSpec<CustomSuggestionBuilder>("custom", CustomSuggestionBuilder::new,
                    CustomSuggestionBuilder::fromXContent));
        }
    }

    public void testThatCustomSuggestersCanBeRegisteredAndWork() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "test", "1").setSource(jsonBuilder()
                .startObject()
                .field("name", "arbitrary content")
                .endObject())
                .setRefreshPolicy(IMMEDIATE).get();

        String randomText = randomAlphaOfLength(10);
        String randomField = randomAlphaOfLength(10);
        String randomSuffix = randomAlphaOfLength(10);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("someName", new CustomSuggestionBuilder(randomField, randomSuffix).text(randomText));
        SearchRequestBuilder searchRequestBuilder = client().prepareSearch("test").setTypes("test").setFrom(0).setSize(1)
                .suggest(suggestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        // TODO: infer type once JI-9019884 is fixed
        // TODO: see also JDK-8039214
        List<Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> suggestions =
            CollectionUtils.<Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>iterableAsArrayList(
                searchResponse.getSuggest().getSuggestion("someName"));
        assertThat(suggestions, hasSize(2));
        assertThat(suggestions.get(0).getText().string(),
            is(String.format(Locale.ROOT, "%s-%s-%s-12", randomText, randomField, randomSuffix)));
        assertThat(suggestions.get(1).getText().string(),
            is(String.format(Locale.ROOT, "%s-%s-%s-123", randomText, randomField, randomSuffix)));
    }

    public static class CustomSuggestionBuilder extends SuggestionBuilder<CustomSuggestionBuilder> {
        protected static final ParseField RANDOM_SUFFIX_FIELD = new ParseField("suffix");

        private String randomSuffix;

        public CustomSuggestionBuilder(String randomField, String randomSuffix) {
            super(randomField);
            this.randomSuffix = randomSuffix;
        }

        /**
         * Read from a stream.
         */
        public CustomSuggestionBuilder(StreamInput in) throws IOException {
            super(in);
            this.randomSuffix = in.readString();
        }

        @Override
        public void doWriteTo(StreamOutput out) throws IOException {
            out.writeString(randomSuffix);
        }

        @Override
        protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(RANDOM_SUFFIX_FIELD.getPreferredName(), randomSuffix);
            return builder;
        }

        @Override
        public String getWriteableName() {
            return "custom";
        }

        @Override
        protected boolean doEquals(CustomSuggestionBuilder other) {
            return Objects.equals(randomSuffix, other.randomSuffix);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(randomSuffix);
        }

        public static CustomSuggestionBuilder fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token;
            String currentFieldName = null;
            String fieldname = null;
            String suffix = null;
            String analyzer = null;
            int sizeField = -1;
            int shardSize = -1;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (SuggestionBuilder.ANALYZER_FIELD.match(currentFieldName)) {
                        analyzer = parser.text();
                    } else if (SuggestionBuilder.FIELDNAME_FIELD.match(currentFieldName)) {
                        fieldname = parser.text();
                    } else if (SuggestionBuilder.SIZE_FIELD.match(currentFieldName)) {
                        sizeField = parser.intValue();
                    } else if (SuggestionBuilder.SHARDSIZE_FIELD.match(currentFieldName)) {
                        shardSize = parser.intValue();
                    } else if (RANDOM_SUFFIX_FIELD.match(currentFieldName)) {
                        suffix = parser.text();
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                                               "suggester[custom] doesn't support field [" + currentFieldName + "]");
                }
            }

            // now we should have field name, check and copy fields over to the suggestion builder we return
            if (fieldname == null) {
                throw new ParsingException(parser.getTokenLocation(), "the required field option is missing");
            }
            CustomSuggestionBuilder builder = new CustomSuggestionBuilder(fieldname, suffix);
            if (analyzer != null) {
                builder.analyzer(analyzer);
            }
            if (sizeField != -1) {
                builder.size(sizeField);
            }
            if (shardSize != -1) {
                builder.shardSize(shardSize);
            }
            return builder;
        }

        @Override
        public SuggestionContext build(QueryShardContext context) throws IOException {
            Map<String, Object> options = new HashMap<>();
            options.put(FIELDNAME_FIELD.getPreferredName(), field());
            options.put(RANDOM_SUFFIX_FIELD.getPreferredName(), randomSuffix);
            CustomSuggester.CustomSuggestionsContext customSuggestionsContext =
                new CustomSuggester.CustomSuggestionsContext(context, options);
            customSuggestionsContext.setField(field());
            assert text != null;
            customSuggestionsContext.setText(BytesRefs.toBytesRef(text));
            return customSuggestionsContext;
        }

    }

}
