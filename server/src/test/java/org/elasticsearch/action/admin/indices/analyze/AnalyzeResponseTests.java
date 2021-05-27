/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction.AnalyzeToken;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.RandomObjects;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class AnalyzeResponseTests extends AbstractWireSerializingTestCase<AnalyzeAction.Response> {

    @SuppressWarnings("unchecked")
    public void testNullResponseToXContent() throws IOException {
        AnalyzeAction.CharFilteredText[] charfilters = null;

        String name = "test_tokens_null";
        AnalyzeAction.AnalyzeToken[] tokens = null;
        AnalyzeAction.AnalyzeTokenList tokenizer = null;


        AnalyzeAction.AnalyzeTokenList tokenfiltersItem = new AnalyzeAction.AnalyzeTokenList(name, tokens);
        AnalyzeAction.AnalyzeTokenList[] tokenfilters = {tokenfiltersItem};

        AnalyzeAction.DetailAnalyzeResponse detail = new AnalyzeAction.DetailAnalyzeResponse(charfilters, tokenizer, tokenfilters);

        AnalyzeAction.Response response = new AnalyzeAction.Response(null, detail);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            Map<String, Object> converted = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            List<Map<String, Object>> tokenfiltersValue = (List<Map<String, Object>>) ((Map<String, Object>)
                converted.get("detail")).get("tokenfilters");
            List<Map<String, Object>> nullTokens = (List<Map<String, Object>>) tokenfiltersValue.get(0).get("tokens");
            String nameValue = (String) tokenfiltersValue.get(0).get("name");
            assertThat(nullTokens.size(), equalTo(0));
            assertThat(name, equalTo(nameValue));
        }
    }

    public void testConstructorArgs() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new AnalyzeAction.Response(null, null));
        assertEquals("Neither token nor detail set on AnalysisAction.Response", ex.getMessage());
    }

    @Override
    protected AnalyzeAction.Response createTestInstance() {
        int tokenCount = randomIntBetween(0, 30);
        AnalyzeAction.AnalyzeToken[] tokens = new AnalyzeAction.AnalyzeToken[tokenCount];
        for (int i = 0; i < tokenCount; i++) {
            tokens[i] = RandomObjects.randomToken(random());
        }
        if (randomBoolean()) {
            AnalyzeAction.CharFilteredText[] charfilters = null;
            AnalyzeAction.AnalyzeTokenList[] tokenfilters = null;
            if (randomBoolean()) {
                charfilters = new AnalyzeAction.CharFilteredText[]{
                    new AnalyzeAction.CharFilteredText("my_charfilter", new String[]{"one two"})
                };
            }
            if (randomBoolean()) {
                tokenfilters = new AnalyzeAction.AnalyzeTokenList[]{
                    new AnalyzeAction.AnalyzeTokenList("my_tokenfilter_1", tokens),
                    new AnalyzeAction.AnalyzeTokenList("my_tokenfilter_2", tokens)
                };
            }
            AnalyzeAction.DetailAnalyzeResponse dar = new AnalyzeAction.DetailAnalyzeResponse(
                charfilters,
                new AnalyzeAction.AnalyzeTokenList("my_tokenizer", tokens),
                tokenfilters);
            return new AnalyzeAction.Response(null, dar);
        }
        return new AnalyzeAction.Response(Arrays.asList(tokens), null);
    }

    /**
     * Either add a token to the token list or change the details token list name
     */
    @Override
    protected AnalyzeAction.Response mutateInstance(AnalyzeAction.Response instance) throws IOException {
        if (instance.getTokens() != null) {
            return new AnalyzeAction.Response(
                    CollectionUtils.appendToCopy(instance.getTokens(), RandomObjects.randomToken(random())), null);
        } else {
            AnalyzeToken[] tokens = instance.detail().tokenizer().getTokens();
            return new AnalyzeAction.Response(null, new AnalyzeAction.DetailAnalyzeResponse(
                    instance.detail().charfilters(),
                    new AnalyzeAction.AnalyzeTokenList("my_other_tokenizer", tokens),
                    instance.detail().tokenfilters()));
        }
    }

    @Override
    protected Reader<AnalyzeAction.Response> instanceReader() {
        return AnalyzeAction.Response::new;
    }

}
