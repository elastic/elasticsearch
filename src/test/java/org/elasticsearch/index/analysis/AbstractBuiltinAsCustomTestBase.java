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

package org.elasticsearch.index.analysis;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.io.Resources;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util._TestUtil;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 * Base class for testing BuiltinAsCustom implementations. Plugins that define
 * builtin analyzers should extend this to test their implementation of
 * BuiltinAsCustom.
 */
public abstract class AbstractBuiltinAsCustomTestBase extends ElasticsearchTokenStreamTestCase {
    private static final int ROUNDS = 20;
    private static final List<String> EXAMPLES;
    static {
        URL resource = Resources.getResource(StandardBuiltinAsCustomTest.class, "builtin_as_custom_examples.txt");
        Splitter splitter = Splitter.on('\n').trimResults().omitEmptyStrings();
        try {
            EXAMPLES = splitter.splitToList(Resources.toString(resource, Charsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract BuiltinAsCustom buildBuiltinAsCustom();

    /**
     * Implement me in subclasses to add extra examples that must parse the
     * same.
     */
    protected List<String> extraExamples() {
        return Collections.emptyList();
    }

    /**
     * Rebuild the builtin analyzer as a custom analyzer and throw a bunch of
     * strings at both of them to make sure they spit out the same tokens.
     * 
     * @param builtinName
     *            name of builtin analyzer to rebuild as a custom analyzer and
     *            test
     * @throws IOException
     *             if thrown by the analyzers
     */
    protected void check(String builtinName) throws IOException {
        XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent);
        builder.startObject().startObject("index").startObject("analysis");
        buildBuiltinAsCustom().build(builder, builtinName);
        builder.endObject().endObject().endObject();
        Settings settings = ImmutableSettings.settingsBuilder().loadFromSource(builder.string()).build();
        AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(settings);
        Analyzer builtin = analysisService.analyzer(builtinName);
        Analyzer custom = analysisService.analyzer("custom_" + builtinName);

        // Both exist
        assertNotNull(builtin);
        assertNotNull(custom);

        // Sanity checks
        checkRandomData(random(), builtin, ROUNDS);
        checkRandomData(random(), custom, ROUNDS);

        // Now test some strings
        for (String example : EXAMPLES) {
            checkSame(builtin, custom, example);
        }
        for (String example : extraExamples()) {
            checkSame(builtin, custom, example);
        }
        for (int i = 0; i < ROUNDS; i++) {
            checkSame(builtin, custom, _TestUtil.randomAnalysisString(random(), 1000, false));
        }
    }

    private void checkSame(Analyzer builtin, Analyzer custom, String input) throws IOException {
        TokenStream builtinStream = builtin.tokenStream("dummy", input);
        TokenStream customStream = custom.tokenStream("dummy", input);

        builtinStream.reset();
        customStream.reset();

        CharTermAttribute builtinTerm = builtinStream.addAttribute(CharTermAttribute.class);
        CharTermAttribute customTerm = customStream.addAttribute(CharTermAttribute.class);
        OffsetAttribute builtinOffset = builtinStream.addAttribute(OffsetAttribute.class);
        OffsetAttribute customOffset = customStream.addAttribute(OffsetAttribute.class);
        TypeAttribute builtinType = builtinStream.addAttribute(TypeAttribute.class);
        TypeAttribute customType = customStream.addAttribute(TypeAttribute.class);
        PositionIncrementAttribute builtinPositionIncrement = builtinStream.addAttribute(PositionIncrementAttribute.class);
        PositionIncrementAttribute customPositionIncrement = customStream.addAttribute(PositionIncrementAttribute.class);
        PositionLengthAttribute builtinPositionLength = builtinStream.addAttribute(PositionLengthAttribute.class);
        PositionLengthAttribute customPositionLength = customStream.addAttribute(PositionLengthAttribute.class);
        KeywordAttribute builtinKeywordMarker = builtinStream.addAttribute(KeywordAttribute.class);
        KeywordAttribute customKeywordMarker = customStream.addAttribute(KeywordAttribute.class);

        while (builtinStream.incrementToken()) {
            assertTrue("Custom ran out of tokens before builtin", customStream.incrementToken());
            assertEquals(builtinTerm, customTerm);
            assertEquals(builtinOffset, customOffset);
            assertEquals(builtinType, customType);
            assertEquals(builtinPositionIncrement, customPositionIncrement);
            assertEquals(builtinPositionLength, customPositionLength);
            assertEquals(builtinKeywordMarker, customKeywordMarker);
        }

        assertFalse("Custom still had tokens after builtin", customStream.incrementToken());
        builtinStream.end();
        customStream.end();
        builtinStream.close();
        customStream.close();
    }

}