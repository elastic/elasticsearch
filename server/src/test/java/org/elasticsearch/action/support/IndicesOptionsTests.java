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

package org.elasticsearch.action.support;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions.Option;
import org.elasticsearch.action.support.IndicesOptions.WildcardStates;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent.MapParams;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.CoreMatchers.equalTo;

public class IndicesOptionsTests extends ESTestCase {

    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            Version version = randomVersionBetween(random(), Version.V_7_0_0, null);
            IndicesOptions indicesOptions = IndicesOptions.fromOptions(
                randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                randomBoolean());

            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(version);
            indicesOptions.writeIndicesOptions(output);

            StreamInput streamInput = output.bytes().streamInput();
            streamInput.setVersion(version);
            IndicesOptions indicesOptions2 = IndicesOptions.readIndicesOptions(streamInput);

            assertThat(indicesOptions2.ignoreUnavailable(), equalTo(indicesOptions.ignoreUnavailable()));
            assertThat(indicesOptions2.allowNoIndices(), equalTo(indicesOptions.allowNoIndices()));
            assertThat(indicesOptions2.expandWildcardsOpen(), equalTo(indicesOptions.expandWildcardsOpen()));
            assertThat(indicesOptions2.expandWildcardsClosed(), equalTo(indicesOptions.expandWildcardsClosed()));

            assertThat(indicesOptions2.forbidClosedIndices(), equalTo(indicesOptions.forbidClosedIndices()));
            assertThat(indicesOptions2.allowAliasesToMultipleIndices(), equalTo(indicesOptions.allowAliasesToMultipleIndices()));

            assertEquals(indicesOptions2.ignoreAliases(), indicesOptions.ignoreAliases());
        }
    }

    public void testFromOptions() {
        boolean ignoreUnavailable = randomBoolean();
        boolean allowNoIndices = randomBoolean();
        boolean expandToOpenIndices = randomBoolean();
        boolean expandToClosedIndices = randomBoolean();
        boolean allowAliasesToMultipleIndices = randomBoolean();
        boolean forbidClosedIndices = randomBoolean();
        boolean ignoreAliases = randomBoolean();
        boolean ingoreThrottled = randomBoolean();

        IndicesOptions indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices,expandToOpenIndices,
                expandToClosedIndices, allowAliasesToMultipleIndices, forbidClosedIndices, ignoreAliases, ingoreThrottled);

        assertThat(indicesOptions.ignoreUnavailable(), equalTo(ignoreUnavailable));
        assertThat(indicesOptions.allowNoIndices(), equalTo(allowNoIndices));
        assertThat(indicesOptions.expandWildcardsOpen(), equalTo(expandToOpenIndices));
        assertThat(indicesOptions.expandWildcardsClosed(), equalTo(expandToClosedIndices));
        assertThat(indicesOptions.allowAliasesToMultipleIndices(), equalTo(allowAliasesToMultipleIndices));
        assertThat(indicesOptions.allowAliasesToMultipleIndices(), equalTo(allowAliasesToMultipleIndices));
        assertThat(indicesOptions.forbidClosedIndices(), equalTo(forbidClosedIndices));
        assertEquals(ignoreAliases, indicesOptions.ignoreAliases());
        assertEquals(ingoreThrottled, indicesOptions.ignoreThrottled());
    }

    public void testFromOptionsWithDefaultOptions() {
        boolean ignoreUnavailable = randomBoolean();
        boolean allowNoIndices = randomBoolean();
        boolean expandToOpenIndices = randomBoolean();
        boolean expandToClosedIndices = randomBoolean();

        IndicesOptions defaultOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(),
                randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

        IndicesOptions indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices,expandToOpenIndices,
                expandToClosedIndices, defaultOptions);

        assertEquals(ignoreUnavailable, indicesOptions.ignoreUnavailable());
        assertEquals(allowNoIndices, indicesOptions.allowNoIndices());
        assertEquals(expandToOpenIndices, indicesOptions.expandWildcardsOpen());
        assertEquals(expandToClosedIndices, indicesOptions.expandWildcardsClosed());
        assertEquals(defaultOptions.allowAliasesToMultipleIndices(), indicesOptions.allowAliasesToMultipleIndices());
        assertEquals(defaultOptions.forbidClosedIndices(), indicesOptions.forbidClosedIndices());
        assertEquals(defaultOptions.ignoreAliases(), indicesOptions.ignoreAliases());
    }

    public void testFromParameters() {
        boolean expandWildcardsOpen = randomBoolean();
        boolean expandWildcardsClosed = randomBoolean();
        String expandWildcardsString;
        if (expandWildcardsOpen && expandWildcardsClosed) {
            if (randomBoolean()) {
                expandWildcardsString = "open,closed";
            } else {
                expandWildcardsString = "all";
            }
        } else if (expandWildcardsOpen) {
            expandWildcardsString = "open";
        } else if (expandWildcardsClosed) {
            expandWildcardsString = "closed";
        } else {
            expandWildcardsString = "none";
        }
        boolean ignoreUnavailable = randomBoolean();
        String ignoreUnavailableString = Boolean.toString(ignoreUnavailable);
        boolean ignoreThrottled = randomBoolean();
        String ignoreThrottledString = Boolean.toString(ignoreThrottled);
        boolean allowNoIndices = randomBoolean();
        String allowNoIndicesString = Boolean.toString(allowNoIndices);

        IndicesOptions defaultOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

        IndicesOptions updatedOptions = IndicesOptions.fromParameters(expandWildcardsString, ignoreUnavailableString,
                allowNoIndicesString, ignoreThrottled, defaultOptions);

        assertEquals(expandWildcardsOpen, updatedOptions.expandWildcardsOpen());
        assertEquals(expandWildcardsClosed, updatedOptions.expandWildcardsClosed());
        assertEquals(ignoreUnavailable, updatedOptions.ignoreUnavailable());
        assertEquals(allowNoIndices, updatedOptions.allowNoIndices());
        assertEquals(defaultOptions.allowAliasesToMultipleIndices(), updatedOptions.allowAliasesToMultipleIndices());
        assertEquals(defaultOptions.forbidClosedIndices(), updatedOptions.forbidClosedIndices());
        assertEquals(defaultOptions.ignoreAliases(), updatedOptions.ignoreAliases());
    }

    public void testSimpleByteBWC() {
        Map<Byte, IndicesOptions> old = new HashMap<>();
        // These correspond to each individual option (bit) in the old byte-based IndicesOptions
        old.put((byte) 0, IndicesOptions.fromOptions(false, false, false, false, true, false, false, false));
        old.put((byte) 1, IndicesOptions.fromOptions(true, false, false, false, true, false, false, false));
        old.put((byte) 2, IndicesOptions.fromOptions(false, true, false, false, true, false, false, false));
        old.put((byte) 4, IndicesOptions.fromOptions(false, false, true, false, true, false, false, false));
        old.put((byte) 8, IndicesOptions.fromOptions(false, false, false, true, true, false, false, false));
        old.put((byte) 16, IndicesOptions.fromOptions(false, false, false, false, false, false, false, false));
        old.put((byte) 32, IndicesOptions.fromOptions(false, false, false, false, true, true, false, false));
        old.put((byte) 64, IndicesOptions.fromOptions(false, false, false, false, true, false, true, false));
        // Test a few multi-selected options
        old.put((byte) 13, IndicesOptions.fromOptions(true, false, true, true, true, false, false, false));
        old.put((byte) 19, IndicesOptions.fromOptions(true, true, false, false, false, false, false, false));
        old.put((byte) 24, IndicesOptions.fromOptions(false, false, false, true, false, false, false, false));
        old.put((byte) 123, IndicesOptions.fromOptions(true, true, false, true, false, true, true, false));

        for (Map.Entry<Byte, IndicesOptions> entry : old.entrySet()) {
            IndicesOptions indicesOptions2 = IndicesOptions.fromByte(entry.getKey());
            logger.info("--> 1 {}", entry.getValue().toString());
            logger.info("--> 2 {}", indicesOptions2.toString());
            assertThat("IndicesOptions for byte " + entry.getKey() + " differ for conversion",indicesOptions2, equalTo(entry.getValue()));
        }
    }

    public void testEqualityAndHashCode() {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(
            randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
            randomBoolean());

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(indicesOptions, opts -> {
            return IndicesOptions.fromOptions(opts.ignoreUnavailable(), opts.allowNoIndices(), opts.expandWildcardsOpen(),
                opts.expandWildcardsClosed(), opts.allowAliasesToMultipleIndices(), opts.forbidClosedIndices(), opts.ignoreAliases(),
                opts.ignoreThrottled());
        }, opts -> {
            boolean mutated = false;
            boolean ignoreUnavailable = opts.ignoreUnavailable();
            boolean allowNoIndices = opts.allowNoIndices();
            boolean expandOpen = opts.expandWildcardsOpen();
            boolean expandClosed = opts.expandWildcardsClosed();
            boolean allowAliasesToMulti = opts.allowAliasesToMultipleIndices();
            boolean forbidClosed = opts.forbidClosedIndices();
            boolean ignoreAliases = opts.ignoreAliases();
            boolean ignoreThrottled = opts.ignoreThrottled();
            while (mutated == false) {
                if (randomBoolean()) {
                    ignoreUnavailable = !ignoreUnavailable;
                    mutated = true;
                }
                if (randomBoolean()) {
                    allowNoIndices = !allowNoIndices;
                    mutated = true;
                }
                if (randomBoolean()) {
                    expandOpen = !expandOpen;
                    mutated = true;
                }
                if (randomBoolean()) {
                    expandClosed = !expandClosed;
                    mutated = true;
                }
                if (randomBoolean()) {
                    allowAliasesToMulti = !allowAliasesToMulti;
                    mutated = true;
                }
                if (randomBoolean()) {
                    forbidClosed = !forbidClosed;
                    mutated = true;
                }
                if (randomBoolean()) {
                    ignoreAliases = !ignoreAliases;
                    mutated = true;
                }
                if (randomBoolean()) {
                    ignoreThrottled = !ignoreThrottled;
                    mutated = true;
                }
            }
            return IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices, expandOpen, expandClosed,
                allowAliasesToMulti, forbidClosed, ignoreAliases, ignoreThrottled);
        });
    }

    public void testFromMap() {
        IndicesOptions defaults = IndicesOptions.strictExpandOpen();
        Collection<String> wildcardStates = randomBoolean() ?
                null : randomSubsetOf(Arrays.asList("open", "closed"));
        Boolean ignoreUnavailable = randomBoolean() ? null : randomBoolean();
        Boolean allowNoIndices = randomBoolean() ? null : randomBoolean();
        Boolean ignoreThrottled = randomBoolean() ? null : randomBoolean();

        Map<String, Object> settings = new HashMap<>();

        if (wildcardStates != null) {
            settings.put("expand_wildcards", wildcardStates);
        }

        if (ignoreUnavailable != null) {
            settings.put("ignore_unavailable", ignoreUnavailable);
        }

        if (allowNoIndices != null) {
            settings.put("allow_no_indices", allowNoIndices);
        }

        if (ignoreThrottled != null) {
            settings.put("ignore_throttled", ignoreThrottled);
        }

        IndicesOptions fromMap = IndicesOptions.fromMap(settings, defaults);

        boolean open = wildcardStates != null ? wildcardStates.contains("open") : defaults.expandWildcardsOpen();
        assertEquals(fromMap.expandWildcardsOpen(), open);
        boolean closed = wildcardStates != null ? wildcardStates.contains("closed") : defaults.expandWildcardsClosed();
        assertEquals(fromMap.expandWildcardsClosed(), closed);

        assertEquals(fromMap.ignoreUnavailable(), ignoreUnavailable == null ? defaults.ignoreUnavailable() : ignoreUnavailable);
        assertEquals(fromMap.allowNoIndices(), allowNoIndices == null ? defaults.allowNoIndices() : allowNoIndices);
        assertEquals(fromMap.ignoreThrottled(), ignoreThrottled == null ? defaults.ignoreThrottled() : ignoreThrottled);
    }

    public void testToXContent() throws IOException {
        Collection<WildcardStates> wildcardStates = randomSubsetOf(Arrays.asList(WildcardStates.values()));
        Collection<Option> options = randomSubsetOf(Arrays.asList(Option.values()));

        IndicesOptions indicesOptions = new IndicesOptions(
                options.isEmpty() ? Option.NONE : EnumSet.copyOf(options),
                wildcardStates.isEmpty() ? WildcardStates.NONE : EnumSet.copyOf(wildcardStates));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        indicesOptions.toXContent(builder, new MapParams(Collections.emptyMap()));
        builder.endObject();
        XContentParser parser = XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput());
        Map<String, Object> map = parser.mapOrdered();

        boolean open = wildcardStates.contains(WildcardStates.OPEN);
        if (open) {
            assertTrue(((List<?>) map.get("expand_wildcards")).contains("open"));
        } else {
            assertFalse(((List<?>) map.get("expand_wildcards")).contains("open"));
        }
        boolean closed = wildcardStates.contains(WildcardStates.CLOSED);
        if (closed) {
            assertTrue(((List<?>) map.get("expand_wildcards")).contains("closed"));
        } else {
            assertFalse(((List<?>) map.get("expand_wildcards")).contains("closed"));
        }
        assertEquals(map.get("ignore_unavailable"), options.contains(Option.IGNORE_UNAVAILABLE));
        assertEquals(map.get("allow_no_indices"), options.contains(Option.ALLOW_NO_INDICES));
        assertEquals(map.get("ignore_throttled"), options.contains(Option.IGNORE_THROTTLED));
    }
}
