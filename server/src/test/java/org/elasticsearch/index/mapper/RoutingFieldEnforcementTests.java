/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * Mapping validation and the data-node check for {@link IndexSettings#ROUTING_FIELD}.
 */
public class RoutingFieldEnforcementTests extends MapperServiceTestCase {

    /**
     * Registers a {@code lowercase} normalizer so a keyword routing field can declare one.
     */
    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return IndexAnalyzers.of(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Map.of("lowercase", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer())),
            Map.of()
        );
    }

    public void testMatchingRoutingParses() throws IOException {
        DocumentMapper mapper = mapper("keyword");
        mapper.parse(source("{\"session_id\": \"s-100\"}", "s-100"));
    }

    public void testIntegralAndIpUseCanonicalValues() throws IOException {
        mapper("long").parse(source("{\"session_id\": \"42\"}", "42"));
        mapper("short").parse(source("{\"session_id\": 7.0}", "7"));
        mapper("ip").parse(source("{\"session_id\": \"::ffff:10.0.0.1\"}", "10.0.0.1"));
    }

    /**
     * What an update that changes the routing field looks like on the primary: the stored routing with a new value.
     */
    public void testMismatchRejected() throws IOException {
        DocumentMapper mapper = mapper("keyword");
        Exception e = expectThrows(Exception.class, () -> mapper.parse(source("{\"session_id\": \"s-200\"}", "s-100")));
        assertThat(messages(e), containsString("routing [s-100] does not match the value [s-200] of routing field [session_id]"));
    }

    public void testMissingRoutingRejected() throws IOException {
        DocumentMapper mapper = mapper("keyword");
        Exception e = expectThrows(Exception.class, () -> mapper.parse(source("{\"session_id\": \"s-100\"}", null)));
        assertThat(messages(e), containsString("routing [null] does not match the value [s-100]"));
    }

    public void testMissingValueRejected() throws IOException {
        DocumentMapper mapper = mapper("keyword");
        Exception e = expectThrows(Exception.class, () -> mapper.parse(source("{\"other\": \"x\"}", "s-100")));
        assertThat(messages(e), containsString("routing field [session_id] is missing or null"));
    }

    public void testRoutingMustBeRequired() {
        Exception e = expectThrows(
            Exception.class,
            () -> createMapperService(
                settings(),
                topMapping(b -> b.startObject("properties").startObject("session_id").field("type", "keyword").endObject().endObject())
            )
        );
        assertThat(messages(e), containsString("[_routing] must be required in the mapping of index"));
    }

    public void testFieldMustBeMapped() {
        Exception e = expectThrows(Exception.class, () -> createMapperService(settings(), routedMapping(b -> {
            b.startObject("other").field("type", "keyword").endObject();
        })));
        assertThat(messages(e), containsString("routing field [session_id] of index [index] must be mapped as one of"));
        assertThat(messages(e), containsString("but it is not mapped"));
    }

    public void testUnsupportedTypesRejected() {
        for (String type : new String[] { "text", "double", "float", "date", "boolean" }) {
            Exception e = expectThrows(Exception.class, () -> mapper(type));
            assertThat(messages(e), containsString("found [" + type + "]"));
        }
    }

    public void testMultiFieldRejected() {
        Exception e = expectThrows(Exception.class, () -> createMapperService(settings("session_id.raw"), routedMapping(b -> {
            b.startObject("session_id").field("type", "text");
            b.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
            b.endObject();
        })));
        assertThat(messages(e), containsString("routing field [session_id.raw] of index [index] must be mapped as one of"));
    }

    public void testNormalizerRejected() {
        Exception e = expectThrows(Exception.class, () -> createMapperService(settings(), routedMapping(b -> {
            b.startObject("session_id").field("type", "keyword").field("normalizer", "lowercase").endObject();
        })));
        assertThat(messages(e), containsString("must not have a normalizer"));
    }

    private DocumentMapper mapper(String type) throws IOException {
        return createMapperService(settings(), routedMapping(b -> b.startObject("session_id").field("type", type).endObject()))
            .documentMapper();
    }

    private static XContentBuilder routedMapping(CheckedConsumer<XContentBuilder, IOException> properties) throws IOException {
        return topMapping(b -> {
            b.startObject("_routing").field("required", true).endObject();
            b.startObject("properties");
            properties.accept(b);
            b.endObject();
        });
    }

    private Settings settings() {
        return settings("session_id");
    }

    private Settings settings(String field) {
        return getIndexSettingsBuilder().put(IndexSettings.ROUTING_FIELD.getKey(), field).build();
    }

    private static SourceToParse source(String json, String routing) {
        return new SourceToParse("1", new BytesArray(json), XContentType.JSON, routing);
    }

    private static String messages(Throwable e) {
        StringBuilder all = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause()) {
            all.append(t.getMessage()).append('\n');
        }
        return all.toString();
    }
}
