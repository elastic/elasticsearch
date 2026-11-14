/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.junit.Before;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reuses REST inputs and expected results with real compute drivers. HTTP, indexing, and stored-field loading
 * remain outside its coverage.
 */
@SuppressWarnings("unchecked")
public class NestedReductionsContractTests extends LuceneTestCase {
    private record Contract(String name, List<Map<String, Object>> setup, List<Map<String, Object>> steps) {
        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Source settings are checked while resolving fields, which this harness does not run. That case is checked by
     * the REST test and by {@link NestedReductionsSourceValidationTests}.
     */
    private static final String FIELD_RESOLUTION_ONLY = "synthetic source is rejected when reading source";

    /**
     * This harness runs without Lucene, so nested fields are always read from source here. The doc-values cases are
     * checked by the REST tests.
     */
    private static final Set<String> INDEXED_ONLY = Set.of(
        "a reduction on the data node reads doc values like an ordinary field",
        "a reduction after a sort and limit runs on the data node",
        "expansion on the data node reads doc values like an ordinary field",
        "synthetic source expands doc values on the data node"
    );

    /**
     * This harness reads every row on one node from one index, so nothing runs on the coordinator after an exchange and
     * no shard maps a field differently from another. The REST tests check those cases.
     */
    private static final Set<String> REST_ONLY = Set.of(
        "expansion after a limit is rejected",
        "expansion after a limit without a sort is rejected",
        "every index must copy the objects into the parent"
    );

    @ParametersFactory
    public static Iterable<Object[]> contracts() throws IOException {
        LogConfigurator.configureESLogging();
        List<Object[]> parameters = new ArrayList<>();
        parameters.addAll(contracts("292_nested_reductions.yml", 68));
        parameters.addAll(contracts("295_nested_expand.yml", 25));
        parameters.addAll(contracts("296_nested_reductions_pushdown.yml", 8));
        parameters.addAll(contracts("302_nested_flatten.yml", 13));
        parameters.addAll(contracts("304_nested_expand_pushdown.yml", 8));
        return parameters;
    }

    private static List<Object[]> contracts(String file, int expected) throws IOException {
        String yaml;
        try (var stream = NestedReductionsContractTests.class.getResourceAsStream("/nested-scope-contract/" + file)) {
            if (stream == null) {
                throw new IOException("missing nested scope contract [" + file + "]");
            }
            yaml = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
        List<Map<String, Object>> setup = null;
        List<Object[]> parameters = new ArrayList<>();
        for (String document : yaml.split("(?m)^---\\s*$")) {
            if (document.lines().allMatch(line -> line.isBlank() || line.stripLeading().startsWith("#"))) {
                continue;
            }
            var parsed = XContentHelper.convertToMap(new BytesArray(document), true, XContentType.YAML).v2();
            if (parsed.containsKey("setup")) {
                setup = (List<Map<String, Object>>) parsed.get("setup");
            } else {
                for (var entry : parsed.entrySet()) {
                    if (entry.getKey().equals(FIELD_RESOLUTION_ONLY) == false
                        && INDEXED_ONLY.contains(entry.getKey()) == false
                        && REST_ONLY.contains(entry.getKey()) == false) {
                        parameters.add(new Object[] { new Contract(entry.getKey(), setup, (List<Map<String, Object>>) entry.getValue()) });
                    }
                }
            }
        }
        if (parameters.size() != expected) {
            throw new AssertionError("expected " + expected + " contracts in [" + file + "], got " + parameters.size());
        }
        return parameters;
    }

    private final Contract contract;

    public NestedReductionsContractTests(@Name("contract") Contract contract) {
        this.contract = contract;
    }

    @Before
    public void onlySnapshotBuildsRegisterNested() {
        assumeTrue("The nested functions are registered on snapshot builds", Build.current().isSnapshot());
    }

    public void testComputeContract() throws Exception {
        checkContract(false, true);
    }

    public void testTransportedComputeContract() throws Exception {
        checkContract(true, true);
    }

    public void testMinimallyLoweredComputeContract() throws Exception {
        checkContract(false, false);
    }

    private void checkContract(boolean transport, boolean fullyOptimize) throws Exception {
        Map<String, Map<String, Object>> mappings = new LinkedHashMap<>();
        List<Document> documents = new ArrayList<>();
        for (var step : contract.setup()) {
            applyInput(step, mappings, documents);
        }
        Map<String, Object> request = null;
        for (var step : contract.steps()) {
            var action = (Map<String, Object>) step.get("do");
            if (action != null && action.containsKey("esql.query")) {
                request = action;
            } else {
                applyInput(step, mappings, documents);
            }
        }
        assertNotNull(contract.name(), request);
        var body = (Map<String, Object>) ((Map<String, Object>) request.get("esql.query")).get("body");
        String query = (String) body.get("query");
        String from = query.substring(query.indexOf("FROM ") + "FROM ".length(), query.indexOf('|')).trim();
        String pattern = from.split(" METADATA ")[0].trim();
        EsIndex index = schema(pattern, mappings);
        Map<String, IndexResolution> lookups = new LinkedHashMap<>();
        if (mappings.containsKey("nr_repeats")) {
            var lookup = schema("nr_repeats", mappings);
            var props = new LinkedHashMap<>(lookup.indexProperties());
            props.put("nr_repeats", new IndexProperties(IndexMode.LOOKUP, 1));
            lookups.put(
                "nr_repeats",
                IndexResolution.valid(
                    new EsIndex(lookup.name(), lookup.mapping(), props, lookup.originalIndices(), lookup.concreteIndices())
                )
            );
        }
        if (request.containsKey("catch")) {
            boolean parsing = contract.steps()
                .stream()
                .anyMatch(step -> "parsing_exception".equals(((Map<?, ?>) step.getOrDefault("match", Map.of())).get("error.type")));
            var error = parsing
                ? assertThrows(ParsingException.class, () -> NestedReductionsTestSupport.analyze(query, index, lookups))
                : assertThrows(VerificationException.class, () -> NestedReductionsTestSupport.analyze(query, index, lookups));
            for (var step : contract.steps()) {
                var contains = (Map<String, Object>) step.get("contains");
                if (contains != null && contains.containsKey("error.reason")) {
                    assertTrue(error.getMessage(), error.getMessage().contains(contains.get("error.reason").toString()));
                }
            }
            return;
        }
        var analyzed = NestedReductionsTestSupport.analyze(query, index, lookups);
        if (transport) {
            analyzed = NestedReductionsTestSupport.roundTrip(query, analyzed);
        }
        var rows = NestedReductionsTestSupport.runLocal(
            query,
            analyzed,
            relation -> (int) documents.stream().filter(doc -> relation.indexProperties().containsKey(doc.index())).count(),
            (relation, attr) -> documents.stream()
                .filter(doc -> relation.indexProperties().containsKey(doc.index()))
                .map(doc -> input(attr, doc))
                .toList(),
            true,
            fullyOptimize
        );
        var columns = analyzed.output().stream().map(attr -> Map.of("name", attr.name(), "type", attr.dataType().outputType())).toList();
        List<?> values = rows;
        if (Boolean.TRUE.equals(body.get("columnar"))) {
            List<List<Object>> columnar = new ArrayList<>();
            for (int c = 0; c < columns.size(); c++) {
                List<Object> column = new ArrayList<>();
                for (var row : rows) {
                    column.add(row.get(c));
                }
                columnar.add(column);
            }
            values = columnar;
        }
        for (var step : contract.steps()) {
            var match = (Map<String, Object>) step.get("match");
            if (match == null) {
                continue;
            }
            if (match.containsKey("columns")) {
                assertEquals(contract.name(), match.get("columns"), columns);
            }
            if (match.containsKey("values")) {
                assertEquals(contract.name(), normalize(match.get("values")), normalize(values));
            }
        }
    }

    private record Document(String index, String id, Map<String, Object> source) {}

    private static void applyInput(Map<String, Object> step, Map<String, Map<String, Object>> mappings, List<Document> documents) {
        var action = (Map<String, Object>) step.get("do");
        if (action == null) {
            return;
        }
        if (action.containsKey("indices.create")) {
            var create = (Map<String, Object>) action.get("indices.create");
            var body = (Map<String, Object>) create.get("body");
            mappings.put(
                (String) create.get("index"),
                (Map<String, Object>) ((Map<String, Object>) body.get("mappings")).get("properties")
            );
        } else if (action.containsKey("bulk")) {
            var bulk = (List<Map<String, Object>>) ((Map<String, Object>) action.get("bulk")).get("body");
            for (int i = 0; i < bulk.size(); i += 2) {
                var metadata = (Map<String, Object>) bulk.get(i).get("index");
                documents.add(new Document((String) metadata.get("_index"), (String) metadata.get("_id"), bulk.get(i + 1)));
            }
        } else if (action.containsKey("index")) {
            var index = (Map<String, Object>) action.get("index");
            documents.add(new Document((String) index.get("index"), (String) index.get("id"), (Map<String, Object>) index.get("body")));
        } else {
            throw new AssertionError("unhandled input action " + action.keySet());
        }
    }

    private static EsIndex schema(String pattern, Map<String, Map<String, Object>> mappings) {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        for (String index : pattern.split(",")) {
            Map<String, IndexFieldCapabilities> fields = new LinkedHashMap<>();
            addFields("", mappings.get(index), fields);
            responses.add(new FieldCapabilitiesIndexResponse(index, index, fields, true, IndexMode.STANDARD, 1, 0, 0, 0));
        }
        var caps = new FieldCapabilitiesResponse(responses, List.of());
        return IndexResolver.mergedMappings(
            pattern,
            false,
            new IndexResolver.FieldsInfo(caps, TransportVersion.current(), true, false, false, false, false, false, true, null),
            false,
            (name, response) -> Map.of("", List.of(name.split(",")))
        ).get();
    }

    private static void addFields(String prefix, Map<String, Object> properties, Map<String, IndexFieldCapabilities> fields) {
        for (var entry : properties.entrySet()) {
            String name = prefix + entry.getKey();
            var mapping = (Map<String, Object>) entry.getValue();
            String type = (String) mapping.getOrDefault("type", "object");
            fields.put(
                name,
                new IndexFieldCapabilities(name, type, false, true, true, false, false, null, null, Map.of(), null, 0, false, false)
            );
            if (mapping.containsKey("properties")) {
                addFields(name + ".", (Map<String, Object>) mapping.get("properties"), fields);
            }
        }
    }

    private static Object input(Attribute attr, Document document) {
        if (attr instanceof MetadataAttribute) {
            return new BytesRef(switch (attr.name()) {
                case "_source" -> json(document.source());
                case "_id" -> document.id();
                case "_index" -> document.index();
                default -> throw new AssertionError(attr.name());
            });
        }
        Object value = XContentMapValues.extractValue(attr.name(), document.source());
        if (value == null) {
            return null;
        }
        if (attr.dataType() == DataType.KEYWORD || attr.dataType() == DataType.TEXT) {
            return new BytesRef(value.toString());
        }
        if (attr.dataType() == DataType.LONG) {
            return ((Number) value).longValue();
        }
        throw new AssertionError("unhandled parent input " + attr);
    }

    private static String json(Object value) {
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.value(value);
            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static Object normalize(Object value) {
        if (value instanceof Number number) {
            return new BigDecimal(number.toString()).stripTrailingZeros();
        }
        if (value instanceof List<?> list) {
            return list.stream().map(NestedReductionsContractTests::normalize).toList();
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> normalized = new LinkedHashMap<>();
            map.forEach((key, item) -> normalized.put(key, normalize(item)));
            return normalized;
        }
        return value;
    }
}
