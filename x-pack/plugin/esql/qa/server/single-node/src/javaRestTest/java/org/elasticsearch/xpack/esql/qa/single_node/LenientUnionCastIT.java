/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FoldablesConvertFunction;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Every {@code union_*} catalog index paired with every other, cast to every conversion target.
 * A lenient union cast either returns a value or null, or fails because nothing can convert.
 * It must not fail with the generic "due to ambiguities" error, and it must not 5xx.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class LenientUnionCastIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    private static final RequestOptions ALLOW_WARNINGS = RequestOptions.DEFAULT.toBuilder()
        .setWarningsHandler(WarningsHandler.PERMISSIVE)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private record CastTarget(String function, String resultType) {}

    public void testEveryTypePairAgainstEveryCastTarget() throws IOException {
        List<String> indices = unionIndices();
        assertThat(indices.size(), greaterThan(1));
        CsvTestsDataLoader.loadDatasetsIntoEs(client(), indices);
        List<CastTarget> targets = castTargets();
        assertThat(targets.size(), greaterThan(1));
        List<String> failures = new ArrayList<>();
        for (int i = 0; i < indices.size(); i++) {
            for (int j = i + 1; j < indices.size(); j++) {
                for (CastTarget target : targets) {
                    checkPair(indices.get(i), indices.get(j), target, failures);
                }
            }
        }
        assertThat(failures, empty());
    }

    /**
     * One target per {@link EsqlDataTypeConverter#converterFunctionFactory} entry that accepts a field.
     * The query name is the registered function name, and the expected column type is {@link DataType#outputType()}.
     */
    private static List<CastTarget> castTargets() {
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry().snapshotRegistry();
        List<CastTarget> targets = new ArrayList<>();
        for (DataType type : DataType.values()) {
            var factory = EsqlDataTypeConverter.converterFunctionFactory(type);
            if (factory == null) {
                continue;
            }
            AbstractConvertFunction convert = factory.apply(Source.EMPTY, Literal.NULL, null);
            // A converted geotile can be an invalid zoom/x/y. Writing that value to the response kills the node.
            // https://github.com/elastic/elasticsearch/issues/160230
            if (type == DataType.GEOTILE
                || convert instanceof FoldablesConvertFunction
                || registry.functionExists(convert.getClass()) == false) {
                continue;
            }
            String function = registry.functionName(convert.getClass()).toUpperCase(Locale.ROOT);
            FunctionDefinition definition = registry.resolveFunction(function.toLowerCase(Locale.ROOT));
            if (definition.capabilities().stream().allMatch(LenientUnionCastIT::capabilityEnabled) == false) {
                continue;
            }
            targets.add(new CastTarget(function, convert.dataType().outputType()));
        }
        targets.sort(Comparator.comparing(CastTarget::function));
        return targets;
    }

    private static boolean capabilityEnabled(String name) {
        for (EsqlCapabilities.Cap cap : EsqlCapabilities.Cap.values()) {
            if (cap.capabilityName().equals(name)) {
                return cap.isEnabled();
            }
        }
        return true;
    }

    /**
     * {@code union_*} rows from the csv catalog whose required capabilities are enabled.
     * Each index holds one document and a {@code field} of that type.
     */
    private static List<String> unionIndices() {
        return CsvTestsDataLoader.CSV_DATASET.values()
            .stream()
            .filter(dataset -> dataset.indexName().startsWith("union_"))
            .filter(dataset -> dataset.requiredCapabilities().stream().allMatch(EsqlCapabilities.Cap::isEnabled))
            .map(CsvTestsDataLoader.TestDataset::indexName)
            .sorted()
            .toList();
    }

    private void checkPair(String left, String right, CastTarget target, List<String> failures) throws IOException {
        String label = left + "|" + right + " " + target.function;
        String query = String.format(Locale.ROOT, "FROM %s,%s | EVAL x = %s(field) | KEEP x", left, right, target.function);
        Request request = new Request("POST", "/_query");
        request.setOptions(ALLOW_WARNINGS);
        request.setJsonEntity("{\"query\":\"" + query + "\"}");
        try {
            Response response = client().performRequest(request);
            Map<String, Object> body = entityAsMap(response);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> columns = (List<Map<String, Object>>) body.get("columns");
            if (columns == null || columns.size() != 1 || target.resultType.equals(columns.get(0).get("type")) == false) {
                failures.add(label + " unexpected columns " + columns);
                return;
            }
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) body.get("values");
            if (values == null || values.size() != 2) {
                failures.add(label + " expected 2 rows, got " + values);
            }
        } catch (ResponseException e) {
            int status = e.getResponse().getStatusLine().getStatusCode();
            String reason = String.valueOf(entityAsMap(e.getResponse()).get("error"));
            // Cannot convert field [field] to [IP]: no type can be converted: [aggregate_metric_double] in [union_aggregate_metric_double],
            // [boolean] in [union_boolean]
            boolean allowed = reason.contains("no type can be converted")
                // Mapped types [date_nanos, datetime] of [field] cannot be accepted in [TO_IP(field)]
                || reason.contains("cannot be accepted");
            if (status >= 500 || reason.contains("due to ambiguities") || allowed == false) {
                failures.add(label + " status " + status + " " + abbreviate(reason));
            }
        }
    }

    private static String abbreviate(String reason) {
        return reason.length() <= 500 ? reason : reason.substring(0, 500);
    }
}
