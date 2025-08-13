/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.vector;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.function.vector.Magnitude;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorScalarFunction;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class VectorScalarFunctionsIT extends AbstractEsqlIntegTestCase {

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        if (EsqlCapabilities.Cap.MAGNITUDE_SCALAR_VECTOR_FUNCTION.isEnabled()) {
            params.add(new Object[] { "v_magnitude", (VectorScalarFunction.ScalarEvaluatorFunction) Magnitude::calculateScalar });
        }
        return params;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // testDifferentDimensions fails the final driver on the coordinator, leading to cancellation of the entire request.
            // If the exchange sink is opened on a remote node but the compute request hasn't been sent, we cannot close the exchange
            // sink (for now).Here, we reduce the inactive sinks interval to ensure those inactive sinks are removed quickly.
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 4000)))
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), InternalExchangePlugin.class);
    }

    private final String functionName;
    private final VectorScalarFunction.ScalarEvaluatorFunction scalarFunction;
    private int numDims;

    public VectorScalarFunctionsIT(
        @Name("functionName") String functionName,
        @Name("scalarFunction") VectorScalarFunction.ScalarEvaluatorFunction scalarFunction
    ) {
        this.functionName = functionName;
        this.scalarFunction = scalarFunction;
    }

    @SuppressWarnings("unchecked")
    public void testEvalOverVector() {
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL result = %s(vector)
                | KEEP vector, result
            """, functionName);

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            valuesList.forEach(values -> {
                float[] v = readVector((List<Float>) values.get(0));
                Double result = (Double) values.get(1);

                assertNotNull(result);
                float expected = scalarFunction.calculateScalar(v);
                assertEquals(expected, result, 0.0001);
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testEvalOverConstant() {
        var randomVector = randomVectorArray();
        var query = String.format(Locale.ROOT, """
                FROM test
                | EVAL result = %s(%s)
                | KEEP vector, result
            """, functionName, Arrays.toString(randomVector));

        try (var resp = run(query)) {
            List<List<Object>> valuesList = EsqlTestUtils.getValuesList(resp);
            valuesList.forEach(values -> {
                float[] v = readVector((List<Float>) values.get(0));
                Double result = (Double) values.get(1);

                assertNotNull(result);
                float expected = scalarFunction.calculateScalar(randomVector);
                assertEquals(expected, result, 0.0001);
            });
        }
    }

    private static float[] readVector(List<Float> leftVector) {
        float[] leftScratch = new float[leftVector.size()];
        for (int i = 0; i < leftVector.size(); i++) {
            leftScratch[i] = leftVector.get(i);
        }
        return leftScratch;
    }

    @Before
    public void setup() throws IOException {
        assumeTrue("Dense vector type is disabled", EsqlCapabilities.Cap.DENSE_VECTOR_FIELD_TYPE.isEnabled());

        createIndexWithDenseVector("test");

        numDims = randomIntBetween(32, 64) * 2; // min 64, even number
        int numDocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            List<Float> v = randomVector();
            docs[i] = prepareIndex("test").setId("" + i).setSource("id", String.valueOf(i), "vector", v);
        }

        indexRandom(true, docs);
    }

    private List<Float> randomVector() {
        assert numDims != 0 : "numDims must be set before calling randomVector()";
        List<Float> vector = new ArrayList<>(numDims);
        for (int j = 0; j < numDims; j++) {
            vector.add(randomFloat());
        }
        return vector;
    }

    private float[] randomVectorArray() {
        assert numDims != 0 : "numDims must be set before calling randomVectorArray()";
        return randomVectorArray(numDims);
    }

    private static float[] randomVectorArray(int dimensions) {
        float[] vector = new float[dimensions];
        for (int j = 0; j < dimensions; j++) {
            vector[j] = randomFloat();
        }
        return vector;
    }

    private void createIndexWithDenseVector(String indexName) throws IOException {
        var client = client().admin().indices();
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "integer")
            .endObject();
        createDenseVectorField(mapping, "vector");
        mapping.endObject().endObject();
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 5));

        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping(mapping)
            .setSettings(settingsBuilder.build());
        assertAcked(CreateRequest);
    }

    private void createDenseVectorField(XContentBuilder mapping, String fieldName) throws IOException {
        mapping.startObject(fieldName).field("type", "dense_vector").field("similarity", "cosine");
        mapping.endObject();
    }
}
