/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.test.eql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.eql.DataLoader.TEST_INDEX;

public abstract class EqlSpecTestCase extends BaseEqlSpecTestCase {

    @ParametersFactory(shuffle = false, argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readTestSpecs() throws Exception {

        // Load EQL validation specs
        Set<String> uniqueTestNames = new HashSet<>();
        List<EqlSpec> specs = EqlSpecLoader.load("/test_queries.toml", true, uniqueTestNames);
        specs.addAll(EqlSpecLoader.load("/additional_test_queries.toml", true, uniqueTestNames));
        specs.addAll(EqlSpecLoader.load("/test_queries_date.toml", true, uniqueTestNames));
        List<EqlSpec> unsupportedSpecs = EqlSpecLoader.load("/test_queries_unsupported.toml", false, uniqueTestNames);

        // Validate only currently supported specs
        List<EqlSpec> filteredSpecs = new ArrayList<>();

        for (EqlSpec spec : specs) {
            boolean supported = true;
            // Check if spec is supported, simple iteration, cause the list is short.
            for (EqlSpec unSpec : unsupportedSpecs) {
                if (spec.equals(unSpec)) {
                    supported = false;
                    break;
                }
            }

            if (supported) {
                filteredSpecs.add(spec);
            }
        }
        return asArray(filteredSpecs);
    }

    @Override
    protected String tiebreaker() {
        return "serial_event_id";
    }

    public EqlSpecTestCase(String query, String name, long[] eventIds) {
        super(TEST_INDEX, query, name, eventIds);
    }
}
