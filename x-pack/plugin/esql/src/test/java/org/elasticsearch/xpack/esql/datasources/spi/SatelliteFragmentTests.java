/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class SatelliteFragmentTests extends ESTestCase {

    private static Map<String, Object> stamp(Object tableParameters) {
        Map<String, Object> declaration = tableParameters == null
            ? Map.of("options", List.of("units"))
            : Map.of("tableParameters", tableParameters);
        return Map.of(SatelliteFragment.CAPABILITY_KEY, declaration);
    }

    /** The names come from the stamp, which travels with the plan; a config rebuilt from the registry knows nothing of them. */
    public void testTheTableParametersComeFromTheCapabilityStamp() {
        Map<String, Object> config = Map.of("endpoint", "grpc://h:1", "location", "s3://b/p/");
        Map<String, Object> result = SatelliteFragment.withTableParameters(config, stamp(List.of("location", "format")));
        assertThat(result.get(SatelliteFragment.TABLE_PARAMETERS_KEY), equalTo(List.of("location", "format")));
        assertThat(result.get("location"), equalTo("s3://b/p/"));
        assertFalse("the input is not changed", config.containsKey(SatelliteFragment.TABLE_PARAMETERS_KEY));
    }

    public void testWithoutDeclaredTableParametersTheConfigIsReturnedAsItIs() {
        Map<String, Object> config = Map.of("endpoint", "grpc://h:1");
        assertThat(SatelliteFragment.withTableParameters(config, stamp(null)), sameInstance(config));
        assertThat(SatelliteFragment.withTableParameters(config, stamp(List.of())), sameInstance(config));
        assertThat(SatelliteFragment.withTableParameters(config, Map.of()), sameInstance(config));
        assertThat(SatelliteFragment.withTableParameters(config, null), sameInstance(config));
    }

    public void testTheNewKeysAreNeverSourceOptions() {
        for (String name : List.of("ca_certificate", "server_name", "token", "credentials", SatelliteFragment.TABLE_PARAMETERS_KEY)) {
            assertFalse(name, SatelliteFragment.isOptionKey(name));
        }
    }
}
