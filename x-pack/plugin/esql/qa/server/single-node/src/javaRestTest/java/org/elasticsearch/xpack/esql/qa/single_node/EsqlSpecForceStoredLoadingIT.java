/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Runs the {@code csv-spec} tests while <strong>requesting</strong> all values to
 * be loaded from {@code stored} fields. This should mostly not change the results.
 * BUT it changes the order of multivalued fields, so:
 * <ul>
 *     <li>We ignore the order of multivalued fields in the results.</li>
 *     <li>
 *         We skip a few tests that have no chance of working with the changed order.
 *     </li>
 * </ul>
 * <p>
 *     Stored-field extraction can be substantially slower than the default source path; the
 *     REST client's default {@value org.elasticsearch.test.rest.ESRestTestCase#CLIENT_SOCKET_TIMEOUT}
 *     (60s) is occasionally exceeded in CI. Use at least a 3-minute socket timeout while still
 *     respecting longer explicit {@code tests.client.socket.timeout} values.
 * </p>
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class EsqlSpecForceStoredLoadingIT extends EsqlSpecIT {
    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<Object[]> orig = EsqlSpecIT.readScriptSpec();
        List<Object[]> specs = new ArrayList<>(orig.size());

        // Filter or hack test cases so they'll pass.
        for (Object[] s : orig) {
            String groupName = (String) s[1];
            CsvTestCase testCase = (CsvTestCase) s[4];
            switch (testCase.requestStored) {
                case SKIP:
                    continue;
                case IGNORE_ORDER:
                    testCase.ignoreOrder = true;
                    break;
            }
            specs.add(s);
        }
        return specs;
    }

    public EsqlSpecForceStoredLoadingIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected MappedFieldType.FieldExtractPreference fieldExtractPreference() {
        return MappedFieldType.FieldExtractPreference.STORED;
    }

    @Override
    protected boolean ignoreValueOrder() {
        return true;
    }

    @Override
    protected void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        final TimeValue configured = TimeValue.parseTimeValue(
            Objects.requireNonNullElse(settings.get(CLIENT_SOCKET_TIMEOUT), "60s"),
            CLIENT_SOCKET_TIMEOUT
        );
        final TimeValue floor = TimeValue.timeValueMinutes(3);
        final TimeValue socketTimeout = configured.millis() >= floor.millis() ? configured : floor;
        final Settings merged = Settings.builder().put(settings).put(CLIENT_SOCKET_TIMEOUT, socketTimeout.getStringRep()).build();
        super.configureClient(builder, merged);
    }
}
