/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Runs the {@code csv-spec} tests while <strong>requesting</strong> all values to
 * be loaded from {@code stored} fields. This should mostly not change the results.
 * BUT tt changes the order of multivalued fields, so:
 * <ul>
 *     <li>We ignore the order of multivalued fields in the results.</li>
 *     <li>
 *         We skip a few tests that have no chance of working with the changed order.
 *     </li>
 * </ul>
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
            String testName = (String) s[2];
            CsvTestCase testCase = (CsvTestCase) s[4];
            if (groupName.equals("mv_expand")) {
                /*
                 * Multivalued field order is changed by source loading.
                 * MV_EXPAND changes that into row order.
                 */
                testCase.ignoreOrder = true;
                if (testName.contains("AfterSort")
                    || testName.equals("DoubleLimit_expandLimitLowerThanAvailable")
                    || testName.equals("TripleLimit_WithWhere_InBetween_MvExpand_And_Limit")
                    || testName.equals("ExpandWithMultiSort")) {
                    // Too reliant on sort order to pass.
                    continue;
                }
            }
            if (testName.contains("MvZip") || testName.contains("MvSlice")) {
                // Too reliant on sort order to pass.
                continue;
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
}
